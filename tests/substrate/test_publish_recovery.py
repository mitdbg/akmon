"""#1791: recover a refused publish without teardown, with real publishes.

Two scenarios on one owned database, through the same service API the CLI
calls:

* initial install with no baseline: the scaffold's inputs are left
  uncommitted, so the first publish is refused for repository provenance;
  the operator commits and re-runs ingest;
* update with a healthy baseline: a record delta plus an edit to the
  fingerprinted ``nomos`` block refuses the publish ``catalog_manifest_drifted``;
  the operator re-claims ownership and re-runs ingest.

At every step the read-only recovery plan is asserted against the database,
the last published generation is proven readable and unchanged while wedged,
the redrive publishes exactly once with distinct node ids and zero
retractions, and a repeated recovery run is a no-op that reports the actual
state. Nothing is deleted and no admission or provenance check is bypassed.
"""
from __future__ import annotations

import json
import os
import subprocess
import threading
import time
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from urllib.parse import quote, urlencode

import psycopg
import pytest
import yaml
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import tuple_row

import okg.substrate.cli.main as cli
from okg.substrate import deployment_upgrade as du
from okg.substrate import publish_recovery as pr
from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.catalog.ownership import (
    catalog_identity_for,
    claim_catalog_ownership,
)
from okg.substrate.deployment_scaffold import create_deployment_scaffold
from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest import IngestOptions, run_ingest
from okg.substrate.publish_proof import assert_publish_proof
from tests.pytest.source_child_patches import (
    install_in_source_children,
    read_records,
)

_CHILD_STUBS = "tests.substrate._publish_recovery_child_stubs"


pytestmark = pytest.mark.postgres


@pytest.fixture
def recovery_private_dsn(okg_dsn: str):
    """Own one fresh database; the scenarios claim catalog ownership, which
    must never touch the shared test database."""
    options = conninfo_to_dict(okg_dsn)
    options.pop("dbname", None)

    def target(name: str) -> str:
        uri = "postgresql:///" + quote(name, safe="")
        if options:
            uri += "?" + urlencode(options, quote_via=quote)
        return uri

    name = "okg_test_recovery_" + uuid.uuid4().hex
    admin, dsn = target("postgres"), target(name)
    with psycopg.connect(admin, autocommit=True, connect_timeout=15) as conn:
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))

    from okg.substrate.db import migrate as migration

    original = dict(migration.REQUIRED_EXTENSIONS)
    # pg_cron belongs to the server's configured database, as in hermetic_dsn.
    migration.REQUIRED_EXTENSIONS.pop("pg_cron", None)
    try:
        migration.ensure_required_extensions(dsn)
        migration.migrate(dsn)
        yield dsn
    finally:
        from okg.substrate.db.pool import close_all

        close_all()
        migration.REQUIRED_EXTENSIONS.clear()
        migration.REQUIRED_EXTENSIONS.update(original)
        with psycopg.connect(admin, autocommit=True) as conn:
            conn.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)),
            )


_GIT_ENV = {
    "GIT_AUTHOR_NAME": "OKG recovery test",
    "GIT_AUTHOR_EMAIL": "recovery@example.invalid",
    "GIT_COMMITTER_NAME": "OKG recovery test",
    "GIT_COMMITTER_EMAIL": "recovery@example.invalid",
}


def _git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=root,
        env={**os.environ, **_GIT_ENV},
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _commit_all(root: Path, message: str) -> None:
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", message)


def _published_node_ids(dsn: str, generation_id: str) -> list[str]:
    """Node ids readable in one published generation, duplicates included."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        generation = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout_generation(generation_id)
        rows = generation.query(
            "SELECT node_id FROM okg.graph_nodes ORDER BY node_id",
        )
    return [str(row["node_id"]) for row in rows]


def _latest_published(dsn: str, deployment: str):
    with psycopg.connect(dsn, autocommit=True) as conn:
        return ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).latest_generation(deployment_name=deployment, status="published")


def _admission_statuses(dsn: str, deployment: str) -> dict[str, int]:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, count(*)
              FROM okg.admission_envelopes
             WHERE deployment_name = %s AND producer_kind = 'source'
             GROUP BY status
            """,
            (deployment,),
        )
        return {str(status): int(count) for status, count in cur.fetchall()}


def _generation_pin_rows(dsn: str) -> int:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM okg.history_generation_pins")
        return int(cur.fetchone()[0])


def _retractions(report: dict[str, Any]) -> list[tuple[int, int]]:
    return [
        (int(result.get("retract_n") or 0), int(result.get("retract_e") or 0))
        for result in report["results"]
    ]


def _failed_publishes(report: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        publish for publish in report["publishes"]
        if publish.get("status") == "fail"
    ]


def _real_publishes(report: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        publish for publish in report["publishes"]
        if not publish.get("publish_skipped")
    ]


def _plan(dsn: str, deployment: str) -> dict[str, Any]:
    plan = pr.plan_publish_recovery(
        dsn=dsn, deployment=deployment, runtime_enabled=False,
    )
    json.dumps(plan)
    assert plan["ok"] is True, plan
    return plan


def test_refused_publish_recovers_without_teardown(
    tmp_path: Path,
    recovery_private_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    record_property,
) -> None:
    dsn = recovery_private_dsn
    root = tmp_path / "deployments"
    dep = create_deployment_scaffold("test", deployments_root=root).deployment_dir
    deployment_yaml = yaml.safe_load((dep / "deployment.yaml").read_text())
    # The deployment directory is its own repository; the code repository is
    # this checkout, pinned as the scaffold publish test pins it.
    deployment_yaml["release"] = {
        "channel": "dev",
        "pin": {"code_repository": str(Path(__file__).resolve().parents[2])},
    }
    (dep / "deployment.yaml").write_text(
        yaml.safe_dump(deployment_yaml, sort_keys=False), encoding="utf-8",
    )
    monkeypatch.setenv("OKG_DSN", dsn)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    _git(dep, "init", "-q", "-b", "main")
    options = IngestOptions(
        dsn=dsn,
        deployment="test",
        deployment_dir=dep,
        owner_id="s14-1791-recovery",
        owner_meta={"pact": "s14-1791-recovery-without-teardown"},
    )

    # ---- initial install, no baseline -------------------------------------
    untouched = _plan(dsn, "test")
    assert untouched["state"] == pr.STATE_NEVER_PUBLISHED
    assert untouched["recovery_command"] == (
        "uv run okg provision --deployment test --json"
    )
    assert untouched["last_known_good"] is None

    assert cli.main([
        "catalog", "load", "--deployment", str(dep), "--apply", "--json",
    ]) == 0
    capsys.readouterr()

    # Inputs deliberately left uncommitted: the source runs, the publish is
    # refused for repository provenance, and the records stay staged.
    wedged_install = run_ingest(options)
    assert wedged_install.any_failed is True
    refused = _failed_publishes(wedged_install.report)
    assert refused, json.dumps(wedged_install.report["publishes"], default=str)
    assert refused[0]["error"] == "repository provenance inputs are not committed"
    assert "commit or gitignore every path" in refused[0]["fix_hint"]
    assert "okg ingest" in refused[0]["fix_hint"]
    assert "git add -A" not in refused[0]["fix_hint"]

    # The provision proof for that publish names the cause and the recovery,
    # not a generation that never existed.
    proof = assert_publish_proof(refused[0], dsn=dsn)
    assert proof["ok"] is False
    assert proof["generation_id"] is None
    assert proof["publish_error"] == "repository provenance inputs are not committed"
    assert "commit or gitignore" in proof["fix_hint"]
    assert "generation describe" not in proof["fix_hint"]

    staged_install = _plan(dsn, "test")
    assert staged_install["state"] == pr.STATE_STAGED_WORK_UNPUBLISHED
    assert staged_install["recovery_command"] == (
        "uv run okg ingest --deployment test --json"
    )
    assert staged_install["okg_up_applies"] is False
    assert staged_install["last_known_good"] is None
    assert _admission_statuses(dsn, "test") == {"accepted": 1}
    assert _latest_published(dsn, "test") is None

    # A second run diagnoses: it stops before any source write and carries
    # the provenance refusal's hint through the staged-source guard.
    with pytest.raises(SubstrateError) as guard:
        run_ingest(options)
    assert "refusing to start new source writes" in str(guard.value)
    assert guard.value.data["publish_error"] == (
        "repository provenance inputs are not committed"
    )
    assert "commit or gitignore" in (guard.value.fix_hint or "")
    assert _plan(dsn, "test") == staged_install  # safe to repeat, same answer

    # Fix the cause, then the documented recovery: `okg ingest`.
    _commit_all(dep, "commit the scaffold inputs")
    recovered_install = run_ingest(options)
    assert recovered_install.any_failed is False, json.dumps(
        recovered_install.report, default=str,
    )
    published = _real_publishes(recovered_install.report)
    assert [publish["label"] for publish in published] == ["staged-source-redrive"]
    assert published[0]["status"] == "published"
    baseline = _latest_published(dsn, "test")
    assert baseline is not None
    baseline_ids = _published_node_ids(dsn, baseline.checkpoint_id)
    # The generated fixture publishes the bootstrap document AND the section it
    # contains; `create_deployment_scaffold` gained that section in #1827 while
    # this test was being written against the one-node fixture.
    assert baseline_ids == ["test:doc:1", "test:doc:1:section:1"]
    # The staged admission is now published. The recovery run's own source
    # pass leaves a zero-fact `accepted` envelope (nothing new to admit),
    # which carries no fact refs and is not staged work.
    statuses = _admission_statuses(dsn, "test")
    assert statuses["published"] == 1 and "failed" not in statuses, statuses

    healthy = _plan(dsn, "test")
    assert healthy["state"] == pr.STATE_HEALTHY
    assert healthy["staged_source_work"] is False
    assert healthy["recovery_command"] is None
    assert healthy["okg_up_applies"] is True
    assert healthy["last_known_good"]["generation_id"] == baseline.checkpoint_id
    # The bootstrap document, the section it contains, and the `contains`
    # edge joining them: the scaffold fixture gained the section in #1827.
    assert healthy["last_known_good"]["recorded_node_count"] == 2
    assert healthy["last_known_good"]["recorded_edge_count"] == 1
    assert healthy["queues"]["dlq_count"] == 0
    assert healthy["queues_ok"] is True

    # Repeated recovery is a no-op that reports the actual state.
    repeat_install = run_ingest(options)
    assert repeat_install.any_failed is False
    assert _real_publishes(repeat_install.report) == []
    assert repeat_install.report["publishes"][-1]["reason"] == "no_source_work"
    assert _latest_published(dsn, "test").checkpoint_id == baseline.checkpoint_id
    assert _plan(dsn, "test")["last_known_good"] == healthy["last_known_good"]
    record_property("initial_install_generation", baseline.checkpoint_id)

    # ---- update with an existing baseline ---------------------------------
    fixture = dep / "fixtures" / "bootstrap_fixture.json"
    payload = json.loads(fixture.read_text())
    payload["nodes"].append({
        "node_id": "test:doc:2",
        "subtype": "document",
        "attrs": {
            "document_id": "doc:2",
            "title": "Second document",
            "body": "added while the manifest had drifted",
        },
        "source_record_id": {"id": "doc:2"},
        "source_revision": {"v": 1},
    })
    fixture.write_text(json.dumps(payload, indent=1))
    manifest_path = dep / "deployment.yaml"
    manifest = yaml.safe_load(manifest_path.read_text())
    manifest["nomos"]["deployment_posture"]["readiness_profiles"] = [
        "personal", "general",
    ]
    manifest_path.write_text(yaml.safe_dump(manifest, sort_keys=False))
    _commit_all(dep, "add a record and drift the nomos block")

    wedged_update = run_ingest(options)
    assert wedged_update.any_failed is True
    drifted = _failed_publishes(wedged_update.report)
    assert drifted and drifted[0]["code"] == "catalog_manifest_drifted", drifted
    assert "okg catalog ownership claim" in drifted[0]["fix_hint"]

    # Last known good: still the latest published generation, still readable,
    # unchanged, while the new record waits staged.
    assert _latest_published(dsn, "test").checkpoint_id == baseline.checkpoint_id
    assert _published_node_ids(dsn, baseline.checkpoint_id) == baseline_ids
    statuses = _admission_statuses(dsn, "test")
    assert statuses["published"] == 1 and "failed" not in statuses, statuses
    staged_update = _plan(dsn, "test")
    assert staged_update["state"] == pr.STATE_STAGED_WORK_UNPUBLISHED
    assert staged_update["last_known_good"]["generation_id"] == baseline.checkpoint_id
    assert staged_update["last_known_good"]["recorded_node_count"] == 2

    # `okg up` on this deployment: its backup refusal must name the recovery
    # from the real database state, not resume itself.
    monkeypatch.setattr(
        du, "_resolve_upgrade_deployment",
        lambda _ctx: SimpleNamespace(runtime=SimpleNamespace(enabled=False)),
    )
    ctx = SimpleNamespace(dsn=dsn, options=SimpleNamespace(deployment="test"))
    pins_before = _generation_pin_rows(dsn)
    guidance = du._backup_refusal_guidance(
        ctx,
        refusal_failures=["smoke_source_versioned", "attribute_read_model_rebuildable"],
    )
    assert _generation_pin_rows(dsn) == pins_before  # no read lease taken
    assert guidance["publish_recovery"]["state"] == pr.STATE_STAGED_WORK_UNPUBLISHED
    assert guidance["publish_recovery"]["execution"]["route"] == "inline"
    assert "not the recovery for a refused publish" in guidance["fix_hint"]
    assert "uv run okg ingest --deployment test --json" in guidance["fix_hint"]
    assert set(guidance["failed_check_hints"]["manifest_configuration"]) == {
        "smoke_source_versioned", "attribute_read_model_rebuildable",
    }

    # Fix the cause the hint names -- the same call `okg catalog ownership
    # claim` makes -- then the documented recovery.
    identity = catalog_identity_for(
        composed=compose_catalog(dep),
        deployment_dir=dep,
        command="okg catalog ownership claim",
    )
    with psycopg.connect(dsn, autocommit=True) as conn:
        claim_catalog_ownership(conn, identity=identity)

    recovered_update = run_ingest(options)
    assert recovered_update.any_failed is False, json.dumps(
        recovered_update.report, default=str,
    )
    redrives = _real_publishes(recovered_update.report)
    assert [publish["label"] for publish in redrives] == ["staged-source-redrive"]
    assert redrives[0]["status"] == "published"
    assert redrives[0]["predecessor_generation_id"] == baseline.checkpoint_id
    updated = _latest_published(dsn, "test")
    assert updated.checkpoint_id != baseline.checkpoint_id
    updated_ids = _published_node_ids(dsn, updated.checkpoint_id)
    assert updated_ids == ["test:doc:1", "test:doc:1:section:1", "test:doc:2"]
    assert len(updated_ids) == len(set(updated_ids))
    assert _published_node_ids(dsn, baseline.checkpoint_id) == baseline_ids
    for report in (
        wedged_install.report, recovered_install.report,
        wedged_update.report, recovered_update.report,
    ):
        assert all(counts == (0, 0) for counts in _retractions(report)), (
            json.dumps(report["results"], default=str)
        )
    statuses = _admission_statuses(dsn, "test")
    assert statuses["published"] == 2 and "failed" not in statuses, statuses

    after = _plan(dsn, "test")
    assert after["state"] == pr.STATE_HEALTHY
    assert after["staged_source_work"] is False
    assert after["last_known_good"]["generation_id"] == updated.checkpoint_id
    assert after["last_known_good"]["recorded_node_count"] == 3
    assert after["queues"]["dlq_count"] == 0

    repeat_update = run_ingest(options)
    assert repeat_update.any_failed is False
    assert _real_publishes(repeat_update.report) == []
    assert _latest_published(dsn, "test").checkpoint_id == updated.checkpoint_id
    assert _plan(dsn, "test")["last_known_good"] == after["last_known_good"]
    record_property("update_generation", updated.checkpoint_id)
    record_property("final_node_count", len(updated_ids))
    record_property("final_dlq_count", after["queues"]["dlq_count"])


def test_health_reports_real_blocked_update_and_recovery(
    tmp_path: Path, recovery_private_dsn: str, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str], record_property,
) -> None:
    """#1863: a changed file blocks a real publish, while the baseline serves reads."""
    import shlex
    from okg.substrate.console_views.health import compose_health
    from okg.substrate.mcp import open_session

    dsn = recovery_private_dsn
    root = tmp_path / "deployments"
    dep = create_deployment_scaffold("test", deployments_root=root).deployment_dir
    manifest = yaml.safe_load((dep / "deployment.yaml").read_text())
    manifest["release"] = {"channel": "dev", "pin": {
        "code_repository": str(Path(__file__).resolve().parents[2]),
    }}
    (dep / "deployment.yaml").write_text(yaml.safe_dump(manifest))
    (dep / "invariants.yaml").write_text(yaml.safe_dump({"invariants": [{
        "name": "reject_bad_title", "severity": "error", "scope": "cross",
        "sql": "SELECT node_id FROM okg.graph_nodes WHERE attrs->>'title' = 'BLOCK'",
    }]}))
    monkeypatch.setenv("OKG_DSN", dsn)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    _git(dep, "init", "-q", "-b", "main")
    _commit_all(dep, "Baseline with publication check")
    assert cli.main(["catalog", "load", "--deployment", str(dep), "--apply", "--json"]) == 0
    capsys.readouterr()
    options = IngestOptions(dsn=dsn, deployment="test", deployment_dir=dep,
                            owner_id="issue-1863", owner_meta={"issue": "1863"})
    baseline_run = run_ingest(options)
    assert not baseline_run.any_failed, json.dumps(baseline_run.report, default=str)
    baseline = _latest_published(dsn, "test").checkpoint_id
    assert len(_published_node_ids(dsn, baseline)) == 2
    with psycopg.connect(dsn, autocommit=True) as conn:
        baseline_health = compose_health(conn, deployment_name="test", session=open_session(conn, deployment_name="test"))
    fixture = dep / "fixtures" / "bootstrap_fixture.json"
    source = json.loads(fixture.read_text())
    source["nodes"].append({"node_id": "test:doc:2", "subtype": "document",
        "attrs": {"document_id": "doc:2", "title": "BLOCK", "body": "changed source"},
        "source_record_id": {"id": "doc:2"}, "source_revision": {"v": 1}})
    fixture.write_text(json.dumps(source))
    _commit_all(dep, "Source change rejected by publication check")
    blocked_run = run_ingest(options)
    assert blocked_run.any_failed, blocked_run.report
    with psycopg.connect(dsn, autocommit=True) as conn:
        blocked_health = compose_health(conn, deployment_name="test", session=open_session(conn, deployment_name="test"))
    blocked = blocked_health["latest_attempt"]
    assert blocked["status"] == "blocked", blocked_health
    assert blocked["generation_id"] != baseline
    assert blocked_health["generation_id"] == baseline
    assert blocked_health["vitals"]["nodes_live"] == 2
    assert "cross_source_error_invariant_failed" in blocked["blocked_reason"]
    assert any(row["code"] == "generation_blocked" for row in blocked_health["checks"])
    capsys.readouterr()
    assert cli.main([*shlex.split(blocked["describe_command"])[1:], "--json"]) == 0
    diagnostic = capsys.readouterr().out
    described = json.loads(diagnostic)
    assert described["generation_id"] == blocked["generation_id"]
    assert "reject_bad_title" in json.dumps(described["blocked_reason"])
    assert "test:doc:2" in json.dumps(described["blocked_reason"])
    source["nodes"][-1]["attrs"]["title"] = "Recovered source"
    source["nodes"][-1]["source_revision"] = {"v": 2}
    fixture.write_text(json.dumps(source))
    _commit_all(dep, "Repair source input")
    recovery = run_ingest(options)
    assert not recovery.any_failed, recovery.report
    with psycopg.connect(dsn, autocommit=True) as conn:
        recovered_health = compose_health(conn, deployment_name="test", session=open_session(conn, deployment_name="test"))
    assert recovered_health["latest_attempt"]["status"] == "published"
    assert recovered_health["generation_id"] != baseline
    assert recovered_health["vitals"]["nodes_live"] == 3
    assert not any(row["code"] == "generation_blocked" for row in recovered_health["checks"])
    assert any(row["generation_id"] == blocked["generation_id"] for row in recovered_health["history"])
    assert _published_node_ids(dsn, baseline) == ["test:doc:1", "test:doc:1:section:1"]
    with psycopg.connect(dsn, autocommit=True) as conn:
        graph = ChronosGraphStore.from_connection(conn).checkout_generation(recovered_health["generation_id"])
        assert graph.query("SELECT attrs->>'title' AS title FROM okg.graph_nodes WHERE node_id = 'test:doc:2'")[0]["title"] == "Recovered source"
    record_property("baseline", baseline)
    record_property("blocked", blocked["generation_id"])
    record_property("recovered", recovered_health["generation_id"])
    artifact = os.environ.get("OKG_HEALTH_PROOF_PATH")
    if artifact:
        Path(artifact).write_text(json.dumps({"baseline": baseline_health, "blocked": blocked_health,
            "recovered": recovered_health, "diagnostic": diagnostic}, indent=2, default=str))


# `retract_chronos_run` sets `lock_timeout = '30s'` on its own connection, so a
# run that waits on the caller's own branch fence takes ~31s and then reports
# `LockNotAvailable`. Bounding every mode below that turns the wedge (#2123)
# into a fast, named failure instead of a 31s wait or an open-ended hang.
_INGEST_WALL_BOUND_S = 25.0

_SHIPPED_ENFORCEMENT_MODES = ("off", "audit", "enforce")


def _redacted_report(report: Any, tmp_path: Path) -> str:
    """The run report as evidence, without this machine in it.

    These reports are recorded as junit properties and committed as PACT
    evidence, and a run report names the checkout and the temporary
    deployment root it ran against. Those absolute paths are operator
    identity, not proof, and committed evidence carries neither them nor
    the host name (which `tests.live.chat.scrub_junit_identity` removes).
    Everything the assertions read is left exactly as the run wrote it.
    """
    text = json.dumps(report, default=str)
    for actual, label in (
        (str(tmp_path.resolve()), "<tmp>"),
        (str(Path(__file__).resolve().parents[2]), "<repo>"),
        (str(Path.home()), "<home>"),
    ):
        text = text.replace(actual, label).replace(
            json.dumps(actual)[1:-1], label,
        )
    return text


def _nomos_decision_rows(dsn: str) -> list[dict[str, Any]]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT effect,
                       reason_code,
                       enforcement_point,
                       target ->> 'kind' AS target_kind
                  FROM okg.nomos_decisions
                 ORDER BY decision_id
                """,
            )
            columns = [column.name for column in cur.description or ()]
            return [dict(zip(columns, row)) for row in cur.fetchall()]


@pytest.mark.parametrize("enforcement_mode", _SHIPPED_ENFORCEMENT_MODES)
def test_ingest_under_each_shipped_nomos_enforcement_mode(
    enforcement_mode: str,
    tmp_path: Path,
    recovery_private_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    record_property,
) -> None:
    """#2123: a real ingest under every shipped `nomos.runtime_enforcement`.

    The shipped profiles set the mode in the deployment manifest --- `audit`
    for academic-pi, git-code and systems-research, `enforce` for
    cs-researcher and workspace-graph --- and no lane ran a real ingest with
    either one on: the okg-workspace runtime fixture overrides the mode to
    `off` so those tests can exercise publishing rather than policy. A fresh
    install therefore reached operators with the source-admission boundary
    never once exercised end to end on a database.

    `audit` is log-and-allow: the boundary is evaluated and the decision row
    is written, and the run still completes. `enforce` with no active policy
    bundle legitimately refuses --- but it must refuse with its policy reason.
    Neither may wait on the branch fence the caller itself holds.
    """
    from okg.substrate.nomos.runtime_enforcement import (
        _clear_nomos_enforcement_mode_cache,
        deployment_nomos_enforcement_mode,
    )

    dsn = recovery_private_dsn
    root = tmp_path / "deployments"
    dep = create_deployment_scaffold("test", deployments_root=root).deployment_dir
    manifest = yaml.safe_load((dep / "deployment.yaml").read_text())
    manifest["release"] = {"channel": "dev", "pin": {
        "code_repository": str(Path(__file__).resolve().parents[2]),
    }}
    nomos_block = dict(manifest.get("nomos") or {})
    nomos_block["runtime_enforcement"] = enforcement_mode
    manifest["nomos"] = nomos_block
    (dep / "deployment.yaml").write_text(yaml.safe_dump(manifest))
    monkeypatch.setenv("OKG_DSN", dsn)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    # The env kill-switch outranks the manifest; this asserts the shipped
    # manifest value, so no ambient override may be in force.
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    _clear_nomos_enforcement_mode_cache()
    _git(dep, "init", "-q", "-b", "main")
    _commit_all(dep, f"Baseline with nomos.runtime_enforcement {enforcement_mode}")
    assert cli.main([
        "catalog", "load", "--deployment", str(dep), "--apply", "--json",
    ]) == 0
    capsys.readouterr()
    assert deployment_nomos_enforcement_mode("test") == enforcement_mode

    options = IngestOptions(
        dsn=dsn,
        deployment="test",
        deployment_dir=dep,
        owner_id="issue-2123",
        owner_meta={"issue": "2123"},
    )
    started = time.monotonic()
    run = run_ingest(options)
    elapsed = time.monotonic() - started
    report = _redacted_report(run.report, tmp_path)
    record_property(f"nomos_{enforcement_mode}_elapsed_s", round(elapsed, 3))
    record_property(f"nomos_{enforcement_mode}_report", report)

    # The wedge signature, named explicitly so a regression reads as itself.
    assert "LockNotAvailable" not in report, report
    assert elapsed < _INGEST_WALL_BOUND_S, f"{elapsed:.1f}s: {report}"

    # Source-level completeness, not the publish: a developer checkout with
    # uncommitted files refuses the publish for repository provenance, which
    # is a different gate from the one this test guards.
    completeness = run.report["completeness"]
    decisions = _nomos_decision_rows(dsn)
    points = {row["enforcement_point"] for row in decisions}

    # One admitted envelope, `accepted` or already `published`: whether the
    # run goes on to publish depends on repository provenance of the
    # checkout, which is a different gate from the one guarded here.
    def _assert_admitted_once() -> None:
        statuses = _admission_statuses(dsn, "test")
        assert set(statuses) <= {"accepted", "published"}, statuses
        assert sum(statuses.values()) == 1, statuses

    if enforcement_mode == "off":
        # `off` skips evaluation entirely, so there is no decision to record.
        assert decisions == [], decisions
        assert completeness["failed_sources"] == [], report
        _assert_admitted_once()
        return

    if enforcement_mode == "audit":
        # Log-and-allow: the run completes AND the strict verdict survives.
        # The `source_run` target is the finalizer's own decision, the one
        # the lock timeout used to roll back along with the transaction that
        # wrote it; the runner's `source_ingest` and `source_emit` rows
        # commit separately and were recorded even while the run failed.
        assert completeness["failed_sources"] == [], report
        _assert_admitted_once()
        assert "source.emit" in points, decisions
        admitted = [
            row for row in decisions if row["target_kind"] == "source_run"
        ]
        assert admitted, decisions
        assert all(row["effect"] == "deny" for row in admitted), admitted
        assert all(
            row["reason_code"] == "nomos.default_deny.no_active_bundle"
            for row in admitted
        ), admitted
        return

    # `enforce` with no active policy bundle is a real refusal, and the
    # source-ingest boundary reaches it first. It must name the policy
    # reason, and it must not accept the run.
    assert completeness["failed_sources"], report
    assert "nomos" in report
    assert "source.ingest" in points, decisions
    assert [row["effect"] for row in decisions] == ["deny"] * len(decisions)
    refused = _admission_statuses(dsn, "test")
    assert "accepted" not in refused and "published" not in refused, refused


def _fact_batch_receipt_states(dsn: str) -> list[str]:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT state FROM okg.fact_batch_receipts ORDER BY chunk_ordinal",
        )
        return [str(row[0]) for row in cur.fetchall()]


@pytest.mark.parametrize("rejection", ("deny", "boundary_error"))
def test_source_admission_rejection_compensates_on_the_fence_holder(
    rejection: str,
    tmp_path: Path,
    recovery_private_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    record_property,
) -> None:
    """#2123: a rejection raised inside the finalizer must not wait on itself.

    `_finalize_source_admission` opens one transaction and, once it attaches
    the run's fact-range refs, holds the branch advisory fence
    (`pg_advisory_xact_lock(hashtext('okg.chronos_graph'), hashtext(branch))`
    in `source_mutation_transaction`) for the rest of that transaction. Both
    Nomos rejection paths compensate the run from inside it. Compensation
    opening a second connection there would retract the graph rows on a
    session that waits on the caller's own fence until `lock_timeout`, so
    the run fails 30 seconds later with `LockNotAvailable` and loses the
    decision with the rolled-back transaction --- the observed defect.

    The gate fix alone hides this, because a correctly gated `audit` run
    never enters the rejection path. This drives both paths directly on a
    real database with real staged facts, so removing `graph_conn` at either
    call site, or having `retract_chronos_run` ignore it, fails here.

    `runtime_enforcement` is `off` so the runner's earlier `source.ingest`
    boundary admits the run and the chunks are really applied; the
    finalizer's own boundary is then driven to each rejection.
    """
    # The source runs in a real spawn child; the finalizer's Nomos boundary
    # is stubbed inside that child.
    install_in_source_children(
        monkeypatch, tmp_path, f"{_CHILD_STUBS}:finalizer_denial",
        rejection=rejection,
    )

    dsn = recovery_private_dsn
    root = tmp_path / "deployments"
    dep = create_deployment_scaffold("test", deployments_root=root).deployment_dir
    manifest = yaml.safe_load((dep / "deployment.yaml").read_text())
    manifest["release"] = {"channel": "dev", "pin": {
        "code_repository": str(Path(__file__).resolve().parents[2]),
    }}
    nomos_block = dict(manifest.get("nomos") or {})
    nomos_block["runtime_enforcement"] = "off"
    manifest["nomos"] = nomos_block
    (dep / "deployment.yaml").write_text(yaml.safe_dump(manifest))
    monkeypatch.setenv("OKG_DSN", dsn)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    _git(dep, "init", "-q", "-b", "main")
    _commit_all(dep, "Baseline for the source-admission fence guard")
    assert cli.main([
        "catalog", "load", "--deployment", str(dep), "--apply", "--json",
    ]) == 0
    capsys.readouterr()

    options = IngestOptions(
        dsn=dsn,
        deployment="test",
        deployment_dir=dep,
        owner_id="issue-2123",
        owner_meta={"issue": "2123"},
    )
    started = time.monotonic()
    run = run_ingest(options)
    elapsed = time.monotonic() - started
    report = _redacted_report(run.report, tmp_path)
    record_property(f"fence_{rejection}_elapsed_s", round(elapsed, 3))
    record_property(f"fence_{rejection}_report", report)

    # The wedge signature and its cost, named so a regression reads as itself.
    assert "LockNotAvailable" not in report, report
    assert elapsed < _INGEST_WALL_BOUND_S, f"{elapsed:.1f}s: {report}"

    # The rejection is the one this test asked for, so the fence-holding
    # branch really ran rather than the run completing past it.
    expected_reason = (
        "nomos_source_admission_failed"
        if rejection == "boundary_error"
        else "nomos.test.finalizer_denied"
    )
    assert run.report["completeness"]["failed_sources"], report
    assert expected_reason in report, report
    statuses = _admission_statuses(dsn, "test")
    assert "accepted" not in statuses and "published" not in statuses, statuses

    # Compensation ran to completion on the caller's session. Receipts exist
    # only because chunks were really applied, which is also what puts graph
    # rows behind the fence for the retraction to remove.
    states = _fact_batch_receipt_states(dsn)
    assert states, "the run must have applied receipts to compensate"
    assert set(states) == {"compensated"}, states


def _run_graph_counts(dsn: str, *, source: str, run_id: str) -> tuple[int, int]:
    """Rows this run still has applied on the default branch."""
    with psycopg.connect(dsn) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        return _graph_counts_on(store, source=source, run_id=run_id)


def _graph_counts_on(
    store: ChronosGraphStore, *, source: str, run_id: str,
) -> tuple[int, int]:
    graph = store.checkout("default")
    nodes = graph.query(
        "SELECT count(*) AS n FROM okg.graph_nodes"
        " WHERE source = :source AND run_id = :run_id",
        {"source": source, "run_id": run_id},
    )[0]["n"]
    edges = graph.query(
        "SELECT count(*) AS n FROM okg.graph_edges"
        " WHERE source = :source AND run_id = :run_id",
        {"source": source, "run_id": run_id},
    )[0]["n"]
    return int(nodes), int(edges)


def test_compensation_terminal_state_survives_a_caller_rollback(
    tmp_path: Path,
    recovery_private_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    record_property,
) -> None:
    """#2152: `compensated` must not outlive the graph retraction it claims.

    With `graph_conn` set (#2123 requires it: compensation must not wait on
    the branch fence the caller itself holds) the graph retraction commits
    with the caller's transaction. If the receipt transitions commit on
    compensation's own connection instead, a caller rollback leaves the
    receipts reading `compensated` while the run's rows are still applied ---
    a terminal state that claims an effect the database does not have.

    This drives the real finalizer to a real rejection, makes the very next
    statement fail so the caller's transaction really rolls back, and
    suppresses the orchestrator's own outer retry so the window is measured
    rather than papered over by whoever happens to compensate next. Then it
    runs the retry twice and requires idempotence both times.
    """
    # The source runs in a real spawn child; the denial, the retraction spy
    # and the forced rollback are installed inside that child.
    records = install_in_source_children(
        monkeypatch, tmp_path, f"{_CHILD_STUBS}:rollback_after_compensation",
    )

    dsn = recovery_private_dsn
    root = tmp_path / "deployments"
    dep = create_deployment_scaffold("test", deployments_root=root).deployment_dir
    manifest = yaml.safe_load((dep / "deployment.yaml").read_text())
    manifest["release"] = {"channel": "dev", "pin": {
        "code_repository": str(Path(__file__).resolve().parents[2]),
    }}
    nomos_block = dict(manifest.get("nomos") or {})
    nomos_block["runtime_enforcement"] = "off"
    manifest["nomos"] = nomos_block
    (dep / "deployment.yaml").write_text(yaml.safe_dump(manifest))
    monkeypatch.setenv("OKG_DSN", dsn)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    _git(dep, "init", "-q", "-b", "main")
    _commit_all(dep, "Baseline for the compensation atomicity guard")
    assert cli.main([
        "catalog", "load", "--deployment", str(dep), "--apply", "--json",
    ]) == 0
    capsys.readouterr()

    options = IngestOptions(
        dsn=dsn,
        deployment="test",
        deployment_dir=dep,
        owner_id="issue-2152",
        owner_meta={"issue": "2152"},
    )
    run_ingest(options)

    # The outer handler's second compensation (no fence) was suppressed in
    # the child, so the state below is what the in-transaction one left.
    compensations = read_records(records, "retract")
    fenced = [call for call in compensations if call["held"]]
    assert fenced, compensations
    call = fenced[0]
    source = str(call["source"])
    run_id = str(call["run_id"])

    torn_nodes, torn_edges = _run_graph_counts(dsn, source=source, run_id=run_id)
    torn_states = _fact_batch_receipt_states(dsn)
    record_property("rollback_graph_rows", f"{torn_nodes} nodes {torn_edges} edges")
    record_property("rollback_receipt_states", ",".join(sorted(set(torn_states))))

    # The rollback really happened: the retraction went with it, so the run's
    # rows are back. Without this the rest of the test proves nothing.
    assert torn_nodes + torn_edges > 0, (torn_nodes, torn_edges)

    # The defect. A receipt may read `applied` or `compensating` here --- both
    # are honest about a retraction that did not commit --- but never
    # `compensated`, which asserts rows that are demonstrably still applied.
    assert torn_states, "the run must have applied receipts to compensate"
    assert "compensated" not in torn_states, (torn_states, torn_nodes, torn_edges)

    # The retry, exactly as any later compensator runs it, must reach the
    # terminal state and really remove the rows.
    from okg.substrate.ingest.compensation import retract_chronos_run

    retract_chronos_run(
        dsn,
        admission_id=call["admission_id"],
        source=source,
        run_id=run_id,
        branch=str(call["branch"]),
    )
    assert _run_graph_counts(dsn, source=source, run_id=run_id) == (0, 0)
    assert set(_fact_batch_receipt_states(dsn)) == {"compensated"}

    # A second retry from the terminal state changes nothing and does not raise.
    retract_chronos_run(
        dsn,
        admission_id=call["admission_id"],
        source=source,
        run_id=run_id,
        branch=str(call["branch"]),
    )
    assert _run_graph_counts(dsn, source=source, run_id=run_id) == (0, 0)
    assert set(_fact_batch_receipt_states(dsn)) == {"compensated"}


# Issue #2244: the two #2152 cases PR #2186 left untested. Both run the real
# finalizer under a real, activated Nomos policy bundle --- no stubbed outcome.
_REAL_DENIAL_REASON = "nomos.test.source_run_outcome_denied"


def _real_denial_policy_bundle() -> Any:
    """A real bundle that lets a source run apply its chunks, then denies it.

    The runner's `source.ingest` and `source.emit` boundaries and the
    finalizer's source-run outcome all evaluate the `source.emit` action (the
    first under `source.ingest`). Only the finalizer's request carries an
    `admission_mode` resource fact, and a rule whose `match` names a missing
    fact does not match, so the deny rule reaches exactly one boundary: the
    run-outcome check `_finalize_source_admission` makes after the chunks
    are applied, while it holds the branch fence.
    """
    from okg.substrate.admission import ADMISSION_MODES
    from okg.substrate.nomos import compile_policy_graph

    return compile_policy_graph({
        "policy_bundle_id": "nomos:test:issue-2244",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "deny_source_run_outcome",
                "effect": "deny",
                "actions": ["source.emit"],
                "subjects": ["*"],
                "match": {"admission_mode": sorted(ADMISSION_MODES)},
                "priority": 100,
                "reason_code": _REAL_DENIAL_REASON,
            },
            {
                "rule_id": "allow_source_boundaries",
                "effect": "allow",
                "actions": ["source.ingest", "source.emit"],
                "subjects": ["*"],
                "priority": 10,
                "reason_code": "nomos.test.source_boundary_allowed",
            },
        ],
    })


def _real_denial_deployment(
    tmp_path: Path,
    dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> Path:
    """Scaffold `test` under `enforce` and activate the real denial bundle."""
    from okg.substrate.nomos import activate_policy_bundle, load_active_policy_bundle
    from okg.substrate.nomos.runtime_enforcement import (
        _clear_nomos_enforcement_mode_cache,
    )

    root = tmp_path / "deployments"
    dep = create_deployment_scaffold("test", deployments_root=root).deployment_dir
    manifest = yaml.safe_load((dep / "deployment.yaml").read_text())
    manifest["release"] = {"channel": "dev", "pin": {
        "code_repository": str(Path(__file__).resolve().parents[2]),
    }}
    nomos_block = dict(manifest.get("nomos") or {})
    nomos_block["runtime_enforcement"] = "enforce"
    manifest["nomos"] = nomos_block
    (dep / "deployment.yaml").write_text(yaml.safe_dump(manifest))
    monkeypatch.setenv("OKG_DSN", dsn)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    _clear_nomos_enforcement_mode_cache()
    _git(dep, "init", "-q", "-b", "main")
    _commit_all(dep, "Baseline under a real denying Nomos bundle")
    assert cli.main([
        "catalog", "load", "--deployment", str(dep), "--apply", "--json",
    ]) == 0
    capsys.readouterr()

    bundle = _real_denial_policy_bundle()
    with psycopg.connect(dsn) as conn:
        # `enforce` with no active bundle denies every activation except the
        # one-time bootstrap, which is how an operator installs the first one.
        activate_policy_bundle(
            conn,
            deployment_name="test",
            bundle=bundle,
            scope="deployment:test",
            activated_by="test:operator",
            bootstrap=True,
        )
        conn.commit()
        active = load_active_policy_bundle(
            conn, deployment_name="test", scope="deployment:test",
        )
    assert active is not None
    assert active.policy_fingerprint == bundle.policy_fingerprint
    return dep


def _spy_real_run_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> list[tuple[bool, str | None]]:
    """Record the real finalizer decision without replacing it.

    The decision row the finalizer persists commits with its transaction, so
    a test that rolls that transaction back cannot read it afterwards; this
    keeps the real outcome observable.
    """
    import okg.substrate.nomos as nomos

    real = nomos.authorize_source_run_outcome_with_active_policy
    seen: list[tuple[bool, str | None]] = []

    def _spy(*args: Any, **kwargs: Any) -> Any:
        outcome = real(*args, **kwargs)
        seen.append((
            outcome is not None and bool(outcome.allowed),
            None if outcome is None else outcome.decision.reason_code,
        ))
        return outcome

    monkeypatch.setattr(
        nomos, "authorize_source_run_outcome_with_active_policy", _spy,
    )
    return seen


def _receipt_rows(dsn: str) -> list[tuple[str, int, str]]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        return _receipt_rows_on(conn)


def _receipt_rows_on(conn: Any) -> list[tuple[str, int, str]]:
    """Receipt rows as `conn` sees them, inside its open transaction if any."""
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            """
            SELECT admission_id::text, chunk_ordinal, state
              FROM okg.fact_batch_receipts
             ORDER BY admission_id, run_id, chunk_ordinal
            """,
        )
        return [(str(a), int(o), str(s)) for a, o, s in cur.fetchall()]


def test_real_policy_denial_rollback_leaves_receipt_and_graph_unchanged(
    tmp_path: Path,
    recovery_private_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    record_property,
) -> None:
    """#2244 case 2: a real Nomos denial, not a stubbed outcome.

    A real activated bundle denies the run at the finalizer, after its
    chunks are applied. The finalizer compensates on its own fenced
    connection; the very next write then fails, so the finalizer's
    transaction really rolls back. The test checks that the real retraction
    returned, that on the finalizer's connection it had already removed the
    run's rows and marked every receipt `compensated`, and that the forced
    failure fired: the rollback, not a skipped retraction, is what leaves
    the graph as it was. With one transaction owner (#2152) the
    rollback takes the retraction AND the `compensated` transition with it:
    the graph keeps exactly the rows it had and no receipt claims them
    retracted. If the two halves could commit separately, the receipts read
    `compensated` while the rows are still applied.
    """
    from okg.substrate.ingest.compensation import retract_chronos_run

    dsn = recovery_private_dsn
    dep = _real_denial_deployment(tmp_path, dsn, monkeypatch, capsys)
    # The source runs in a real spawn child; the outcome spy, the fenced
    # retraction observer and the forced rollback are installed there.
    records = install_in_source_children(
        monkeypatch, tmp_path, f"{_CHILD_STUBS}:real_denial_rollback",
    )

    run_ingest(IngestOptions(
        dsn=dsn,
        deployment="test",
        deployment_dir=dep,
        owner_id="issue-2244",
        owner_meta={"issue": "2244"},
    ))

    outcomes = [
        (row["allowed"], row["reason"]) for row in read_records(records, "outcome")
    ]
    forced_failures = [row["status"] for row in read_records(records, "forced_failure")]
    fenced = read_records(records, "fenced")
    # The real bundle, not a stub, produced the denial the finalizer acted on.
    assert (False, _REAL_DENIAL_REASON) in outcomes, outcomes
    assert len(fenced) == 1, fenced
    call = fenced[0]
    source, run_id = str(call["source"]), str(call["run_id"])
    rows_before = tuple(call["rows_before"])
    receipts_before = [tuple(row) for row in call["receipts_before"]]
    call["rows_in_txn"] = tuple(call.get("rows_in_txn") or ())
    call["receipts_in_txn"] = [tuple(row) for row in call.get("receipts_in_txn") or ()]
    assert sum(rows_before) > 0, "the run must have applied graph rows"
    assert receipts_before, "the run must have applied receipts"
    assert {state for *_key, state in receipts_before} == {"applied"}

    # The real retraction ran to completion inside the finalizer's
    # transaction, and only then did the forced failure roll it back. Without
    # these, "the graph is unchanged" could mean the retraction never ran.
    assert call["retract_returned"] is True, call
    assert forced_failures == ["rejected"], forced_failures
    record_property(
        "in_txn_before_rollback",
        f"{call['rows_in_txn']} "
        f"{sorted({state for *_key, state in call['receipts_in_txn']})}",
    )
    assert call["rows_in_txn"] == (0, 0), call["rows_in_txn"]
    assert [key for *key, _state in call["receipts_in_txn"]] == [
        key for *key, _state in receipts_before
    ]
    assert {state for *_key, state in call["receipts_in_txn"]} == {
        "compensated",
    }, call["receipts_in_txn"]

    rows_after = _run_graph_counts(dsn, source=source, run_id=run_id)
    receipts_after = _receipt_rows(dsn)
    record_property("rows_before_after", f"{rows_before} -> {rows_after}")
    record_property(
        "receipt_states_after",
        ",".join(state for *_key, state in receipts_after),
    )

    # The graph is exactly as the denial found it: the retraction rolled back.
    assert rows_after == rows_before, (rows_before, rows_after)
    # The same receipts, none added, and none claiming the rows are gone.
    # `compensating` is the in-flight marker committed before the retraction
    # started; it is honest about a retraction that did not commit.
    assert [key for *key, _state in receipts_after] == [
        key for *key, _state in receipts_before
    ]
    states_after = {state for *_key, state in receipts_after}
    assert "compensated" not in states_after, (receipts_after, rows_after)
    assert states_after <= {"applied", "compensating"}, receipts_after

    # A later compensator converges: rows gone, every receipt terminal.
    retract_chronos_run(
        dsn,
        admission_id=call["admission_id"],
        source=source,
        run_id=run_id,
        branch=str(call["branch"]),
    )
    assert _run_graph_counts(dsn, source=source, run_id=run_id) == (0, 0)
    assert {state for *_key, state in _receipt_rows(dsn)} == {"compensated"}


def _blocked_on_idle_transaction(dsn: str) -> list[tuple[int, int, str]]:
    """(waiter, blocker, lock type) where a backend waits on a paused transaction.

    The blocker must be `idle in transaction`: a backend holding locks while
    its client is parked, which is exactly the gated first compensation. The
    lock type is the `pg_locks.locktype` of the lock the waiter has not been
    granted (`advisory` for the branch fence `apply_facts` takes).
    """
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT waiter.pid, blocker.pid, wanted.locktype
              FROM pg_stat_activity AS waiter
              JOIN pg_stat_activity AS blocker
                ON blocker.pid = ANY (pg_blocking_pids(waiter.pid))
              JOIN pg_locks AS wanted
                ON wanted.pid = waiter.pid AND NOT wanted.granted
             WHERE waiter.datname = current_database()
               AND waiter.wait_event_type = 'Lock'
               AND blocker.state = 'idle in transaction'
            """,
        )
        return [(int(w), int(b), str(t)) for w, b, t in cur.fetchall()]


def _database_digest(dsn: str) -> dict[str, str]:
    """An md5 of every row of every user table, keyed by table.

    Two equal digests mean no committed write landed in between: no graph
    row, interval, receipt, audit, or log row was added, changed, or removed.
    """
    digests: dict[str, str] = {}
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT schemaname, relname FROM pg_stat_user_tables ORDER BY 1, 2",
        )
        for schema, table in cur.fetchall():
            cur.execute(
                sql.SQL(
                    "SELECT count(*), md5(coalesce("
                    "string_agg(t::text, '|' ORDER BY t::text), ''))"
                    " FROM {}.{} AS t",
                ).format(sql.Identifier(schema), sql.Identifier(table)),
            )
            count, digest = cur.fetchone()
            digests[f"{schema}.{table}"] = f"{count}:{digest}"
    return digests


@pytest.mark.parametrize("overlap", ("effect", "marker"))
def test_racing_compensations_retract_once_and_never_tear(
    overlap: str,
    tmp_path: Path,
    recovery_private_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    record_property,
) -> None:
    """#2244 case 1: two compensations of the same run, forced to overlap.

    A real denial leaves a run with applied receipts and graph rows (every
    in-ingest compensation is suppressed so the run stays that way). Two
    threads then run `retract_chronos_run` for it, each on its own
    connection --- the path a retry or a second worker takes.

    The overlap is forced by gates and Postgres lock state, never by sleeps:

    * `effect`: the first compensation stops at its first `compensating ->
      compensated` receipt transition, after its graph retraction ran and
      before its effect transaction ends. An outside reader must then see
      the run whole (rows applied, receipts `compensating`); any commit
      between the retraction and the terminal transition, or a terminal
      transition committed ahead of the retraction, shows up here as rows
      gone or a receipt already `compensated`. The second compensation
      starts only then. The test releases the first only once Postgres
      reports the second waiting, on an `advisory` lock, for the first's
      idle open transaction: the branch fence `apply_facts` takes and holds
      until that transaction ends. The second stops after its own
      `apply_facts`, and its commit leaves every table byte-for-byte as the
      first left it.
    * `marker`: the second compensation reads the receipts as `applied` and
      stops just before claiming them. The first runs to completion. The
      second then refuses with a typed receipt-transition error and writes
      nothing.

    Either way exactly one compensation changes the database, the receipts
    are the same rows ending `compensated`, and the run's rows are gone.
    """
    from okg.substrate.fact_batch import (
        ApplicationReceiptTransitionError,
        PostgresApplicationReceiptStore,
    )
    from okg.substrate.ingest.compensation import retract_chronos_run

    dsn = recovery_private_dsn
    dep = _real_denial_deployment(tmp_path, dsn, monkeypatch, capsys)
    # The source runs in a real spawn child with every in-ingest
    # compensation suppressed there; the race below runs in this process.
    records = install_in_source_children(
        monkeypatch, tmp_path, f"{_CHILD_STUBS}:real_denial_suppressed",
    )
    run_ingest(IngestOptions(
        dsn=dsn,
        deployment="test",
        deployment_dir=dep,
        owner_id="issue-2244",
        owner_meta={"issue": "2244"},
    ))
    outcomes = [
        (row["allowed"], row["reason"]) for row in read_records(records, "outcome")
    ]
    suppressed = read_records(records, "suppressed")
    assert (False, _REAL_DENIAL_REASON) in outcomes, outcomes
    assert suppressed, "the real denial must have asked for compensation"
    call = suppressed[0]
    source, run_id = str(call["source"]), str(call["run_id"])
    kwargs = {
        "admission_id": call["admission_id"],
        "source": source,
        "run_id": run_id,
        "branch": str(call["branch"]),
    }
    rows_before = _run_graph_counts(dsn, source=source, run_id=run_id)
    receipts_before = _receipt_rows(dsn)
    assert sum(rows_before) > 0, "the run must have applied graph rows"
    assert {state for *_key, state in receipts_before} == {"applied"}

    first_name, second_name = "compensation-first", "compensation-second"
    reached = {first_name: threading.Event(), second_name: threading.Event()}
    release = {first_name: threading.Event(), second_name: threading.Event()}

    def _park(name: str) -> None:
        reached[name].set()
        if not release[name].wait(60):
            raise RuntimeError(f"test gate for {name} was never released")

    real_apply = ChronosGraphStore.apply_facts
    real_transition = PostgresApplicationReceiptStore.transition

    def _gated_apply(self: Any, facts: Any, **apply_kwargs: Any) -> Any:
        result = real_apply(self, facts, **apply_kwargs)
        name = threading.current_thread().name
        if overlap == "effect" and name == second_name:
            # The second is past the first's fence, before its own commit.
            _park(name)
        return result

    first_terminal_parked: list[bool] = []

    def _gated_transition(self: Any, key: Any, state: str, **kw: Any) -> Any:
        name = threading.current_thread().name
        if (
            overlap == "effect"
            and name == first_name
            and state == "compensated"
            and not first_terminal_parked
        ):
            # The retraction has run and the first receipt is about to go
            # terminal: the last point before the effect transaction ends.
            # Any commit the retraction made on its own is visible from here.
            first_terminal_parked.append(True)
            _park(name)
        if overlap == "marker" and name == second_name and state == "compensating":
            # The receipts were read as `applied`; nothing is claimed yet.
            _park(name)
        return real_transition(self, key, state, **kw)

    monkeypatch.setattr(ChronosGraphStore, "apply_facts", _gated_apply)
    monkeypatch.setattr(
        PostgresApplicationReceiptStore, "transition", _gated_transition,
    )

    results: dict[str, BaseException | None] = {}

    def _compensate() -> None:
        name = threading.current_thread().name
        try:
            retract_chronos_run(dsn, **kwargs)
        except BaseException as exc:  # noqa: BLE001 - reported below
            results[name] = exc
        else:
            results[name] = None

    first = threading.Thread(target=_compensate, name=first_name)
    second = threading.Thread(target=_compensate, name=second_name)
    try:
        if overlap == "effect":
            first.start()
            assert reached[first_name].wait(60), results

            # The first retraction has run but its transaction is open. An
            # outside reader sees the run whole, never rows gone under a live
            # receipt nor a receipt claiming rows that are still there.
            mid_rows = _run_graph_counts(dsn, source=source, run_id=run_id)
            mid_states = {state for *_key, state in _receipt_rows(dsn)}
            record_property("mid_race", f"{mid_rows} {sorted(mid_states)}")
            assert (mid_rows, mid_states) == (rows_before, {"compensating"}), (
                "an outside reader saw a half-committed compensation",
                mid_rows,
                mid_states,
            )

            second.start()
            # Postgres, not a timer, says when the overlap exists: the second
            # compensation is waiting on the first's parked transaction.
            deadline = time.monotonic() + 20
            blocked: list[tuple[int, int, str]] = []
            while time.monotonic() < deadline and second.is_alive():
                blocked = _blocked_on_idle_transaction(dsn)
                if blocked:
                    break
                time.sleep(0.05)
            record_property("second_blocked_on", repr(blocked))
            assert blocked and second.is_alive(), (
                "the second compensation must wait on the first's open "
                "effect transaction, not finish around it",
                results,
            )
            # What it waits on is the branch fence `apply_facts` takes, held
            # by the first until its effect transaction ends.
            assert {locktype for *_pids, locktype in blocked} == {"advisory"}, (
                blocked
            )
            assert not reached[second_name].is_set(), results
            release[first_name].set()
            first.join(60)
            assert results.get(first_name, "running") is None, results
            # The second is past the lock and parked before its commit.
            assert reached[second_name].wait(60), results
            winner_state = _database_digest(dsn)
            release[second_name].set()
            second.join(60)
            assert results.get(second_name, "running") is None, results
        else:
            second.start()
            assert reached[second_name].wait(60), results
            first.start()
            first.join(60)
            assert results.get(first_name, "running") is None, results
            winner_state = _database_digest(dsn)
            release[second_name].set()
            second.join(60)
            refusal = results.get(second_name)
            assert isinstance(refusal, ApplicationReceiptTransitionError), results
            assert "'compensated' -> 'compensating'" in str(refusal), refusal
    finally:
        for event in release.values():
            event.set()
        for thread in (first, second):
            if thread.ident is not None:
                thread.join(60)
    assert not first.is_alive() and not second.is_alive()
    record_property(
        "race_results",
        {name: None if exc is None else repr(exc) for name, exc in results.items()},
    )

    # The loser changed nothing: every table is as the winner left it.
    loser_changed = {
        table: (winner_state.get(table), digest)
        for table, digest in _database_digest(dsn).items()
        if winner_state.get(table) != digest
    }
    assert loser_changed == {}, loser_changed

    # One effect: the run's rows are gone, and the same receipt rows --- none
    # duplicated, none added --- all end `compensated`.
    assert _run_graph_counts(dsn, source=source, run_id=run_id) == (0, 0)
    receipts_after = _receipt_rows(dsn)
    assert [key for *key, _state in receipts_after] == [
        key for *key, _state in receipts_before
    ]
    assert {state for *_key, state in receipts_after} == {"compensated"}
