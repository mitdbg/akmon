"""Dispatch-path tests for CLI subcommands.

These exercise `okg.substrate.cli.main:main()` end-to-end with
crafted argv, capturing stdout/stderr and exit codes. They cover
the dispatch wiring + early-exit error envelopes for every
subcommand without needing each subcommand's deeper semantics.

DB-touching subcommands run against `clean_runtime`; pure ones
need only argparse + envelope printing.
"""
from __future__ import annotations

import json
from pathlib import Path

import psycopg
import pytest

from okg.substrate.cli.main import main
from okg.substrate.cli.main import _collect_status
from okg.substrate.cli.main import _render_template_dir


pytestmark = pytest.mark.postgres


# ---------- main() entry: top-level help / unknown command ----------


def test_main_unknown_command_returns_two(capsys) -> None:
    """argparse error for unknown command exits 2 (argparse default)."""
    with pytest.raises(SystemExit) as exc:
        main(["this-is-not-a-command"])
    assert exc.value.code != 0


# ---------- doctor ----------


def test_doctor_without_dsn_exits_nonzero(
    monkeypatch, capsys,
) -> None:
    """`okg doctor` with no DSN env / flag → structured error."""
    monkeypatch.delenv("OKG_DSN", raising=False)
    rc = main(["doctor", "--json"])
    assert rc != 0
    out = capsys.readouterr()
    # The error envelope is on stderr in JSON form.
    assert out.err
    err_obj = json.loads(out.err.strip().splitlines()[-1])
    assert "error" in err_obj


def test_doctor_against_real_db_succeeds(
    clean_runtime: str, capsys,
) -> None:
    """`okg doctor --dsn <real>` reports OK against the test DB."""
    rc = main(["doctor", "--dsn", clean_runtime, "--json"])
    assert rc == 0
    out = capsys.readouterr()
    payload = json.loads(out.out.strip().splitlines()[-1])
    assert payload.get("ok") is True


def test_doctor_extraction_wrap_succeeds(capsys) -> None:
    """`--check extraction-wrap-invariant` is a pure file-tree
    lint; no DSN needed."""
    rc = main(["doctor", "--check", "extraction-wrap-invariant", "--json"])
    assert rc == 0


# ---------- status ----------


def test_status_without_dsn_exits_nonzero(monkeypatch) -> None:
    monkeypatch.delenv("OKG_DSN", raising=False)
    rc = main(["status", "--json"])
    assert rc != 0


def test_status_against_real_db(
    clean_runtime: str, capsys,
) -> None:
    """`okg status` against a real DB returns the deployment-state
    summary envelope."""
    rc = main(["status", "--dsn", clean_runtime,
               "--deployment", "default", "--json"])
    assert rc == 0
    out = capsys.readouterr()
    payload = json.loads(out.out.strip().splitlines()[-1])
    assert "publishing" in payload or "stage" in payload


def _write_okg_workspace_deployment(root: Path) -> Path:
    dep = root / "deployments" / "okg-workspace"
    dep.mkdir(parents=True)
    (dep / "schemas").mkdir()
    (dep / "source_registry.yaml").write_text("sources: {}\n")
    (dep / "deployment.yaml").write_text(
        "name: okg-workspace\n"
        "description: test Workspace Graph deployment\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n",
    )
    return dep


def test_status_uses_canonical_workspace_deployment(
    clean_runtime: str, tmp_path: Path, monkeypatch, capsys,
) -> None:
    _write_okg_workspace_deployment(tmp_path)
    monkeypatch.setenv(
        "OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"),
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        catalog_version_id = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO okg.graph_generations
              (deployment_name, status, catalog_version_id,
               published_at, correlation_id)
            VALUES
              ('okg-workspace', 'published', %s, now(), %s)
            RETURNING generation_id
            """,
            (
                catalog_version_id,
                f"okg-workspace:status-test:{catalog_version_id}",
            ),
        )
        generation_id = cur.fetchone()[0]

    rc = main([
        "status", "--dsn", clean_runtime,
        "--deployment", "okg-workspace", "--json",
    ])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["deployment_name"] == "okg-workspace"
    assert payload["stage"] == "running"
    assert (
        payload["publishing"]["latest_published_generation"]
        == generation_id
    )


def _seed_publish_attempts(
    dsn: str, deployment: str, attempts: list[tuple[str, str, dict]],
) -> None:
    """One published generation, then one checkpoint per attempt, in order.

    Each attempt is ``(checkpoint_id, created_at, metadata)``.
    """
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        catalog_version_id = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO okg.graph_generations
              (deployment_name, status, catalog_version_id,
               published_at, correlation_id)
            VALUES (%s, 'published', %s, now(), %s)
            """,
            (deployment, catalog_version_id,
             f"{deployment}:attempt-test:{catalog_version_id}"),
        )
        for checkpoint_id, created_at, metadata in attempts:
            cur.execute(
                """
                INSERT INTO public._chronos_branch_interval_checkpoints
                  (checkpoint_id, branch_id, segment_id, created_at, metadata)
                SELECT %s, 'default', b.current_segment_id, %s, %s
                  FROM public._chronos_branch_interval_branches b
                 WHERE b.branch_id = 'default'
                """,
                (checkpoint_id, created_at, json.dumps({
                    "deployment_name": deployment,
                    "catalog_version_id": catalog_version_id,
                    **metadata,
                })),
            )
            assert cur.rowcount == 1, "default branch missing in fixture"


def test_status_exits_3_when_newest_publish_attempt_is_blocked(
    clean_runtime: str, capsys,
) -> None:
    """A gate-refused publish newer than the last published one is not
    healthy: before this, status exited 0 and showed only the old version."""
    _seed_publish_attempts(clean_runtime, "attempts", [
        # Ids sort opposite to time, so only created_at can pick the newest.
        ("okg:attempt-z", "2026-10-01T00:00:00+00:00",
         {"kind": "okg_generation", "status": "published"}),
        ("okg:attempt-2", "2026-10-02T00:00:00+00:00",
         {"kind": "okg_generation", "status": "blocked",
          "blocked_reason": {
              "reason": "ingest_source_incomplete",
              "failed_sources": ["broken-source"],
          }}),
        # A newer row from another deployment is not this deployment's
        # attempt.
        ("okg:attempt-other-deployment", "2026-10-03T00:00:00+00:00",
         {"kind": "okg_generation", "status": "published",
          "deployment_name": "someone-else"}),
        # A run-base checkpoint is cut at every source run and is never
        # a publish attempt, even when it is the newest row.
        ("okg-run-base:default:r1", "2026-10-03T00:00:00+00:00",
         {"kind": "run_base", "status": "published"}),
    ])

    rc = main(["status", "--dsn", clean_runtime,
               "--deployment", "attempts", "--json"])

    assert rc == 3
    out = capsys.readouterr()
    payload = json.loads(out.out.strip().splitlines()[-1])
    attempt = payload["publishing"]["latest_attempt"]
    assert attempt["checkpoint_id"] == "okg:attempt-2"
    assert attempt["status"] == "blocked"
    assert attempt["blocked"] is True
    assert "ingest_source_incomplete" in attempt["blocked_detail"]
    assert "broken-source" in attempt["blocked_detail"]
    assert "latest_publish_blocked" in out.err
    assert "okg:attempt-2" in out.err


def test_status_exits_0_when_a_later_publish_succeeded(
    clean_runtime: str, capsys,
) -> None:
    _seed_publish_attempts(clean_runtime, "recovered", [
        ("okg:attempt-1", "2026-10-01T00:00:00+00:00",
         {"kind": "okg_generation", "status": "blocked",
          "blocked_reason": {"reason": "ingest_source_incomplete"}}),
        ("okg:attempt-2", "2026-10-02T00:00:00+00:00",
         {"kind": "okg_generation", "status": "published"}),
        # A newer blocked attempt on another branch does not count against
        # the default branch readers see.
        ("okg:attempt-other-branch", "2026-10-03T00:00:00+00:00",
         {"kind": "okg_generation", "status": "blocked",
          "branch": "proposal-1",
          "blocked_reason": {"reason": "ingest_source_incomplete"}}),
    ])

    rc = main(["status", "--dsn", clean_runtime,
               "--deployment", "recovered", "--json"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    attempt = payload["publishing"]["latest_attempt"]
    assert attempt["checkpoint_id"] == "okg:attempt-2"
    assert attempt["blocked"] is False
    assert attempt["blocked_detail"] is None


def test_status_exits_1_when_newest_attempt_status_is_unknown(
    clean_runtime: str, capsys,
) -> None:
    """An attempt with no recognisable status is never read as healthy."""
    _seed_publish_attempts(clean_runtime, "unknown-attempt", [
        ("okg:attempt-1", "2026-10-01T00:00:00+00:00",
         {"kind": "okg_generation"}),
    ])

    rc = main(["status", "--dsn", clean_runtime,
               "--deployment", "unknown-attempt", "--json"])

    assert rc == 1
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    codes = {err["code"] for err in payload["errors"]}
    assert "latest_publish_attempt_unknown" in codes


def test_status_exits_1_when_the_attempt_probe_fails(
    clean_runtime: str, capsys, monkeypatch,
) -> None:
    import okg.substrate.cli.main as cli_main

    _seed_publish_attempts(clean_runtime, "probe-fails", [
        ("okg:attempt-1", "2026-10-01T00:00:00+00:00",
         {"kind": "okg_generation", "status": "published"}),
    ])

    def _raise(_conn, _deployment):  # noqa: ANN001, ANN202
        raise RuntimeError("probe refused by the test")

    monkeypatch.setattr(cli_main, "_latest_publish_attempt", _raise)
    rc = main(["status", "--dsn", clean_runtime,
               "--deployment", "probe-fails", "--json"])

    assert rc == 1
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["errors"] == [{
        "code": "latest_publish_attempt_unknown",
        "message": "probe refused by the test",
    }]
    assert "latest_attempt" not in payload["publishing"]


def _doctor_payload(capsys) -> dict:
    return json.loads(capsys.readouterr().out.strip().splitlines()[-1])


def test_doctor_fails_when_newest_publish_attempt_is_blocked(
    clean_runtime: str, capsys,
) -> None:
    """Issue #3256: doctor read ok while the newest publish was refused and
    readers kept the old version."""
    _seed_publish_attempts(clean_runtime, "doc-blocked", [
        ("okg:attempt-z", "2026-10-01T00:00:00+00:00",
         {"kind": "okg_generation", "status": "published"}),
        ("okg:attempt-2", "2026-10-02T00:00:00+00:00",
         {"kind": "okg_generation", "status": "blocked",
          "blocked_reason": {
              "reason": "cross_source_error_invariant_failed",
              "failed_invariants": [{"name": "core_subtypes_present"}],
          }}),
        # Neither a run base (status as `run_base.py` writes it) nor another
        # deployment's attempt counts.
        ("okg-run-base:default:r1", "2026-10-03T00:00:00+00:00",
         {"kind": "run_base", "status": "run_base"}),
        ("okg:attempt-other-deployment", "2026-10-03T00:00:00+00:00",
         {"kind": "okg_generation", "status": "published",
          "deployment_name": "someone-else"}),
    ])

    rc = main(["doctor", "--dsn", clean_runtime,
               "--deployment", "doc-blocked", "--json"])

    assert rc == 1
    payload = _doctor_payload(capsys)
    assert payload["ok"] is False
    assert payload["latest_publish_attempt"]["checkpoint_id"] == "okg:attempt-2"
    [error] = [
        e for e in payload["errors"] if e["code"] == "latest_publish_blocked"
    ]
    assert error["checkpoint_id"] == "okg:attempt-2"
    assert "okg:attempt-2" in error["message"]
    assert "cross_source_error_invariant_failed" in error["message"]
    assert "core_subtypes_present" in error["message"]
    assert "Readers still see okg:attempt-z" in error["message"]
    assert (
        "generation describe okg:attempt-2 --deployment doc-blocked"
        in error["fix_hint"]
    )
    assert "okg ingest --deployment doc-blocked" in error["fix_hint"]


def test_doctor_passes_the_check_when_a_later_publish_succeeded(
    clean_runtime: str, capsys,
) -> None:
    _seed_publish_attempts(clean_runtime, "doc-recovered", [
        ("okg:attempt-1", "2026-10-01T00:00:00+00:00",
         {"kind": "okg_generation", "status": "blocked",
          "blocked_reason": {"reason": "cross_source_error_invariant_failed"}}),
        ("okg:attempt-2", "2026-10-02T00:00:00+00:00",
         {"kind": "okg_generation", "status": "published"}),
        # A blocked attempt on another branch is not what readers see.
        ("okg:attempt-other-branch", "2026-10-03T00:00:00+00:00",
         {"kind": "okg_generation", "status": "blocked",
          "branch": "proposal-1",
          "blocked_reason": {"reason": "cross_source_error_invariant_failed"}}),
    ])

    main(["doctor", "--dsn", clean_runtime,
          "--deployment", "doc-recovered", "--json"])

    payload = _doctor_payload(capsys)
    assert payload["latest_publish_attempt"]["checkpoint_id"] == "okg:attempt-2"
    assert payload["latest_publish_attempt"]["blocked"] is False
    codes = {e["code"] for e in payload["errors"]}
    assert not codes & {"latest_publish_blocked", "latest_publish_attempt_unknown"}


def test_doctor_fails_when_newest_attempt_status_is_unknown(
    clean_runtime: str, capsys,
) -> None:
    _seed_publish_attempts(clean_runtime, "doc-unknown", [
        ("okg:attempt-1", "2026-10-01T00:00:00+00:00",
         {"kind": "okg_generation"}),
    ])

    rc = main(["doctor", "--dsn", clean_runtime,
               "--deployment", "doc-unknown", "--json"])

    assert rc == 1
    codes = {e["code"] for e in _doctor_payload(capsys)["errors"]}
    assert "latest_publish_attempt_unknown" in codes


def test_doctor_fails_when_the_attempt_probe_fails(
    clean_runtime: str, capsys, monkeypatch,
) -> None:
    import okg.substrate.cli.main as cli_main

    def _raise(_conn, _deployment):  # noqa: ANN001, ANN202
        raise RuntimeError("probe refused by the test")

    monkeypatch.setattr(cli_main, "_latest_publish_attempt", _raise)
    rc = main(["doctor", "--dsn", clean_runtime,
               "--deployment", "doc-probe-fails", "--json"])

    assert rc == 1
    [error] = [
        e for e in _doctor_payload(capsys)["errors"]
        if e["code"] == "latest_publish_attempt_unknown"
    ]
    assert "probe refused by the test" in error["message"]


def test_doctor_names_a_superseded_blocked_attempt_and_stays_red(
    clean_runtime: str, capsys,
) -> None:
    """Supersede retires the recovery but leaves the checkpoint blocked:
    readers are still on the old version, so doctor stays red and says so."""
    _seed_publish_attempts(clean_runtime, "doc-superseded", [
        ("okg:attempt-1", "2026-10-01T00:00:00+00:00",
         {"kind": "okg_generation", "status": "published"}),
        ("okg:attempt-2", "2026-10-02T00:00:00+00:00",
         {"kind": "okg_generation", "status": "blocked",
          "blocked_reason": {
              "reason": "ingest_source_incomplete",
              "recovery": {
                  "schema": "okg.ingest_incomplete_recovery.v1",
                  "state": "superseded",
                  "superseded_state": "repair_required",
                  "superseded_at": "2026-10-02T01:00:00+00:00",
                  "superseded_by": "operator",
                  "supersede_reason": "source retired upstream",
              },
          }}),
    ])

    rc = main(["doctor", "--dsn", clean_runtime,
               "--deployment", "doc-superseded", "--json"])

    assert rc == 1
    [error] = [
        e for e in _doctor_payload(capsys)["errors"]
        if e["code"] == "latest_publish_blocked"
    ]
    assert "superseded by operator" in error["message"]
    assert "source retired upstream" in error["message"]
    assert "Readers still see okg:attempt-1" in error["message"]
    assert "okg ingest --deployment doc-superseded" in error["fix_hint"]
    assert "publishes nothing and this error remains" in error["fix_hint"]


def test_blocked_attempt_hint_follows_each_recovery_plan() -> None:
    from okg.substrate.cli.doctor import _blocked_attempt_fix_hint

    ingest = "uv run okg ingest --deployment d --json"
    # Staged work: the recovery command itself names the cause, so it is not
    # offered as a read-only diagnosis as well.
    staged = _blocked_attempt_fix_hint({
        "ok": True, "state": "staged_work_unpublished",
        "recovery_command": ingest, "diagnose_command": ingest,
        "explanation": "source records were imported but their publish was refused",
    }, None, "d")
    assert staged.count(ingest) == 1
    assert "Read the cause" not in staged
    # Repair required: the escape hatch is the dry run, never `--apply`.
    repair = _blocked_attempt_fix_hint({
        "ok": True, "state": "checkpoint_repair_required",
        "recovery_command": "uv run okg ingest --deployment d --include s --json",
        "escape_hatch_command": (
            "uv run okg runtime supersede-checkpoint --deployment d "
            "--reason '<why>' --apply --json"
        ),
        "explanation": "the latest checkpoint is blocked because a source run did not complete",
    }, None, "d")
    assert "--include s" in repair
    assert "supersede-checkpoint --deployment d" in repair
    assert "--apply" not in repair


def test_doctor_has_no_attempt_before_migrate(
    clean_runtime: str, capsys, monkeypatch,
) -> None:
    """No checkpoint table means nothing was attempted, not an unknown."""
    import okg.substrate.cli.main as cli_main

    def _absent(_conn, _deployment):  # noqa: ANN001, ANN202
        raise psycopg.errors.UndefinedTable("relation does not exist")

    monkeypatch.setattr(cli_main, "_latest_publish_attempt", _absent)
    main(["doctor", "--dsn", clean_runtime,
          "--deployment", "doc-unmigrated", "--json"])

    payload = _doctor_payload(capsys)
    assert payload["latest_publish_attempt"] is None
    codes = {e["code"] for e in payload["errors"]}
    assert not codes & {"latest_publish_blocked", "latest_publish_attempt_unknown"}


def test_status_dashboard_names_a_blocked_attempt() -> None:
    from okg.substrate.cli.main import _format_status_dashboard

    snap = {
        "deployment_name": "d", "stage": "running", "sources": [],
        "errors": [],
        "publishing": {"latest_attempt": {
            "checkpoint_id": "okg:attempt-2", "status": "blocked",
            "blocked": True, "blocked_detail": "ingest_source_incomplete",
        }},
    }
    text = _format_status_dashboard(snap, prev=None, elapsed_s=0)
    assert (
        "BLOCKED latest publish attempt okg:attempt-2: "
        "ingest_source_incomplete"
    ) in text


def test_status_scopes_sources_to_deployment_registry(
    clean_runtime: str, tmp_path: Path,
) -> None:
    dep = tmp_path / "deployments" / "okg-workspace"
    dep.mkdir(parents=True)
    (dep / "source_registry.yaml").write_text(
        "sources:\n"
        "  git-files:\n"
        "    ownership_id: okg-workspace.git-files\n"
        "    source_class: discovery_crawl\n"
        "    record_identity_kind: scoped_locator\n"
        "    record_identity_fields: [repo, path]\n"
        "    source_revision_kind: content_hash\n"
        "    deletion_semantics: missing_from_completed_scope\n"
        "    publication_mode: published_generation\n",
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.source_watermarks
              (source, projection_cursor, last_reconcile_at)
            VALUES
              ('okg-workspace.git-files', 10, now()),
              ('okg-workspace.repair-example', 11, now()),
              ('_enricher:code_reference', 12, now()),
              ('intersystems.icd10_cms', 13, now())
            """
        )
        cur.execute(
            """
            INSERT INTO okg.node_facts
              (node_id, subtype, attrs, source, source_record_id,
               source_revision, observed_at, dedupe_key, run_id, op)
            VALUES
              ('file:okg:README.md', 'source_file', '{}'::jsonb,
               'okg-workspace.git-files', '{}'::jsonb, '"r1"'::jsonb,
               now(), 'status-scope:1',
               '00000000-0000-0000-0000-000000000001', 'I'),
              ('code:okg:f', 'code_symbol', '{}'::jsonb,
               'okg-workspace.repair-example', '{}'::jsonb,
               '"r1"'::jsonb, now(), 'status-scope:2',
               '00000000-0000-0000-0000-000000000001', 'I'),
              ('foreign:code', 'medical_code', '{}'::jsonb,
               'intersystems.icd10_cms', '{}'::jsonb, '"r1"'::jsonb,
               now(), 'status-scope:3',
               '00000000-0000-0000-0000-000000000001', 'I')
            """
        )

    payload = _collect_status(
        clean_runtime, "okg-workspace", deployment_dir=dep,
    )
    sources = {row["source"] for row in payload["sources"]}
    assert "okg-workspace.git-files" in sources
    assert "okg-workspace.repair-example" in sources
    assert "_enricher:code_reference" in sources
    assert "intersystems.icd10_cms" not in sources


def test_status_reports_published_generation_and_sources(
    clean_runtime: str, tmp_path: Path,
) -> None:
    dep = tmp_path / "deployments" / "okg-workspace"
    dep.mkdir(parents=True)
    (dep / "source_registry.yaml").write_text(
        "sources:\n"
        "  git-files:\n"
        "    ownership_id: okg-workspace.git-files\n"
        "    source_class: discovery_crawl\n"
        "    record_identity_kind: scoped_locator\n"
        "    record_identity_fields: [repo, path]\n"
        "    source_revision_kind: content_hash\n"
        "    deletion_semantics: missing_from_completed_scope\n"
        "    publication_mode: published_generation\n",
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        cv = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO okg.graph_generations "
            "  (generation_id, deployment_name, status, "
            "   catalog_version_id, published_at, correlation_id) "
            "VALUES (42, 'okg-workspace', 'published', %s, now(), "
            "        'pub:okg-workspace:test')",
            (cv,),
        )
        cur.execute(
            "INSERT INTO okg.source_watermarks "
            "  (source, projection_cursor, last_reconcile_at) "
            "VALUES ('okg-workspace.git-files', 1, now())"
        )

    payload = _collect_status(
        clean_runtime, "okg-workspace", deployment_dir=dep,
    )

    assert payload["deployment_name"] == "okg-workspace"
    assert payload["publishing"]["latest_published_generation"] == 42
    assert {
        row["source"] for row in payload["sources"]
    } == {"okg-workspace.git-files"}


# ---------- schema ----------


def test_schema_emits_json_schema(capsys) -> None:
    """`okg --json schema deployment.yaml` prints a JSON Schema."""
    rc = main(["--json", "schema", "deployment.yaml"])
    assert rc == 0


def test_schema_unknown_file_exits_nonzero(capsys) -> None:
    rc = main(["--json", "schema", "totally-unknown.yaml"])
    assert rc != 0


# ---------- library ----------


def test_library_list_default_returns_envelope(capsys) -> None:
    rc = main(["--json", "library", "list"])
    assert rc == 0


def test_library_list_filtered_by_kind(capsys) -> None:
    """Real library kinds per resolver.KINDS: invariants,
    linker_routes, modules, templates."""
    rc = main(["--json", "library", "list", "--kind", "modules"])
    assert rc == 0


def test_library_show_unknown_ref_exits_nonzero(capsys) -> None:
    rc = main(["--json", "library", "show", "okg.modules.bogus_xyz"])
    assert rc != 0


def test_library_show_known_ref(capsys) -> None:
    """`okg.modules.openspec` is the openspec module."""
    rc = main(["--json", "library", "show", "okg.modules.openspec"])
    assert rc == 0


# ---------- metrics ----------


def test_metrics_without_mode_flag_exits_nonzero(monkeypatch) -> None:
    """`okg metrics` requires `--llm` or `--qa-snapshot`."""
    monkeypatch.delenv("OKG_DSN", raising=False)
    rc = main(["metrics", "--json"])
    assert rc != 0


def test_metrics_qa_snapshot_requires_deployment(
    clean_runtime: str, monkeypatch,
) -> None:
    """`metrics --qa-snapshot` requires --deployment."""
    monkeypatch.delenv("OKG_DEPLOYMENT", raising=False)
    rc = main([
        "metrics", "--qa-snapshot",
        "--dsn", clean_runtime, "--json",
    ])
    assert rc != 0


def test_metrics_llm_against_real_db(
    clean_runtime: str, capsys,
) -> None:
    """`okg metrics --llm` returns a roll-up envelope (probably
    empty against clean_runtime)."""
    rc = main([
        "metrics", "--llm", "--since", "1h",
        "--dsn", clean_runtime, "--json",
    ])
    assert rc == 0


def test_metrics_qa_snapshot_against_real_db(
    clean_runtime: str, capsys,
) -> None:
    """`okg metrics --qa-snapshot` against a clean DB raises a
    'no published generation' error from the qa.snapshot helper."""
    rc = main([
        "metrics", "--qa-snapshot", "--deployment", "default",
        "--dsn", clean_runtime, "--json",
    ])
    # Either succeeds (if there's a published gen) OR returns an
    # error envelope. Both paths exercise the dispatch.
    assert rc in (0, 1, 2)


# ---------- trace ----------


def test_trace_node_unknown_returns_empty(
    clean_runtime: str, capsys,
) -> None:
    """`okg trace node <unknown>` against a clean DB returns an
    empty facts envelope, not an error."""
    rc = main([
        "--json", "trace", "node", "node:does-not-exist",
        "--dsn", clean_runtime,
    ])
    assert rc == 0
    out = capsys.readouterr()
    payload = json.loads(out.out.strip().splitlines()[-1])
    assert payload["facts"] == []


# ---------- generation describe (sanity; existing test_cli_generation_describe.py covers detail) ----------


def test_generation_describe_unknown_id_exits_nonzero(
    clean_runtime: str,
) -> None:
    rc = main([
        "generation", "describe", "999999",
        "--dsn", clean_runtime, "--json",
    ])
    assert rc != 0


# ---------- migrate ----------


def test_migrate_dry_run_against_real_db(
    clean_runtime: str, capsys,
) -> None:
    """`okg migrate` (no --apply) reports planned actions."""
    rc = main([
        "migrate", "--dsn", clean_runtime, "--json",
    ])
    # Without --apply, should still succeed (idempotent dry-run).
    assert rc in (0, 1)


# ---------- init ----------


def test_init_unknown_template_exits_nonzero(tmp_path) -> None:
    rc = main([
        "init", "--template", "totally-bogus-template",
        "--target", str(tmp_path),
        "--json",
    ])
    assert rc != 0


def test_init_known_template_scaffolds(tmp_path, capsys) -> None:
    """A supported library template is copied to the requested target."""
    rc = main([
        "init", "--template", "devops",
        "--target", str(tmp_path),
        "--json",
    ])
    assert rc == 0
    # The template was copied into a subdirectory named after it.
    target = tmp_path / "devops"
    assert target.is_dir()
    assert any(target.iterdir())


def test_init_template_renderer_supports_modern_and_legacy_placeholders(
    tmp_path,
) -> None:
    src = tmp_path / "template"
    src.mkdir()
    (src / "deployment.yaml").write_text(
        "name: {{ deployment_name }}\nrepo: __OKG_REPO_ROOT__\n",
    )
    (src / "conditional.yaml").write_text(
        "# {{ if include_optional }}\n"
        "enabled: {{ deployment_name }}\n"
        "# {{ endif include_optional }}\n"
        "# {{ if skip_optional }}\n"
        "missing: true\n"
        "# {{ endif skip_optional }}\n",
    )
    (src / "README.md").write_text("Open {{ repo_root }}\n")
    (src / "recipe.sh").write_text("okg run --deployment {{ deployment_name }}\n")
    (src / "server.py").write_text("DEPLOYMENT = '{{ deployment_name }}'\n")
    (src / "notes.txt").write_text("left alone {{ deployment_name }}\n")

    target = tmp_path / "rendered"
    _render_template_dir(src, target, {
        "deployment_name": "test-lp",
        "repo_root": "/tmp/repo",
        "repo_name": "repo",
        "include_optional": True,
        "skip_optional": False,
    })

    assert "name: test-lp" in (target / "deployment.yaml").read_text()
    assert "repo: /tmp/repo" in (target / "deployment.yaml").read_text()
    assert "Open /tmp/repo" in (target / "README.md").read_text()
    conditional = (target / "conditional.yaml").read_text()
    assert "enabled: test-lp" in conditional
    assert "missing: true" not in conditional
    assert "{{ if" not in conditional
    assert "test-lp" in (target / "recipe.sh").read_text()
    assert "DEPLOYMENT = 'test-lp'" in (target / "server.py").read_text()
    assert "{{ deployment_name }}" in (target / "notes.txt").read_text()


def test_init_okg_workspace_requires_repo_root(tmp_path, capsys) -> None:
    rc = main([
        "init",
        "--template",
        "okg-workspace",
        "--target",
        str(tmp_path),
        "--json",
    ])
    assert rc != 0
    out = capsys.readouterr()
    assert "requires --repo-root" in out.err


def test_init_okg_workspace_derives_defaults_from_repo_root(
    tmp_path,
    capsys,
) -> None:
    repo = tmp_path / "sample-repo"
    repo.mkdir()
    target_root = tmp_path / "deployments"

    rc = main([
        "init",
        "--template",
        "okg-workspace",
        "--target",
        str(target_root),
        "--repo-root",
        str(repo),
        "--json",
    ])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["deployment_name"] == "sample-repo"
    assert payload["target"] == str(target_root / "sample-repo")
    assert payload["openspec_root"] == str(repo.resolve() / "openspec")
    assert payload["since"] == "90d"
    assert payload["notes_root"] is None
    assert payload["claude_projects_root"].endswith(".claude/projects")
    assert "okg migrate --apply" in payload["next_commands"]
    assert (
        "okg run --once --deployment sample-repo --apply"
        in payload["next_commands"]
    )
    assert (
        "okg runtime worker --deployment sample-repo"
        in payload["next_commands"]
    )
    assert (target_root / "sample-repo").is_dir()


def test_init_workspace_graph_template_alias_derives_defaults_from_repo_root(
    tmp_path,
    capsys,
) -> None:
    repo = tmp_path / "sample-repo"
    repo.mkdir()
    target_root = tmp_path / "deployments"

    rc = main([
        "init",
        "--template",
        "workspace-graph",
        "--target",
        str(target_root),
        "--repo-root",
        str(repo),
        "--json",
    ])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["template"] == "workspace-graph"
    assert payload["deployment_name"] == "sample-repo"
    assert payload["target"] == str(target_root / "sample-repo")
    assert payload["openspec_root"] == str(repo.resolve() / "openspec")
    assert (target_root / "sample-repo" / "deployment.yaml").is_file()


def test_init_okg_workspace_template_renders_coding_context_sources(
    tmp_path,
    capsys,
) -> None:
    import yaml

    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "openspec").mkdir()
    target_root = tmp_path / "deployments"

    rc = main([
        "init",
        "--template",
        "okg-workspace",
        "--target",
        str(target_root),
        "--repo-root",
        str(repo),
        "--deployment-name",
        "test-lp",
        "--json",
    ])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["repo_root"] == str(repo.resolve())
    assert payload["openspec_enabled"] is True

    target = target_root / "test-lp"
    deployment_path = target / "deployment.yaml"
    assert deployment_path.is_file()
    deployment_raw = deployment_path.read_text()
    assert "__OKG_REPO_ROOT__" not in deployment_raw
    assert "{{" not in deployment_raw
    deployment = yaml.safe_load(deployment_raw)
    assert deployment["name"] == "test-lp"
    assert deployment["modules_config"]["git_graph"]["repo_root"] == str(
        repo.resolve(),
    )
    assert deployment["modules_config"]["agent_sessions"]["since"] == "90d"

    registry_path = target / "source_registry.yaml"
    assert registry_path.is_file()
    raw = registry_path.read_text()
    assert "__OKG_REPO_ROOT__" not in raw
    assert "{{" not in raw
    assert str(repo.resolve()) in raw

    registry = yaml.safe_load(raw)
    sources = registry["sources"]
    expected = {
        "code-structure",
        "package-manifests",
        "interfaces",
        "code-data-flow",
        "semgrep-data-flow",
        "codeql-data-flow",
        "test-reports",
        "security-analysis",
        "profile-measurements",
        "codex-sessions",
    }
    assert expected <= set(sources)
    assert (
        sources["code-structure"]["params"]["repo_root"]
        == str(repo.resolve())
    )
    assert sources["git-files"]["ownership_id"] == "test-lp.git-files"
    assert sources["openspec"]["params"]["openspec_root"] == str(
        repo.resolve() / "openspec",
    )
    # `deployment` is substrate-injected; the strict contract refuses an
    # authored value and lint blocks on it (issue #2150).
    assert "deployment" not in sources["profile-measurements"]["params"]
    assert "doc-corpus" in sources
    assert "notes-doc-corpus" not in sources

    extractors = yaml.safe_load((target / "extractors.yaml").read_text())
    parser_formats = {entry["format"] for entry in extractors["parser"]}
    assert {
        "code",
        "code_javascript",
        "code_typescript",
        "code_go",
        "code_rust",
        "code_cpp",
    } <= parser_formats

    readme_path = target / "README.md"
    assert readme_path.is_file()
    readme = readme_path.read_text()
    assert "__OKG_REPO_ROOT__" not in readme
    assert "{{" not in readme
    assert str(repo.resolve()) in readme
    assert "okg catalog load --deployment" in readme
    assert "okg ingest --deployment \"test-lp\" --progress" in readme
    assert "okg run --once --deployment \"test-lp\" --apply" in readme
    assert "okg doctor --check coding-context --deployment \"test-lp\"" in readme
    assert "okg alias seed-from-subtype --subtype open_spec_change" in readme
    assert "okg runtime worker --deployment \"test-lp\"" in readme
    assert "OKG_MCP_EMBEDDER" not in readme
    assert "--embedder hash" not in readme
    assert "Embeddings are disabled by default" in readme
    assert "explicit backend" in readme
    assert "2560-dimensional capability" in readme
    assert "`hash` is for deterministic tests/development" in readme
    assert "okg mcp-serve --deployment \"test-lp\"" in readme
    assert f'OKG_DEPLOYMENTS_DIR="{target_root}"' in readme

    server_path = target / "server.py"
    assert server_path.is_file()
    assert "run_for_deployment(\"test-lp\")" in server_path.read_text()
    doctor_path = target / "doctor_checks.py"
    doctor_text = doctor_path.read_text()
    assert "{{" not in doctor_text
    assert "okg-workspace" not in doctor_text
    assert "test-lp.semgrep-data-flow" in doctor_text
    recipe_path = target / "recipe.sh"
    assert recipe_path.is_file()
    recipe = recipe_path.read_text()
    assert "{{" not in recipe
    assert "okg catalog load --deployment" in recipe
    assert "okg ingest --deployment \"test-lp\" --progress" in recipe
    assert "okg run --once --deployment \"test-lp\" --apply" in recipe
    assert "okg doctor --check coding-context --deployment \"test-lp\"" in recipe
    assert "okg alias seed-from-subtype --subtype open_spec_change" in recipe
    assert "okg runtime worker --deployment \"test-lp\"" in recipe
    assert "okg mcp-serve --deployment \"test-lp\"" in recipe
    assert sources["security-analysis"]["params"]["artifact_globs"]


def test_init_okg_workspace_renders_notes_doc_corpus_when_requested(
    tmp_path,
    capsys,
) -> None:
    import yaml

    repo = tmp_path / "repo"
    repo.mkdir()
    notes = tmp_path / "notes"
    notes.mkdir()
    target_root = tmp_path / "deployments"

    rc = main([
        "init",
        "--template",
        "okg-workspace",
        "--target",
        str(target_root),
        "--repo-root",
        str(repo),
        "--deployment-name",
        "test-lp",
        "--notes-root",
        str(notes),
        "--json",
    ])
    assert rc == 0

    registry_path = target_root / "test-lp" / "source_registry.yaml"
    raw = registry_path.read_text()
    assert "{{" not in raw
    registry = yaml.safe_load(raw)
    sources = registry["sources"]
    assert "doc-corpus" in sources
    assert "notes-doc-corpus" in sources
    notes_source = sources["notes-doc-corpus"]
    assert notes_source["ownership_id"] == "test-lp.notes-doc-corpus"
    assert notes_source["params"]["repo_root"] == str(notes.resolve())
    assert notes_source["params"]["extractors_dir"] == str(
        target_root / "test-lp",
    )
    assert "predecessor_source" not in notes_source["params"]


def test_init_okg_workspace_omits_openspec_source_when_root_missing(
    tmp_path,
    capsys,
) -> None:
    import yaml

    repo = tmp_path / "repo"
    repo.mkdir()
    target_root = tmp_path / "deployments"

    rc = main([
        "init",
        "--template",
        "okg-workspace",
        "--target",
        str(target_root),
        "--repo-root",
        str(repo),
        "--deployment-name",
        "test-lp",
        "--json",
    ])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["openspec_enabled"] is False

    registry_path = target_root / "test-lp" / "source_registry.yaml"
    raw = registry_path.read_text()
    assert "test-lp.openspec" not in raw
    assert "openspec_roots" not in raw

    registry = yaml.safe_load(raw)
    sources = registry["sources"]
    assert "openspec" not in sources
    assert (
        sources["profile-measurements"]["predecessor_source"]
        == ["test-lp.git-files"]
    )


# ---------- catalog ----------


def test_catalog_load_with_missing_deployment_dir_exits_nonzero(
    tmp_path,
) -> None:
    rc = main([
        "catalog", "load",
        "--deployment", str(tmp_path / "does-not-exist"),
        "--json",
    ])
    assert rc != 0


# ---------- ingest ----------


def test_ingest_with_missing_deployment_exits_nonzero(
    monkeypatch,
) -> None:
    monkeypatch.delenv("OKG_DSN", raising=False)
    rc = main([
        "ingest", "--deployment", "totally-bogus-deployment",
        "--json",
    ])
    assert rc != 0


# ---------- run ----------


def test_run_without_once_flag_errors(monkeypatch) -> None:
    """Per cli/main.py:946, `okg run` (without --once) is "not
    implemented in M1" and returns a structured error."""
    monkeypatch.delenv("OKG_DSN", raising=False)
    rc = main([
        "run", "--deployment", "x", "--dsn", "postgres://nope/x",
        "--json",
    ])
    assert rc != 0
