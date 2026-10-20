"""A swallowed enricher crash is named by every publish surface.

The publisher logs and swallows an enricher that raises: the publish still
commits, the enricher's watermark stays put, and the next cycle retries it.
``okg doctor --check enricher-liveness`` reported that, but ``okg run
--once`` printed a bare "Published graph version N" and plain ``okg
doctor`` read clean. These tests force an enricher to raise inside a real
publish cycle against PostgreSQL and read what each command prints:

* ``okg run --once`` in-process: the headline names the enricher, its error
  class and failure streak; ``--json`` carries ``run_summary.
  enricher_failures``; the exit code stays 0 because the publish committed;
* ``okg run --once`` on the runtime-queue path: the same, in its payload;
* ``okg ingest``: the report gains ``enricher_failures`` and a text line;
* plain ``okg doctor --deployment``: an ``enricher_failed`` warning and an
  ``enricher_liveness`` block.
"""
from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import psycopg
import pytest

import okg.substrate.cli.main as cli
from okg.substrate.cli.main import main
from okg.substrate.cli.run_summary import build_run_summary, render_run_summary
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import (
    bind_test_catalog_writer,
    seed_catalog,
)

DEPLOYMENT = "test-pub"
ENRICHER = "always_failing_enricher"


class _FailingEnricher:
    name = ENRICHER
    publisher_skip_when_clean = False
    dormant_ok = True
    reads_subtypes = ("concept",)
    reads_edge_types = ()
    emits_edge_types = ()
    requires_narrowings = ()
    write_envelope = {
        "nodes": [{"subtype": "concept"}],
        "nondeterministic": False,
    }
    declares_node_provenance = True

    def enrich(self, conn, *, generation_id, incremental=None):
        del generation_id, incremental
        # A real database error, so the publisher's savepoint recovery runs
        # exactly as it does for a production enricher crash.
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM okg.enricher_visibility_missing")


def _publisher(dsn: str) -> Publisher:
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["concept"], narrowings=[])
    return Publisher(
        dsn,
        DEPLOYMENT,
        enrichers=[_FailingEnricher()],
        catalog_identity=bind_test_catalog_writer(dsn, DEPLOYMENT),
    )


def _run_args(**overrides: Any) -> SimpleNamespace:
    values = {
        "once": True,
        "apply": True,
        "inline": True,
        "deployment": DEPLOYMENT,
        "dsn": None,
        "branch": None,
        "force_enrichers": False,
        "reconcile_count_stamp": False,
        "include_llm_invariants": False,
        "llm_invariant": None,
        "verbose": False,
        "owner_id": None,
        "owner_meta": None,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


@pytest.mark.postgres
def test_run_once_names_the_failed_enricher_in_text_and_json(
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import okg.substrate.publisher.factory as factory

    publisher = _publisher(clean_runtime)
    monkeypatch.setattr(
        cli,
        "_resolve_run_context",
        lambda _args: (clean_runtime, DEPLOYMENT, None, False),
    )
    monkeypatch.setattr(
        factory, "build_deployment_publisher", lambda **_kw: publisher,
    )

    rc = cli._cmd_run(_run_args(), as_json=False)
    text = capsys.readouterr().out
    headline = text.splitlines()[0]

    # The publish committed, so the exit code stays 0; the headline no
    # longer reads as a bare publish.
    assert rc == 0, text
    assert headline.startswith("Published graph version "), text
    assert (
        f"; 1 enricher failed: {ENRICHER} (UndefinedTable, first failure)"
        in headline
    ), text
    assert (
        "okg doctor --check enricher-liveness --deployment test-pub" in text
    )

    # Nothing new was staged, so the second cycle publishes no new version,
    # but it still retries the enricher, which fails again.
    rc = cli._cmd_run(_run_args(), as_json=True)
    payload = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert payload["status"] == "published"
    assert payload["run_summary"]["outcome"] == "no_change"
    assert payload["run_summary"]["exit_code"] == 0
    assert payload["run_summary"]["enricher_failures"] == [{
        "name": ENRICHER,
        "error_class": "UndefinedTable",
        "consecutive_failures": 2,
    }]

    rc = cli._cmd_run(_run_args(), as_json=False)
    lines = capsys.readouterr().out.splitlines()
    assert rc == 0
    assert lines[0].startswith("No change:"), lines
    assert lines[1] == (
        f"1 enricher failed: {ENRICHER} (UndefinedTable, 3 cycles in a row)."
    ), lines


@pytest.mark.postgres
def test_runtime_queue_run_once_names_the_failed_enricher(
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import okg.substrate.runtime.admin as runtime_admin

    summary = _publisher(clean_runtime).cycle()
    monkeypatch.setattr(
        cli,
        "_resolve_run_context",
        lambda _args: (clean_runtime, DEPLOYMENT, None, True),
    )
    monkeypatch.setattr(
        runtime_admin,
        "client_enqueue_publisher_cycle",
        lambda **_kw: SimpleNamespace(
            workflow_id="wf-enricher-visibility",
            status="SUCCESS",
            summary=summary,
            error=None,
            elapsed_seconds=1.0,
        ),
    )

    rc = cli._cmd_run(_run_args(inline=False), as_json=False)
    text = capsys.readouterr().out
    assert rc == 0, text
    assert "outcome=published" in text.splitlines()[0]
    assert (
        f"; 1 enricher failed: {ENRICHER} (UndefinedTable, first failure)"
        in text.splitlines()[0]
    ), text

    rc = cli._cmd_run(_run_args(inline=False), as_json=True)
    payload = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert payload["outcome"] == "published"
    assert payload["enricher_failures"] == [{
        "name": ENRICHER,
        "error_class": "UndefinedTable",
        "consecutive_failures": 1,
    }]


@pytest.mark.postgres
def test_ingest_report_names_the_failed_enricher(clean_runtime: str) -> None:
    published = _publisher(clean_runtime).cycle()
    report: dict[str, Any] = {"publishes": [published]}

    rows, lines = cli._ingest_enricher_failure_lines(
        report, DEPLOYMENT, command_exit_code=0,
    )

    assert rows == [{
        "name": ENRICHER,
        "error_class": "UndefinedTable",
        "consecutive_failures": 1,
        "generation_id": published["generation_id"],
    }]
    assert lines[0] == (
        f"published graph version {published['generation_id']}; "
        f"1 enricher failed: {ENRICHER} (UndefinedTable, first failure)"
    )
    assert lines[1] == (
        "  the new graph version was published without that enricher's "
        "output; the next cycle retries it. The exit code stays 0. Inspect: "
        "okg doctor --check enricher-liveness --deployment test-pub"
    )


@pytest.mark.postgres
def test_plain_doctor_warns_about_the_failed_enricher(
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _publisher(clean_runtime).cycle()

    main([
        "doctor",
        "--deployment", DEPLOYMENT,
        "--dsn", clean_runtime,
        "--json",
    ])
    payload = json.loads(capsys.readouterr().out)

    liveness = payload["enricher_liveness"]
    assert liveness["failed_count"] == 1
    assert liveness["ok"] is False
    assert liveness["check_command"] == (
        "okg doctor --check enricher-liveness --deployment test-pub"
    )
    failed = [
        w for w in payload.get("warnings") or []
        if w.get("code") == "enricher_failed"
    ]
    assert len(failed) == 1, payload.get("warnings")
    assert failed[0]["enricher_name"] == ENRICHER
    assert failed[0]["error_class"] == "UndefinedTable"
    assert failed[0]["consecutive_failures"] == 1
    # Silent enrichers are counted in the block, never one warning each: a
    # fresh deployment's bundled linkers are all silent until inputs exist.
    assert liveness["silent_enrichers"] == []
    assert not any(
        w.get("code") == "enricher_silent"
        for w in payload.get("warnings") or []
    )


@pytest.mark.postgres
def test_plain_doctor_probe_failure_is_a_warning_not_a_clean_result(
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.cli import doctor as doctor_module

    def fail_query(*_args, **_kwargs):
        raise RuntimeError("simulated progress query failure")

    monkeypatch.setattr(doctor_module, "_enricher_liveness_findings", fail_query)
    main([
        "doctor",
        "--deployment", DEPLOYMENT,
        "--dsn", clean_runtime,
        "--json",
    ])
    payload = json.loads(capsys.readouterr().out)

    assert payload["enricher_liveness"]["ok"] is False
    assert payload["enricher_liveness"]["code"] == (
        "enricher_liveness_probe_failed"
    )
    assert any(
        w.get("code") == "enricher_liveness_probe_failed"
        and "simulated progress query failure" in w.get("message", "")
        for w in payload.get("warnings") or []
    )


def test_partial_and_clean_runs_are_not_enricher_failures() -> None:
    from okg.substrate.cli.run_summary import enricher_failures

    out = {
        "status": "published",
        "generation_id": "gen-1",
        "enricher_runs": [
            {"name": "_enricher_setup", "status": "ok"},
            {"name": "summarize", "status": "partial",
             "reason": "failed:incomplete_inputs",
             "consecutive_failure_count": 1},
            {"name": "tagger", "status": "ok",
             "consecutive_failure_count": 0},
        ],
    }
    assert enricher_failures(out) == []
    headline = render_run_summary(build_run_summary(out, deployment="demo"))[0]
    assert headline == "Published graph version gen-1"


def test_failed_run_without_exception_class_reads_it_from_the_reason() -> None:
    from okg.substrate.cli.run_summary import enricher_failures

    out = {"enricher_runs": [{
        "name": "tagger", "status": "failed", "reason": "failed:Timeout",
        "consecutive_failure_count": 3,
    }]}
    assert enricher_failures(out) == [{
        "name": "tagger", "error_class": "Timeout", "consecutive_failures": 3,
    }]
    summary = build_run_summary({**out, "status": "published",
                                 "generation_id": "g"}, deployment="demo")
    assert render_run_summary(summary)[0] == (
        "Published graph version g; 1 enricher failed: "
        "tagger (Timeout, 3 cycles in a row)"
    )


# ---------------------------------------------------------------------------
# Round 2 (review of PR 2978)
# ---------------------------------------------------------------------------


class _OtherEnricher:
    """A registered enricher that is not the failing one (stale-row test)."""

    name = "other_registered_enricher"


class _IncompleteEnricher:
    name = "incomplete_enricher"
    publisher_skip_when_clean = False
    dormant_ok = True
    reads_subtypes = ("concept",)
    reads_edge_types = ()
    emits_edge_types = ()
    requires_narrowings = ()
    write_envelope = {
        "nodes": [{"subtype": "concept"}],
        "nondeterministic": False,
    }
    declares_node_provenance = True

    def enrich(self, conn, *, generation_id, incremental=None):
        del conn, generation_id, incremental
        from okg.substrate.enrichers.base import EnrichResult

        # Returned, but left inputs unprocessed: the publisher records
        # `failed:incomplete_inputs` and run status `partial`.
        return EnrichResult(
            self.name, n_edges_emitted=0, incomplete=True, n_input_failures=1,
        )


_THIS_MODULE = "tests.substrate.test_enricher_failure_visibility"


def _write_deployment(tmp_path, *, enricher_classes: list[str]):
    """A minimal deployment directory named after the publisher's deployment."""
    root = tmp_path / "deployments"
    dep = root / DEPLOYMENT
    (dep / "schemas").mkdir(parents=True)
    (dep / "source_registry.yaml").write_text(
        "sources:\n"
        "  crawl:\n"
        "    module: no_such_module\n"
        "    class: NoSuchSource\n"
        "    ownership_id: test-pub.crawl\n"
        "    source_class: discovery_crawl\n"
        "    record_identity_kind: scoped_locator\n"
        "    record_identity_fields: [path]\n"
        "    source_revision_kind: content_hash\n"
        "    deletion_semantics: missing_from_completed_scope\n"
        "    publication_mode: published_generation\n"
    )
    enrichers = "".join(f"- class: {cls}\n" for cls in enricher_classes)
    (dep / "deployment.yaml").write_text(
        f"name: {DEPLOYMENT}\n"
        "description: enricher failure visibility fixture\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "modules: []\n"
        + (f"enrichers:\n{enrichers}" if enricher_classes else "")
    )
    return root


@pytest.mark.postgres
def test_runtime_queue_ingest_names_the_failed_enricher(
    tmp_path,
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import okg.substrate.runtime.admin as runtime_admin

    published = _publisher(clean_runtime).cycle()
    monkeypatch.setenv(
        "OKG_DEPLOYMENTS_DIR", str(_write_deployment(tmp_path, enricher_classes=[])),
    )
    monkeypatch.setattr(cli, "_deployment_runtime_enabled", lambda _d: True)
    monkeypatch.setattr(
        runtime_admin,
        "client_enqueue_source_sync",
        lambda **_kw: SimpleNamespace(
            workflow_id="wf-ingest", status="SUCCESS", any_failed=False,
            elapsed_seconds=1.0, report={"publishes": [published]},
            error=None, error_code=None,
        ),
    )
    argv = ["ingest", "--deployment", DEPLOYMENT, "--dsn", clean_runtime]

    rc = main(argv)
    text = capsys.readouterr().out
    assert rc == 0, text
    assert (
        f"published graph version {published['generation_id']}; 1 enricher "
        f"failed: {ENRICHER} (UndefinedTable, first failure)"
    ) in text

    rc = main([*argv, "--json"])
    payload = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert payload["enricher_failures"] == [{
        "name": ENRICHER,
        "error_class": "UndefinedTable",
        "consecutive_failures": 1,
        "generation_id": published["generation_id"],
    }]


@pytest.mark.postgres
def test_inline_ingest_command_names_the_failed_enricher(
    tmp_path,
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import okg.substrate.ingest.service as ingest_service

    published = _publisher(clean_runtime).cycle()
    monkeypatch.setenv(
        "OKG_DEPLOYMENTS_DIR", str(_write_deployment(tmp_path, enricher_classes=[])),
    )
    monkeypatch.setattr(
        ingest_service,
        "run_ingest",
        lambda _options: SimpleNamespace(
            report={"publishes": [published]}, any_failed=False,
        ),
    )
    argv = [
        "ingest", "--deployment", DEPLOYMENT, "--dsn", clean_runtime,
        "--inline",
    ]

    rc = main(argv)
    text = capsys.readouterr().out
    assert rc == 0, text
    assert f"1 enricher failed: {ENRICHER} (UndefinedTable" in text

    rc = main([*argv, "--json"])
    payload = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert [row["name"] for row in payload["enricher_failures"]] == [ENRICHER]


def test_blocked_publish_hint_does_not_claim_a_commit_or_exit_zero() -> None:
    out = {
        "status": "blocked", "generation_id": "g-blocked",
        "blocked_reason": "gate x",
        "enricher_runs": [{
            "name": "tagger", "status": "failed",
            "exception_class": "LookupError", "consecutive_failure_count": 2,
        }],
    }
    summary = build_run_summary(out, deployment="demo")
    text = "\n".join(render_run_summary(summary))

    assert summary["exit_code"] == 3
    assert "1 enricher failed: tagger (LookupError, 2 cycles in a row)." in text
    assert "published without" not in text
    assert "exit code stays 0" not in text
    assert "this cycle published nothing (outcome blocked)" in text


def test_no_change_hint_says_no_new_version_was_published() -> None:
    out = {
        "status": "published", "generation_id": "g-same", "noop": True,
        "reused_generation": True,
        "enricher_runs": [{
            "name": "tagger", "status": "failed",
            "exception_class": "LookupError", "consecutive_failure_count": 4,
        }],
    }
    summary = build_run_summary(out, deployment="demo")
    assert summary["outcome"] == "no_change"
    text = "\n".join(render_run_summary(summary))
    assert "no new graph version was published" in text
    assert "published without" not in text


@pytest.mark.postgres
def test_runtime_queue_run_blocked_hint_is_outcome_aware(
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import okg.substrate.runtime.admin as runtime_admin

    summary = {
        "status": "blocked", "generation_id": "g-blocked",
        "blocked_reason": {"reason": "gate x"}, "phase_b_blocked": True,
        "enricher_runs": [{
            "name": "tagger", "status": "failed",
            "exception_class": "LookupError", "consecutive_failure_count": 1,
        }],
    }
    monkeypatch.setattr(
        cli, "_resolve_run_context",
        lambda _args: (clean_runtime, DEPLOYMENT, None, True),
    )
    monkeypatch.setattr(
        runtime_admin, "client_enqueue_publisher_cycle",
        lambda **_kw: SimpleNamespace(
            workflow_id="wf-blocked", status="SUCCESS", summary=summary,
            error=None, elapsed_seconds=1.0,
        ),
    )
    rc = cli._cmd_run(_run_args(inline=False), as_json=False)
    text = capsys.readouterr().out
    assert rc == 3, text
    assert "1 enricher failed: tagger (LookupError, first failure)" in text
    assert "published without" not in text
    assert "this cycle published nothing (outcome blocked)" in text


@pytest.mark.postgres
def test_plain_doctor_groups_quarantines_by_enricher(
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.enricher_input_quarantines
              (deployment_name, branch_name, enricher_name, item_ref,
               input_signature, operator_version, model_id, prompt_hash,
               input_hash, status, attempt_count, attempt_limit,
               last_failure_class, last_error, first_failed_at,
               last_failed_at, quarantined_at)
            SELECT 'test-pub', 'default',
                   CASE WHEN i <= 300 THEN 'claim_extractor'
                        ELSE 'summarizer' END,
                   'paragraph:' || i, 'sig-' || i, 'v1', 'model', 'prompt',
                   'input-' || i, 'active', 2, 2,
                   'incomplete_extraction_response', 'malformed JSON',
                   now() - interval '1 minute', now(), now()
              FROM generate_series(1, 302) AS i
            """
        )

    main(["doctor", "--deployment", DEPLOYMENT, "--dsn", clean_runtime, "--json"])
    payload = json.loads(capsys.readouterr().out)

    quarantined = [
        w for w in payload.get("warnings") or []
        if w.get("code") == "enricher_input_quarantined"
    ]
    assert [
        (w["enricher_name"], w["quarantined_inputs"]) for w in quarantined
    ] == [("claim_extractor", 300), ("summarizer", 2)]
    assert payload["enricher_liveness"]["quarantine_count"] == 302


@pytest.mark.postgres
def test_plain_doctor_probe_runs_under_a_statement_timeout(
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.cli import doctor as doctor_module

    seen: list[str] = []
    real = doctor_module._enricher_liveness_findings

    def spy(conn, deployment):
        with conn.cursor() as cur:
            cur.execute("SHOW statement_timeout")
            seen.append(str(cur.fetchone()[0]))
        return real(conn, deployment)

    monkeypatch.setattr(doctor_module, "_enricher_liveness_findings", spy)
    main(["doctor", "--deployment", DEPLOYMENT, "--dsn", clean_runtime, "--json"])
    capsys.readouterr()
    assert seen == ["5s"]


@pytest.mark.postgres
def test_plain_doctor_lists_a_removed_enricher_as_stale_without_warning(
    tmp_path,
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _publisher(clean_runtime).cycle()
    argv = ["doctor", "--deployment", DEPLOYMENT, "--dsn", clean_runtime, "--json"]

    # Still registered: warned.
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(_write_deployment(
        tmp_path / "registered",
        enricher_classes=[f"{_THIS_MODULE}._FailingEnricher"],
    )))
    main(argv)
    payload = json.loads(capsys.readouterr().out)
    assert payload["enricher_liveness"]["registry"] == "deployment.yaml"
    assert payload["enricher_liveness"]["failed_count"] == 1
    assert payload["enricher_liveness"]["stale_enrichers"] == []

    # Removed from deployment.yaml: listed as stale, never warned.
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(_write_deployment(
        tmp_path / "removed",
        enricher_classes=[f"{_THIS_MODULE}._OtherEnricher"],
    )))
    main(argv)
    payload = json.loads(capsys.readouterr().out)
    liveness = payload["enricher_liveness"]
    assert liveness["failed_count"] == 0
    assert liveness["stale_enrichers"] == [ENRICHER]
    assert "no longer in deployment.yaml" in liveness["stale_note"]
    assert not any(
        w.get("enricher_name") == ENRICHER
        for w in payload.get("warnings") or []
    )


@pytest.mark.postgres
def test_partial_run_is_incomplete_in_both_run_and_doctor(
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from okg.substrate.cli.run_summary import enricher_failures

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["concept"], narrowings=[])
    out = Publisher(
        clean_runtime, DEPLOYMENT, enrichers=[_IncompleteEnricher()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, DEPLOYMENT),
    ).cycle()
    run = next(r for r in out["enricher_runs"] if r["name"] == "incomplete_enricher")
    assert run["status"] == "partial"
    # Run: an incomplete run is not an enricher failure.
    assert enricher_failures(out) == []

    main(["doctor", "--deployment", DEPLOYMENT, "--dsn", clean_runtime, "--json"])
    payload = json.loads(capsys.readouterr().out)
    liveness = payload["enricher_liveness"]
    # Doctor agrees: incomplete, not failed, and warned under its own code.
    assert liveness["failed_count"] == 0
    assert liveness["incomplete_count"] == 1
    # An incomplete enricher is still a finding: the block is not ok.
    assert liveness["ok"] is False
    codes = [w.get("code") for w in payload.get("warnings") or []]
    assert "enricher_failed" not in codes
    assert codes.count("enricher_incomplete") == 1


def test_plain_doctor_skips_the_enricher_probe_when_the_database_is_down(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dsn = "postgresql://u:p@127.0.0.1:1/x"
    seen: list[str] = []

    def refuse(target, *_args, **_kwargs):
        seen.append(target)
        raise RuntimeError("connection refused")

    monkeypatch.setattr("okg.substrate.db.connection.connect", refuse)
    main(["doctor", "--deployment", "no-such-deployment", "--dsn", dsn, "--json"])
    payload = json.loads(capsys.readouterr().out)

    # One connect attempt; the unreachable database is already an error and
    # the enricher block says it was skipped rather than reading clean.
    assert seen == [dsn]
    assert payload["enricher_liveness"] == {
        "ok": False,
        "skipped": True,
        "code": "enricher_liveness_skipped_database_unreachable",
    }
    assert not any(
        w.get("code") == "enricher_liveness_probe_failed"
        for w in payload.get("warnings") or []
    )


# ---------------------------------------------------------------------------
# Round 3: the hint agrees with the command's exit code; every queued batch
# is read; the registry-load cause is shown; silent and incomplete rows.
# ---------------------------------------------------------------------------

def _failed_publish(generation_id: str) -> dict[str, Any]:
    """A published cycle summary whose enricher raised (no database)."""
    return {
        "status": "published", "generation_id": generation_id,
        "enricher_runs": [{
            "name": ENRICHER, "status": "failed",
            "exception_class": "UndefinedTable",
            "consecutive_failure_count": 1,
        }],
    }


def _clean_publish(generation_id: str) -> dict[str, Any]:
    return {
        "status": "published", "generation_id": generation_id,
        "enricher_runs": [],
    }


def _queued(
    report: dict[str, Any] | None,
    *,
    status: str = "SUCCESS",
    any_failed: bool = False,
    error: str | None = None,
    error_code: str | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        workflow_id=f"wf-{status}", status=status, any_failed=any_failed,
        elapsed_seconds=1.0, report=report, error=error,
        error_code=error_code,
    )


def _queued_ingest(
    tmp_path, monkeypatch: pytest.MonkeyPatch, results: list[SimpleNamespace],
) -> list[str]:
    """Route ``okg ingest`` through the runtime queue, one batch per result."""
    import okg.substrate.ingest.planning as planning
    import okg.substrate.runtime.admin as runtime_admin

    # The fixture registry is real (one source, "crawl"); only the plan and
    # the batch split are stubbed so the queue sees one batch per result.
    monkeypatch.setenv(
        "OKG_DEPLOYMENTS_DIR", str(_write_deployment(tmp_path, enricher_classes=[])),
    )
    monkeypatch.setattr(cli, "_deployment_runtime_enabled", lambda _d: True)
    monkeypatch.setattr(
        planning, "plan_ingest_sources",
        lambda *_a, **_k: SimpleNamespace(ordered=("crawl",)),
    )
    monkeypatch.setattr(
        cli, "_sync_source_batches_by_run_mode",
        lambda *_a, **_k: [
            {
                "sources": ["crawl"], "run_mode": "scope_complete",
                "include_predecessors": True, "include_dependents": False,
                "dependent_fanout": None,
            }
            for _ in results
        ],
    )
    pending = list(results)
    monkeypatch.setattr(
        runtime_admin, "client_enqueue_source_sync",
        lambda **_kw: pending.pop(0),
    )
    return ["ingest", "--deployment", DEPLOYMENT, "--mode", "reconcile"]


def test_ingest_hint_states_the_command_exit_code_not_zero() -> None:
    report = {"publishes": [_failed_publish("g-1")]}

    _rows, ok_lines = cli._ingest_enricher_failure_lines(
        report, DEPLOYMENT, command_exit_code=0,
    )
    _rows, failed_lines = cli._ingest_enricher_failure_lines(
        report, DEPLOYMENT, command_exit_code=1,
    )

    assert "The exit code stays 0" in ok_lines[1]
    assert "exit code stays 0" not in failed_lines[1]
    assert (
        "this command exits 1 because another source or publish failed"
    ) in failed_lines[1]


@pytest.mark.postgres
def test_queued_ingest_with_a_failed_source_does_not_claim_exit_zero(
    tmp_path,
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    argv = _queued_ingest(tmp_path, monkeypatch, [
        _queued({"publishes": [_failed_publish("g-1")]}, any_failed=True),
    ])

    rc = main([*argv, "--dsn", clean_runtime])
    text = capsys.readouterr().out

    assert rc == 1, text
    assert f"1 enricher failed: {ENRICHER}" in text
    assert "exit code stays 0" not in text
    assert "this command exits 1" in text


@pytest.mark.postgres
def test_inline_ingest_with_a_failed_source_does_not_claim_exit_zero(
    tmp_path,
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import okg.substrate.ingest.service as ingest_service

    monkeypatch.setenv(
        "OKG_DEPLOYMENTS_DIR", str(_write_deployment(tmp_path, enricher_classes=[])),
    )
    monkeypatch.setattr(
        ingest_service, "run_ingest",
        lambda _options: SimpleNamespace(
            report={"publishes": [_failed_publish("g-1")]}, any_failed=True,
        ),
    )
    rc = main([
        "ingest", "--deployment", DEPLOYMENT, "--dsn", clean_runtime,
        "--inline",
    ])
    text = capsys.readouterr().out

    assert rc == 1, text
    assert f"1 enricher failed: {ENRICHER}" in text
    assert "exit code stays 0" not in text
    assert "this command exits 1" in text


@pytest.mark.postgres
def test_queued_ingest_names_batch_one_enricher_when_batch_two_fails(
    tmp_path,
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    argv = _queued_ingest(tmp_path, monkeypatch, [
        _queued({"publishes": [_failed_publish("g-1")]}),
        _queued(None, status="ERROR", any_failed=True, error="source s1 failed"),
    ])

    rc = main([*argv, "--dsn", clean_runtime])
    text = capsys.readouterr().out

    assert rc == 1, text
    assert f"published graph version g-1; 1 enricher failed: {ENRICHER}" in text
    assert "exit code stays 0" not in text
    assert "this command exits 1" in text


@pytest.mark.postgres
@pytest.mark.parametrize("failing_batch", [0, 1])
def test_queued_ingest_reads_every_batch_report(
    failing_batch: int,
    tmp_path,
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reports = [
        {"publishes": [_clean_publish("g-clean")]},
        {"publishes": [_clean_publish("g-clean")]},
    ]
    reports[failing_batch] = {"publishes": [_failed_publish("g-failed")]}
    argv = _queued_ingest(
        tmp_path, monkeypatch, [_queued(report) for report in reports],
    )

    rc = main([*argv, "--dsn", clean_runtime, "--json"])
    payload = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert [
        (row["name"], row["generation_id"])
        for row in payload["enricher_failures"]
    ] == [(ENRICHER, "g-failed")]


@pytest.mark.postgres
def test_queued_ingest_typed_refusal_keeps_earlier_enricher_failures(
    tmp_path,
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    argv = _queued_ingest(tmp_path, monkeypatch, [
        _queued({"publishes": [_failed_publish("g-1")]}),
        _queued(
            None, status="ERROR", any_failed=True,
            error="batch two refused", error_code="source_sync_refused",
        ),
    ])

    rc = main([*argv, "--dsn", clean_runtime, "--json"])
    error = json.loads(capsys.readouterr().err)["error"]

    assert rc == 1
    assert error["code"] == "source_sync_refused"
    assert [
        (row["name"], row["generation_id"])
        for row in error["data"]["enricher_failures"]
    ] == [(ENRICHER, "g-1")]


@pytest.mark.postgres
def test_plain_doctor_names_why_the_enricher_registry_is_unknown(
    tmp_path,
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _publisher(clean_runtime).cycle()
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(_write_deployment(
        tmp_path, enricher_classes=["okg_no_such_enricher_module.Missing"],
    )))

    main(["doctor", "--deployment", DEPLOYMENT, "--dsn", clean_runtime, "--json"])
    payload = json.loads(capsys.readouterr().out)
    liveness = payload["enricher_liveness"]

    # The registry could not be read: every row stays current (the failing
    # enricher is still warned about), and the block says why.
    assert liveness["registry"] == "unknown"
    assert "okg_no_such_enricher_module" in liveness["registry_error"]
    assert liveness["failed_count"] == 1
    assert liveness["stale_enrichers"] == []
    unreadable = [
        w for w in payload.get("warnings") or []
        if w.get("code") == "enricher_registry_unreadable"
    ]
    assert len(unreadable) == 1
    assert "okg_no_such_enricher_module" in unreadable[0]["message"]


@pytest.mark.postgres
def test_plain_doctor_names_only_registered_silent_enrichers(
    tmp_path,
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.cli import doctor as doctor_module

    monkeypatch.setattr(
        doctor_module, "_enricher_liveness_findings",
        lambda _conn, _deployment: [
            {"enricher_name": "other_registered_enricher"},
            {"enricher_name": "removed_silent_enricher"},
        ],
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(_write_deployment(
        tmp_path, enricher_classes=[f"{_THIS_MODULE}._OtherEnricher"],
    )))

    main(["doctor", "--deployment", DEPLOYMENT, "--dsn", clean_runtime, "--json"])
    liveness = json.loads(capsys.readouterr().out)["enricher_liveness"]

    assert liveness["silent_enrichers"] == ["other_registered_enricher"]
    assert liveness["silent_count"] == 1
    assert liveness["stale_enrichers"] == ["removed_silent_enricher"]
