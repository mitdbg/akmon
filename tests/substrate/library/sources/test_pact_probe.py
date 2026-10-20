"""PACT source probes for issue #323.

The DB-free tests cover PACTSource's exact evaluation snapshot. Database tests
cover the standalone coordination-event ledger probe.
"""
from __future__ import annotations

import hashlib
import os
import subprocess
from pathlib import Path

import psycopg
import pytest
import yaml

from okg.substrate.library.sources.change_probe import (
    tokens_equal,
    validate_token_composition,
)
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.library.sources.pact import (
    PACTCoordinationEventSource,
    PACTSource,
    _LedgerWatermarkProbe,
)

@pytest.fixture(autouse=True)
def _ledger_off(monkeypatch):
    # Default these tests to the disabled ledger so the union depends only on the
    # git half (no DB needed); the ledger-on path is exercised explicitly below.
    monkeypatch.setenv("OKG_PACT_DECISION_PROJECTION", "off")


@pytest.fixture
def repo_with_pact(tmp_path):
    repo = tmp_path / "repo"
    _write_manifest(repo, {
        "pact_version": 4,
        "target_pact_version": 4,
        "change": {"id": "demo", "title": "Demo"},
        "requirements": [{
            "id": "req.demo",
            "statement": "The demo source remains exact.",
            "evidence_policy": {"required": ["executed_test"]},
        }],
        "tasks": [{
            "id": "task.demo",
            "title": "Verify demo",
            "status": "todo",
            "verification": "pytest -q",
            "requirements": ["req.demo"],
        }],
    })
    (repo / "README.md").write_text("top\n")  # outside the pact subtree
    subprocess.run(["git", "-C", str(repo), "init", "-q"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.email", "t@t"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.name", "t"], check=True)
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-q", "-m", "i"], check=True)
    return repo


def test_declares_exact_content_probe(repo_with_pact):
    assert PACTSource.change_probe_kind == "content_hash"
    s = PACTSource(repo="demo", pact_root=str(repo_with_pact / "pact"))
    assert s.probe_short_circuit_safe is True
    validate_token_composition(s.change_probe.build_token())


def test_manifest_inventory_cache_survives_fresh_adapter(
    repo_with_pact,
    monkeypatch,
):
    from okg.substrate.library.sources import pact as pact_source

    pact_source._cached_manifest_inventory.cache_clear()
    original = pact_source.load_manifest_bytes
    parse_calls = 0

    def counted_load_manifest_bytes(*args, **kwargs):
        nonlocal parse_calls
        parse_calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(
        pact_source,
        "load_manifest_bytes",
        counted_load_manifest_bytes,
    )
    first = PACTSource(repo="demo", pact_root=repo_with_pact / "pact")
    first_token = first.change_probe.build_token()
    first_parse_calls = parse_calls
    assert first_parse_calls > 0

    second = PACTSource(repo="demo", pact_root=repo_with_pact / "pact")
    second_token = second.change_probe.build_token()

    assert tokens_equal(first_token, second_token)
    assert parse_calls == first_parse_calls
    pact_source._cached_manifest_inventory.cache_clear()


def test_pact_file_edit_inside_subtree_detected(repo_with_pact):
    s = PACTSource(repo="demo", pact_root=str(repo_with_pact / "pact"))
    t0 = s.change_probe.build_token()
    manifest_path = (
        repo_with_pact / "pact" / "changes" / "demo" / "pact.yaml"
    )
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    manifest["change"]["title"] = "Demo changed"
    manifest_path.write_text(
        yaml.safe_dump(manifest),
        encoding="utf-8",
    )
    assert not tokens_equal(t0, s.change_probe.build_token())


def test_edit_outside_pact_subtree_ignored(repo_with_pact):
    s = PACTSource(repo="demo", pact_root=str(repo_with_pact / "pact"))
    t0 = s.change_probe.build_token()
    (repo_with_pact / "README.md").write_text("top-edited\n")
    assert tokens_equal(t0, s.change_probe.build_token())


def test_pact_file_delete_detected(repo_with_pact):
    s = PACTSource(repo="demo", pact_root=str(repo_with_pact / "pact"))
    t0 = s.change_probe.build_token()
    (repo_with_pact / "pact" / "changes" / "demo" / "pact.yaml").unlink()
    assert not tokens_equal(t0, s.change_probe.build_token())


def _write_manifest(repo, manifest):
    path = repo / "pact" / "changes" / "demo" / "pact.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(manifest), encoding="utf-8")
    return path


def _node(facts, node_id):
    return next(
        fact
        for fact in facts
        if isinstance(fact, NodeFact) and fact.node_id == node_id
    )


def _run_with_cursor(source, run_id, *, cursor=None, scope_key=None):
    run = source.run(
        run_id,
        mode="cursor",
        cursor=cursor,
        sync_scope=(
            {"scope_keys": [scope_key]}
            if scope_key is not None
            else None
        ),
    )
    return run, list(run.facts)


def _digest(path):
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _commit(repo, message, timestamp):
    env = {
        **os.environ,
        "GIT_AUTHOR_DATE": timestamp,
        "GIT_COMMITTER_DATE": timestamp,
    }
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "commit", "-q", "-m", message],
        check=True,
        env=env,
    )


def _init_git(repo):
    subprocess.run(["git", "-C", str(repo), "init", "-q"], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.email", "t@t"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.name", "t"],
        check=True,
    )
    _commit(repo, "initial", "2025-01-01T00:00:00+00:00")


def test_project_policy_change_busts_token_and_changes_facts(tmp_path):
    repo = tmp_path / "repo"
    manifest = {
        "pact_version": 0,
        "target_pact_version": 1,
        "change": {"id": "demo", "title": "Demo"},
        "requirements": [{
            "id": "req.demo",
            "statement": "custom proof must satisfy the requirement",
            "evidence_policy": {"required": ["proof_alias"]},
        }],
        "tasks": [{
            "id": "task.demo",
            "title": "Demo task",
            "status": "done",
            "verification": "pytest -q",
            "requirements": ["req.demo"],
        }],
        "evidence": [{
            "id": "ev.proof.demo",
            "kind": "proof_canonical",
            "status": "pass",
            "refs": ["req.demo", "task.demo"],
        }],
    }
    _write_manifest(repo, manifest)
    _init_git(repo)
    source = PACTSource(repo="demo", pact_root=repo / "pact")
    before_token = source.change_probe.build_token()
    first, first_facts = _run_with_cursor(source, "project-before")
    first_req = _node(first_facts, "pact-req:demo:req.demo")
    assert first_req.attrs["satisfaction_status"] == "unsatisfied"

    (repo / "pact" / "project.yaml").write_text(
        yaml.safe_dump({
            "evidence_kinds": {
                "canonical": ["proof_canonical"],
                "aliases": {"proof_alias": "proof_canonical"},
            },
        }),
        encoding="utf-8",
    )
    after_token = source.change_probe.build_token()
    assert not tokens_equal(before_token, after_token)

    second, second_facts = _run_with_cursor(
        source,
        "project-after",
        cursor=first.next_cursor,
    )
    second_req = _node(second_facts, "pact-req:demo:req.demo")
    assert second_req.attrs["satisfaction_status"] == "satisfied"
    assert second_req.source_revision != first_req.source_revision
    assert second.applied_raw_token is not None
    assert second.applied_token_authority == "exact"
    assert second.applied_token_scope == "global"


def test_formal_artifact_change_busts_token_and_changes_facts(tmp_path):
    repo = tmp_path / "repo"
    module = repo / "formal" / "Demo.tla"
    config = repo / "formal" / "Demo.cfg"
    refinement = repo / "tests" / "test_demo.py"
    artifact = repo / "artifacts" / "formal-report.yaml"
    for path, content in (
        (module, "---- MODULE Demo ----\n====\n"),
        (config, "SPECIFICATION Spec\n"),
        (refinement, "def test_demo():\n    assert True\n"),
        (artifact, "status: pass\n"),
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    checker = "tlc2.TLC formal/Demo.tla"
    refinement_ref = "tests/test_demo.py::test_demo"
    manifest = {
        "pact_version": 0,
        "target_pact_version": 3,
        "change": {"id": "demo", "title": "Demo"},
        "requirements": [{
            "id": "req.demo",
            "statement": "formal proof must remain fresh",
            "evidence_policy": {"required": ["formal_check"]},
        }],
        "tasks": [{
            "id": "task.demo",
            "title": "Demo task",
            "status": "done",
            "verification": "pytest -q",
            "requirements": ["req.demo"],
            "evidence": ["ev.formal.demo"],
        }],
        "formal": [{
            "id": "formal.demo",
            "requirement": "req.demo",
            "property_id": "DemoInvariant",
            "module": "formal/Demo.tla",
            "config": "formal/Demo.cfg",
            "checker_command": checker,
            "refinement_tests": [refinement_ref],
        }],
        "evidence": [{
            "id": "ev.formal.demo",
            "kind": "formal_check",
            "status": "pass",
            "refs": ["formal.demo", "req.demo", "task.demo"],
            "artifact": "artifacts/formal-report.yaml",
            "data": {
                "property_id": "DemoInvariant",
                "checker_command": checker,
                "fresh": True,
                "refinement_tests": [refinement_ref],
                "module_digest": _digest(module),
                "config_digest": _digest(config),
                "artifact_digest": _digest(artifact),
            },
        }],
    }
    _write_manifest(repo, manifest)
    _init_git(repo)
    source = PACTSource(repo="demo", pact_root=repo / "pact")
    before_token = source.change_probe.build_token()
    first, first_facts = _run_with_cursor(source, "artifact-before")
    first_req = _node(first_facts, "pact-req:demo:req.demo")
    assert first_req.attrs["satisfaction_status"] == "satisfied"

    artifact.write_text("status: pass\nrun: newer\n", encoding="utf-8")
    after_token = source.change_probe.build_token()
    assert not tokens_equal(before_token, after_token)

    second, second_facts = _run_with_cursor(
        source,
        "artifact-after",
        cursor=first.next_cursor,
    )
    second_req = _node(second_facts, "pact-req:demo:req.demo")
    assert second_req.attrs["satisfaction_status"] == "unsatisfied"
    assert second_req.source_revision != first_req.source_revision
    assert second.applied_raw_token is not None
    assert second.applied_token_authority == "exact"


def test_git_history_change_busts_token_and_changes_derived_state(
    tmp_path,
):
    repo = tmp_path / "repo"
    app = repo / "src" / "app.py"
    app.parent.mkdir(parents=True)
    app.write_text("VALUE = 1\n", encoding="utf-8")
    manifest = {
        "pact_version": 0,
        "target_pact_version": 1,
        "change": {
            "id": "demo",
            "title": "Demo",
            "affected_code": ["src/app.py"],
        },
        "requirements": [{
            "id": "req.demo",
            "statement": "executed proof must remain newer than code",
            "evidence_policy": {"required": ["pytest"]},
        }],
        "tasks": [{
            "id": "task.demo",
            "title": "Demo task",
            "status": "done",
            "verification": "pytest -q",
            "requirements": ["req.demo"],
        }],
        "test_plan": [{
            "id": "test.demo",
            "requirement": "req.demo",
            "test_id": "tests/test_demo.py::test_demo",
        }],
        "evidence": [{
            "id": "ev.pytest.demo",
            "kind": "pytest",
            "status": "pass",
            "refs": ["req.demo", "task.demo", "test.demo"],
            "data": {
                "attached_at": "2026-01-01T00:00:00Z",
                "junit_test_ids": ["tests/test_demo.py::test_demo"],
            },
        }],
    }
    _write_manifest(repo, manifest)
    (repo / "tests").mkdir()
    (repo / "tests" / "test_demo.py").write_text(
        "def test_demo():\n    assert True\n",
        encoding="utf-8",
    )
    subprocess.run(["git", "-C", str(repo), "init", "-q"], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.email", "t@t"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.name", "t"],
        check=True,
    )
    _commit(repo, "initial", "2025-01-01T00:00:00+00:00")

    source = PACTSource(repo="demo", pact_root=repo / "pact")
    before_token = source.change_probe.build_token()
    first, first_facts = _run_with_cursor(source, "history-before")
    first_change = _node(first_facts, "pact-change:demo")
    assert first_change.attrs["derived_state"] == "awaiting_review"

    app.write_text("VALUE = 2\n", encoding="utf-8")
    _commit(repo, "new code", "2027-01-01T00:00:00+00:00")
    after_token = source.change_probe.build_token()
    assert not tokens_equal(before_token, after_token)

    second, second_facts = _run_with_cursor(
        source,
        "history-after",
        cursor=first.next_cursor,
    )
    second_change = _node(second_facts, "pact-change:demo")
    assert second_change.attrs["derived_state"] == "in_progress"
    assert second_change.source_revision != first_change.source_revision
    assert second.applied_raw_token is not None
    assert second.applied_token_authority == "exact"


def test_shallow_history_deepening_busts_token_and_changes_derived_state(
    tmp_path,
):
    repo = tmp_path / "repo"
    app = repo / "src" / "app.py"
    app.parent.mkdir(parents=True)
    app.write_text("VALUE = 1\n", encoding="utf-8")
    _write_manifest(repo, {
        "pact_version": 0,
        "target_pact_version": 1,
        "change": {
            "id": "demo",
            "title": "Demo",
            "affected_code": ["src/app.py"],
        },
        "requirements": [{
            "id": "req.demo",
            "statement": "proof remains newer than affected code",
            "evidence_policy": {"required": ["pytest"]},
        }],
        "tasks": [{
            "id": "task.demo",
            "title": "Demo task",
            "status": "done",
            "verification": "pytest -q",
            "requirements": ["req.demo"],
        }],
        "evidence": [{
            "id": "ev.pytest.demo",
            "kind": "pytest",
            "status": "pass",
            "refs": ["req.demo", "task.demo"],
            "data": {"attached_at": "2026-01-01T00:00:00Z"},
        }],
    })
    subprocess.run(["git", "-C", str(repo), "init", "-q"], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.email", "t@t"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.name", "t"],
        check=True,
    )
    _commit(repo, "initial", "2025-01-01T00:00:00+00:00")
    (repo / "unrelated.txt").write_text("second\n", encoding="utf-8")
    _commit(repo, "unrelated", "2025-02-01T00:00:00+00:00")

    head = subprocess.check_output(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        text=True,
    ).strip()
    shallow_raw = subprocess.check_output(
        ["git", "-C", str(repo), "rev-parse", "--git-path", "shallow"],
        text=True,
    ).strip()
    shallow_path = Path(shallow_raw)
    if not shallow_path.is_absolute():
        shallow_path = repo / shallow_path
    shallow_path.write_text(f"{head}\n", encoding="utf-8")

    source = PACTSource(repo="demo", pact_root=repo / "pact")
    shallow_token = source.change_probe.build_token()
    shallow_run, shallow_facts = _run_with_cursor(source, "shallow")
    shallow_req = _node(shallow_facts, "pact-req:demo:req.demo")
    assert shallow_req.attrs["satisfaction_status"] == "satisfied"

    shallow_path.unlink()
    deep_token = source.change_probe.build_token()
    assert not tokens_equal(shallow_token, deep_token)
    deep_run, deep_facts = _run_with_cursor(
        source,
        "deepened",
        cursor=shallow_run.next_cursor,
    )
    deep_req = _node(deep_facts, "pact-req:demo:req.demo")
    assert deep_req.attrs["satisfaction_status"] == "satisfied"
    assert deep_req.source_revision != shallow_req.source_revision
    assert deep_run.applied_raw_token is not None
    assert deep_run.applied_token_authority == "exact"


# ---- PACTCoordinationEventSource: a standalone ledger-watermark probe ----

def test_coordination_event_source_declares_mutable_api_ledger_probe():
    assert PACTCoordinationEventSource.change_probe_kind == "mutable_api"
    assert PACTCoordinationEventSource.probe_short_circuit_safe is True
    s = PACTCoordinationEventSource(repo="okg")
    assert isinstance(s.change_probe, _LedgerWatermarkProbe)


def test_coordination_event_probe_token_is_sound_and_source_scoped():
    # Disabled ledger -> stable token (no DB), still a sound composition.
    s = PACTCoordinationEventSource(repo="okg")
    tok = s.change_probe.build_token()
    validate_token_composition(tok)
    assert tokens_equal(tok, s.change_probe.build_token())
    # The probe fingerprints THIS source class, so its emit_version differs from
    # the default (probe-class) emit target -- an emission-logic edit busts it.
    default_probe = _LedgerWatermarkProbe("okg")
    assert (
        tok["components"]["emit_version"]
        != default_probe.build_token()["components"]["emit_version"]
    )


@pytest.mark.postgres
def test_coordination_event_probe_busts_on_new_coordination_event(
    okg_migrated, monkeypatch
):
    monkeypatch.setenv("OKG_DSN", okg_migrated)
    monkeypatch.setenv("OKG_PACT_DECISION_PROJECTION", "on")
    from okg.substrate.pact import decisions

    with psycopg.connect(okg_migrated) as schema_conn:
        schema_conn.execute(decisions._DDL)
    monkeypatch.setattr(
        decisions,
        "require_ledger_connection",
        lambda: psycopg.connect(okg_migrated),
    )

    source = PACTCoordinationEventSource(repo="okg")
    before = source.change_probe.build_token()
    conn = psycopg.connect(okg_migrated)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {decisions.COORDINATION_EVENT_TABLE}
                    (event_type, change_id, payload)
                VALUES (%s, %s, %s::jsonb)
                """,
                ("probe-test", "coordination-probe-test", "{}"),
            )
        conn.commit()
    finally:
        conn.close()

    assert not tokens_equal(before, source.change_probe.build_token())


@pytest.mark.postgres
def test_coordination_event_probe_busts_on_in_place_update_from_exact_rows(
    okg_migrated,
    monkeypatch,
):
    monkeypatch.setenv("OKG_DSN", okg_migrated)
    monkeypatch.setenv("OKG_PACT_DECISION_PROJECTION", "on")
    from okg.substrate.pact import decisions

    with psycopg.connect(okg_migrated) as schema_conn:
        schema_conn.execute(decisions._DDL)
    monkeypatch.setattr(
        decisions,
        "require_ledger_connection",
        lambda: psycopg.connect(okg_migrated),
    )

    with psycopg.connect(okg_migrated) as conn:
        event_id = conn.execute(
            f"""
            INSERT INTO {decisions.COORDINATION_EVENT_TABLE}
                (event_type, change_id, principal, payload)
            VALUES (%s, %s, %s, %s::jsonb)
            RETURNING id
            """,
            (
                "probe-update",
                "coordination-probe-update",
                "before",
                '{"version": 1}',
            ),
        ).fetchone()[0]

    source = PACTCoordinationEventSource(repo="okg")
    before_token = source.change_probe.build_token()
    with psycopg.connect(okg_migrated) as conn:
        before_watermark = decisions.load_coordination_event_watermark(conn)
        conn.execute(
            f"""
            UPDATE {decisions.COORDINATION_EVENT_TABLE}
               SET principal = %s, payload = %s::jsonb
             WHERE id = %s
            """,
            ("after", '{"version": 2}', event_id),
        )
    with psycopg.connect(okg_migrated) as conn:
        after_watermark = decisions.load_coordination_event_watermark(conn)

    after_token = source.change_probe.build_token()
    assert before_watermark == after_watermark
    assert not tokens_equal(before_token, after_token)

    replay = source.run("update-replay", authoritative_replay=True)
    facts = list(replay.facts)
    updated = next(
        fact
        for fact in facts
        if int(fact.source_record_id["event_id"]) == event_id
    )
    assert updated.attrs["actor_id"] == "after"
    assert updated.attrs["payload"] == {"version": 2}
    assert replay.applied_raw_token == source.change_probe.token_for_value(
        source.last_run_stats["ledger"]["identity"]
    )
    assert tokens_equal(replay.applied_raw_token, source.change_probe.build_token())


@pytest.mark.postgres
def test_coordination_event_snapshot_delete_changes_applied_identity_and_record_set(
    okg_migrated,
    monkeypatch,
):
    monkeypatch.setenv("OKG_DSN", okg_migrated)
    monkeypatch.setenv("OKG_PACT_DECISION_PROJECTION", "on")
    from okg.substrate.pact import decisions

    with psycopg.connect(okg_migrated) as schema_conn:
        schema_conn.execute(decisions._DDL)
    monkeypatch.setattr(
        decisions,
        "require_ledger_connection",
        lambda: psycopg.connect(okg_migrated),
    )

    with psycopg.connect(okg_migrated) as conn:
        event_id = conn.execute(
            f"""
            INSERT INTO {decisions.COORDINATION_EVENT_TABLE}
                (event_type, change_id, payload)
            VALUES (%s, %s, %s::jsonb)
            RETURNING id
            """,
            ("probe-delete", "coordination-probe-delete", "{}"),
        ).fetchone()[0]

    source = PACTCoordinationEventSource(repo="okg")
    before = source.run("before-delete", authoritative_replay=True)
    list(before.facts)
    assert before.record_set is not None
    assert str(event_id) in before.record_set.records

    with psycopg.connect(okg_migrated) as conn:
        conn.execute(
            f"DELETE FROM {decisions.COORDINATION_EVENT_TABLE} WHERE id = %s",
            (event_id,),
        )

    after_probe = source.change_probe.build_token()
    assert not tokens_equal(before.applied_raw_token, after_probe)
    after = source.run("after-delete", authoritative_replay=True)
    list(after.facts)
    assert after.record_set is not None
    assert str(event_id) not in after.record_set.records
    assert tokens_equal(after.applied_raw_token, after_probe)


@pytest.mark.postgres
def test_coordination_snapshot_detects_delayed_commit_after_sequence_allocation(
    okg_migrated,
    monkeypatch,
):
    monkeypatch.setenv("OKG_DSN", okg_migrated)
    monkeypatch.setenv("OKG_PACT_DECISION_PROJECTION", "on")
    from okg.substrate.pact import decisions

    with psycopg.connect(okg_migrated) as schema_conn:
        schema_conn.execute(decisions._DDL)
    monkeypatch.setattr(
        decisions,
        "require_ledger_connection",
        lambda: psycopg.connect(okg_migrated),
    )

    slow = psycopg.connect(okg_migrated)
    try:
        slow_id = slow.execute(
            f"""
            INSERT INTO {decisions.COORDINATION_EVENT_TABLE}
                (event_type, change_id, payload)
            VALUES (%s, %s, %s::jsonb)
            RETURNING id
            """,
            ("slow-commit", "coordination-probe-sequence", "{}"),
        ).fetchone()[0]
        with psycopg.connect(okg_migrated) as fast:
            fast_id = fast.execute(
                f"""
                INSERT INTO {decisions.COORDINATION_EVENT_TABLE}
                    (event_type, change_id, payload)
                VALUES (%s, %s, %s::jsonb)
                RETURNING id
                """,
                ("fast-commit", "coordination-probe-sequence", "{}"),
            ).fetchone()[0]
        assert slow_id < fast_id

        source = PACTCoordinationEventSource(repo="okg")
        captured = source.run("before-slow-commit", authoritative_replay=True)
        facts = list(captured.facts)
        captured_ids = {
            int(fact.source_record_id["event_id"])
            for fact in facts
        }
        assert fast_id in captured_ids
        assert slow_id not in captured_ids
        captured_token = captured.applied_raw_token
        assert captured_token == source.change_probe.token_for_value(
            source.last_run_stats["ledger"]["identity"]
        )

        slow.commit()
        after_commit = source.change_probe.build_token()
        assert not tokens_equal(captured_token, after_commit)
    finally:
        slow.close()


# ---- ledger watermark (cheap max+count) against a migrated DB ----

@pytest.mark.postgres
def test_load_decision_watermark_detects_insert_and_delete(okg_migrated, monkeypatch):
    monkeypatch.setenv("OKG_DSN", okg_migrated)
    from okg.substrate.pact import decisions

    conn = psycopg.connect(okg_migrated)
    try:
        conn.execute(decisions._DDL)
        conn.commit()
        w0 = decisions.load_decision_watermark(conn)
        rec = decisions.record_decision(
            conn,
            change_id="chg-probe-test",
            verdict="approve",
            principal="tester",
            authority="human",
            from_state="awaiting_review",
            to_state="retired",
            intent_digest="sha256:x",
        )
        w1 = decisions.load_decision_watermark(conn)
        assert (w1["max_id"], w1["count"]) != (w0["max_id"], w0["count"])
        assert w1["max_id"] >= rec["decision_id"]
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {decisions.LEDGER_TABLE} WHERE id = %s",
                (rec["decision_id"],),
            )
        conn.commit()
        w2 = decisions.load_decision_watermark(conn)
        assert w2["count"] == w0["count"]  # delete restores the count
    finally:
        conn.close()


@pytest.mark.postgres
def test_decision_probe_detects_in_place_row_mutation(
    okg_migrated,
    monkeypatch,
):
    monkeypatch.setenv("OKG_DSN", okg_migrated)
    monkeypatch.setenv("OKG_PACT_DECISION_PROJECTION", "on")
    from okg.substrate.pact.decisions import (
        LEDGER_TABLE,
        record_decision,
    )
    from okg.substrate.pact import decisions

    with psycopg.connect(okg_migrated) as schema_conn:
        schema_conn.execute(decisions._DDL)
    monkeypatch.setattr(
        decisions,
        "require_ledger_connection",
        lambda: psycopg.connect(okg_migrated),
    )

    probe = _LedgerWatermarkProbe("okg", watermark_kind="decisions")
    conn = psycopg.connect(okg_migrated)
    try:
        rec = record_decision(
            conn,
            change_id="chg-probe-row-mutation",
            verdict="approve",
            principal="tester",
            authority="human",
            from_state="awaiting_review",
            to_state="retired",
            intent_digest="sha256:row-mutation",
        )
        before = probe.build_token()
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE {LEDGER_TABLE} SET principal = %s WHERE id = %s",
                ("mutated-tester", rec["decision_id"]),
            )
        conn.commit()
        after = probe.build_token()
    finally:
        conn.close()

    assert not tokens_equal(before, after)
