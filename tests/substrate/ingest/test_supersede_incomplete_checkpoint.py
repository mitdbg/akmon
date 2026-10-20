"""An operator can retire an incomplete checkpoint's repair contract
(issue #1599), and the CLI verb `runtime supersede-checkpoint` reaches it.

On okg-workspace (2026-09-04) `agent-sessions` failed its record-set coverage
check after `git-files` had already completed in the same workflow. The
resulting incomplete checkpoint recorded an EXACT repair contract naming both
sources, and because `agent-sessions` could not be made to pass, every later
workflow refused with `incomplete_recovery_contract_mismatch` — 34 errors,
with no documented way out that applied.
"""
from __future__ import annotations

import argparse
import itertools
import json
import shlex
import uuid
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import psycopg
import pytest
import yaml

from okg.substrate.cli import runtime as cli_runtime
from okg.substrate.cli.main import _build_parser
from okg.substrate.errors import SubstrateError
from okg.substrate.db import pool as pool_module
from okg.substrate.ingest import incomplete_recovery as ir
from okg.substrate.ingest import IngestOptions, run_ingest
from tests.pytest.catalog_fixture_builders import seed_catalog
from tests.pytest.pre_3342_freeze import freeze_partial_failures


DSN = "postgresql://fake/supersede"
DEPLOYMENT = "okg-workspace"
CHECKPOINT = "20260904T042246366836Z:382cb2a39d97"


def _blocked_state(state=ir.REPAIR_REQUIRED, **recovery_extra):
    recovery = {
        "schema": ir.RECOVERY_SCHEMA,
        "state": state,
        "predecessor_checkpoint_id": "20260904T012400000000Z:aaaa",
        "source_contract": [
            {"source": "git-files", "run_mode": "scope_complete"},
            {"source": "agent-sessions", "run_mode": "scope_complete"},
        ],
        "required_sources": ["agent-sessions", "git-files"],
        "admission_ids": [],
    }
    recovery.update(recovery_extra)
    return {
        "staged_work_present": True,
        "latest_checkpoint_id": CHECKPOINT,
        "latest_checkpoint_status": "blocked",
        "latest_blocked_reason": ir.BLOCKED_INGEST_REASON,
        "latest_blocked_receipt": {
            "reason": ir.BLOCKED_INGEST_REASON,
            "status": "incomplete",
            "recovery": recovery,
        },
        "latest_published_checkpoint_id": None,
        "branch_mutation_token": "token",
    }


@pytest.fixture
def blocked(monkeypatch):
    state = _blocked_state()
    monkeypatch.setattr(
        ir, "staged_source_redrive_state", lambda **_: state,
    )
    return state


class _FakeCursor:
    def __init__(self, rowcount: int) -> None:
        self.rowcount = rowcount
        self.executed: list[tuple] = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakeConnection:
    def __init__(self, rowcount: int) -> None:
        self.cursor_obj = _FakeCursor(rowcount)

    def cursor(self):
        return self.cursor_obj

    @contextmanager
    def transaction(self):
        yield


def _install_fake_write(monkeypatch, rowcount: int) -> _FakeConnection:
    conn = _FakeConnection(rowcount)

    @contextmanager
    def _fake_pooled(dsn, *, purpose, **kwargs):
        yield conn

    monkeypatch.setattr(pool_module, "pooled_connection", _fake_pooled)
    return conn


# ---------------------------------------------------------------------------
# Refusals
# ---------------------------------------------------------------------------


def test_a_reason_is_required(blocked):
    with pytest.raises(SubstrateError) as exc:
        ir.supersede_incomplete_checkpoint(
            dsn=DSN, deployment=DEPLOYMENT, reason="   ", apply=True,
        )
    assert exc.value.data["reason"] == (
        "incomplete_recovery_supersede_reason_required"
    )


def test_refuses_when_there_is_no_incomplete_checkpoint(monkeypatch):
    monkeypatch.setattr(
        ir,
        "staged_source_redrive_state",
        lambda **_: {
            "staged_work_present": False,
            "latest_checkpoint_id": CHECKPOINT,
            "latest_checkpoint_status": "published",
            "latest_blocked_reason": None,
            "latest_blocked_receipt": {},
            "latest_published_checkpoint_id": CHECKPOINT,
            "branch_mutation_token": "token",
        },
    )
    with pytest.raises(SubstrateError) as exc:
        ir.supersede_incomplete_checkpoint(
            dsn=DSN, deployment=DEPLOYMENT, reason="cleanup", apply=True,
        )
    assert exc.value.data["reason"] == (
        "incomplete_recovery_nothing_to_supersede"
    )


def test_refuses_a_checkpoint_id_that_is_not_the_latest(blocked):
    with pytest.raises(SubstrateError) as exc:
        ir.supersede_incomplete_checkpoint(
            dsn=DSN,
            deployment=DEPLOYMENT,
            reason="cleanup",
            checkpoint_id="some-other-checkpoint",
            apply=True,
        )
    assert exc.value.data["reason"] == (
        "incomplete_recovery_supersede_checkpoint_drift"
    )


def test_refuses_when_already_superseded(monkeypatch):
    monkeypatch.setattr(
        ir,
        "staged_source_redrive_state",
        lambda **_: _blocked_state(
            state=ir.SUPERSEDED,
            supersede_reason="first time",
            superseded_by="operator",
        ),
    )
    with pytest.raises(SubstrateError) as exc:
        ir.supersede_incomplete_checkpoint(
            dsn=DSN, deployment=DEPLOYMENT, reason="again", apply=True,
        )
    assert exc.value.data["reason"] == "incomplete_recovery_already_superseded"
    assert exc.value.data["supersede_reason"] == "first time"


def test_a_concurrent_change_refuses_instead_of_overwriting(
    blocked, monkeypatch,
):
    """The UPDATE pins the prior state; zero rows means someone moved it."""
    _install_fake_write(monkeypatch, rowcount=0)
    with pytest.raises(SubstrateError) as exc:
        ir.supersede_incomplete_checkpoint(
            dsn=DSN, deployment=DEPLOYMENT, reason="unfixable source",
            apply=True,
        )
    assert exc.value.data["reason"] == (
        "incomplete_recovery_supersede_state_drift"
    )


# ---------------------------------------------------------------------------
# The happy paths
# ---------------------------------------------------------------------------


def test_dry_run_changes_nothing_and_names_the_contract(blocked, monkeypatch):
    conn = _install_fake_write(monkeypatch, rowcount=1)
    report = ir.supersede_incomplete_checkpoint(
        dsn=DSN, deployment=DEPLOYMENT, reason="agent-sessions cannot pass",
    )
    assert report["applied"] is False
    assert report["checkpoint_id"] == CHECKPOINT
    assert report["prior_state"] == ir.REPAIR_REQUIRED
    assert report["required_sources"] == ["agent-sessions", "git-files"]
    assert "--apply" in report["next_command"]
    assert conn.cursor_obj.executed == [], "a dry run must not write"


def test_apply_records_who_why_and_when(blocked, monkeypatch):
    conn = _install_fake_write(monkeypatch, rowcount=1)
    report = ir.supersede_incomplete_checkpoint(
        dsn=DSN,
        deployment=DEPLOYMENT,
        reason="agent-sessions cannot pass coverage; see #1599",
        actor="riley",
        apply=True,
    )
    assert report["applied"] is True
    assert report["superseded_at"]

    (sql, params), = conn.cursor_obj.executed
    assert "_chronos_branch_interval_checkpoints" in sql
    # The checkpoint's own status is untouched: nothing here claims the
    # incomplete sources ingested.
    assert "'{blocked_reason,recovery}'" in sql
    # The write is scoped to the recovery object. The checkpoint's own
    # `status` stays `blocked`: nothing here claims the incomplete sources
    # ingested, and no history is deleted.
    assert "'{status}'" not in sql
    assert "DELETE" not in sql.upper()
    # The compare-and-swap pins the WHOLE recovery object, not just its
    # state, so a contract that moved cannot be silently retired.
    prior = json.loads(params["prior_recovery"])
    assert prior["state"] == ir.REPAIR_REQUIRED
    assert prior["required_sources"] == ["agent-sessions", "git-files"]
    patch = json.loads(params["patch"])
    assert patch["state"] == ir.SUPERSEDED
    assert patch["superseded_state"] == ir.REPAIR_REQUIRED
    assert patch["superseded_by"] == "riley"
    assert patch["supersede_reason"].startswith("agent-sessions cannot pass")


def test_a_superseded_checkpoint_no_longer_demands_a_repair_contract():
    """The wedge is what supersession removes."""
    wedged = _blocked_state()
    with pytest.raises(SubstrateError) as exc:
        ir.resume_before_source_writes(
            redrive_state=wedged,
            deployment=DEPLOYMENT,
            # A different workflow, with a different source selection.
            source_contract=[{"source": "code-structure"}],
            ensure_feedback=lambda *a: None,
            load_state=lambda: wedged,
            promote=lambda *a: {},
        )
    assert exc.value.data["reason"] == (
        "incomplete_recovery_contract_mismatch"
    )

    retired = _blocked_state(
        state=ir.SUPERSEDED,
        superseded_state=ir.REPAIR_REQUIRED,
        supersede_reason="agent-sessions cannot pass coverage",
        superseded_by="riley",
    )
    resolution = ir.resume_before_source_writes(
        redrive_state=retired,
        deployment=DEPLOYMENT,
        source_contract=[{"source": "code-structure"}],
        ensure_feedback=lambda *a: None,
        load_state=lambda: retired,
        promote=lambda *a: {},
    )
    assert resolution["authority"] is None
    assert resolution["report"] is None


def test_a_superseded_receipt_is_not_mistaken_for_a_legacy_one():
    """The legacy branch refuses the whole run; supersession must not."""
    retired = _blocked_state(state=ir.SUPERSEDED)
    assert ir.recovery_from_redrive_state(
        retired, deployment=DEPLOYMENT,
    ) is None


# ---------------------------------------------------------------------------
# The mismatch refusal names the way out (issue #2074)
# ---------------------------------------------------------------------------


def _mismatch_refusal(source_contract, *, deployment=DEPLOYMENT):
    wedged = _blocked_state()
    with pytest.raises(SubstrateError) as exc:
        ir.resume_before_source_writes(
            redrive_state=wedged,
            deployment=deployment,
            source_contract=source_contract,
            ensure_feedback=lambda *a: None,
            load_state=lambda: wedged,
            promote=lambda *a: {},
        )
    return exc.value


def _parsed_escape_hatch(command):
    argv = shlex.split(command)
    assert argv[:3] == ["uv", "run", "okg"]
    return _build_parser().parse_args(argv[3:])


def test_mismatch_refusal_names_supersede_for_its_deployment():
    """A different source selection is refused, and the refusal says how to
    get out when a recorded source can never pass."""
    error = _mismatch_refusal(
        [{"source": "code-structure"}], deployment="kdflex",
    )
    data = error.data
    assert data["reason"] == "incomplete_recovery_contract_mismatch"
    # The refusal decision and its evidence are unchanged.
    assert data["expected_source_contract"] == (
        _blocked_state()["latest_blocked_receipt"]["recovery"][
            "source_contract"
        ]
    )
    assert data["actual_source_contract"] == [{"source": "code-structure"}]

    # Plain words: what the checkpoint requires, and what this run asked for.
    assert data["expected_sources"] == ["git-files", "agent-sessions"]
    assert data["actual_sources"] == ["code-structure"]
    assert data["changed_sources"] == [
        "agent-sessions", "code-structure", "git-files",
    ]
    message = str(error)
    assert CHECKPOINT in message
    assert "repeat exactly the sources git-files, agent-sessions" in message
    assert "this run selected code-structure" in message

    # The escape hatch is the real CLI verb, for THIS deployment, as a DRY
    # RUN: its reason is a placeholder, and an applied supersede cannot be
    # undone, so pasting it as-is must change nothing.
    command = data["escape_hatch_command"]
    assert "--apply" not in shlex.split(command)
    args = _parsed_escape_hatch(command)
    assert args.command == "runtime"
    assert args.rt_subcmd == "supersede-checkpoint"
    assert args.deployment == "kdflex"
    assert args.apply is False
    assert command in error.fix_hint

    # The hint says to read the dry run, then re-run with --apply and a real
    # reason.
    hint = error.fix_hint
    assert "First run the dry run" in hint
    assert "run the same command again with --apply" in hint
    assert "--reason replaced by what was actually done" in hint
    assert "cannot be undone" in hint

    # And it says what superseding does and does not do, including what
    # becomes of the stranded run's finished work (proved end to end by
    # test_the_next_publish_after_a_supersede_shows_the_stranded_writes).
    assert "deletes nothing" in hint
    assert "does not count the unfinished sources as ingested" in hint
    assert "stays blocked and is never published" in hint
    assert "already wrote becomes readable with the next publish" in hint
    assert "admission records stay marked blocked" in hint
    assert error.to_json()["error"]["data"]["escape_hatch_command"] == command


def test_the_publish_recovery_plan_still_offers_the_applying_command():
    """One helper builds both forms; the recovery plan's form is unchanged."""
    from okg.substrate.publish_recovery import _supersede_command

    applying = _supersede_command("kdflex")
    assert applying == _supersede_command("kdflex", apply=True)
    assert _parsed_escape_hatch(applying).apply is True
    dry_run = _supersede_command("kdflex", apply=False)
    assert dry_run == applying.replace("--apply ", "")


def test_mismatch_refusal_on_same_sources_names_the_changed_binding():
    """Same sources, different run mode: the message names the source."""
    error = _mismatch_refusal([
        {"source": "git-files", "run_mode": "scope_complete"},
        {"source": "agent-sessions", "run_mode": "incremental"},
    ])
    assert error.data["changed_sources"] == ["agent-sessions"]
    assert "same sources" in str(error)
    assert "recorded for agent-sessions is different" in str(error)
    args = _parsed_escape_hatch(error.data["escape_hatch_command"])
    assert args.deployment == DEPLOYMENT
    assert args.apply is False


# ---------------------------------------------------------------------------
# The CLI verb
# ---------------------------------------------------------------------------


def test_cli_runtime_supersede_checkpoint_dispatches(monkeypatch, capsys):
    # The CLI resolves the deployment by name, so it must be one that ships;
    # kubernetes-smoke does (okg-workspace is this repository's own).
    deployment = "kubernetes-smoke"
    parser = _build_parser()
    args = parser.parse_args([
        "runtime", "supersede-checkpoint",
        "--deployment", deployment,
        "--reason", "agent-sessions cannot pass coverage",
        "--apply",
        "--json",
    ])
    assert args.rt_subcmd == "supersede-checkpoint"
    assert args.apply is True
    assert args.actor == "operator"

    seen: dict = {}

    def _fake_supersede(**kwargs):
        seen.update(kwargs)
        return {
            "deployment": deployment,
            "checkpoint_id": CHECKPOINT,
            "prior_state": ir.REPAIR_REQUIRED,
            "required_sources": ["agent-sessions", "git-files"],
            "reason": kwargs["reason"],
            "actor": kwargs["actor"],
            "applied": True,
        }

    monkeypatch.setattr(
        ir, "supersede_incomplete_checkpoint", _fake_supersede,
    )
    monkeypatch.setattr(
        cli_runtime,
        "resolve_deployment",
        lambda *a, **k: argparse.Namespace(
            runtime=argparse.Namespace(enabled=True),
        ),
        raising=False,
    )
    args.dsn = DSN
    rc = cli_runtime._cmd_runtime(args, as_json=True)
    assert rc == 0
    assert seen["deployment"] == deployment
    assert seen["apply"] is True
    payload = json.loads(capsys.readouterr().out)
    assert payload["applied"] is True
    assert payload["checkpoint_id"] == CHECKPOINT


def test_cli_requires_a_reason():
    parser = _build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args([
            "runtime", "supersede-checkpoint", "--deployment", DEPLOYMENT,
        ])


def test_cli_defaults_to_a_dry_run():
    parser = _build_parser()
    args = parser.parse_args([
        "runtime", "supersede-checkpoint",
        "--deployment", DEPLOYMENT, "--reason", "x",
    ])
    assert args.apply is False


# ---------------------------------------------------------------------------
# The write, against a real database
# ---------------------------------------------------------------------------


_PG_METADATA = {
    "kind": "generation",
    "deployment_name": DEPLOYMENT,
    "branch": "default",
    "status": "blocked",
    "catalog_version_id": 7,
    "created_at": "2026-09-04T04:22:46.366836+00:00",
    "blocked_reason": {
        "reason": ir.BLOCKED_INGEST_REASON,
        "status": "incomplete",
        "completeness": {"failed_sources": ["agent-sessions"]},
        "recovery": {
            "schema": ir.RECOVERY_SCHEMA,
            "state": ir.REPAIR_REQUIRED,
            "predecessor_checkpoint_id": "20260904T012400000000Z:aaaa",
            "source_contract": [
                {"source": "git-files", "run_mode": "scope_complete"},
                {"source": "agent-sessions", "run_mode": "scope_complete"},
            ],
            "required_sources": ["agent-sessions", "git-files"],
            "admission_ids": [],
        },
    },
}


def _seed_blocked_checkpoint(dsn, monkeypatch):
    """Insert one blocked checkpoint row and point the reader at it."""
    with pool_module.pooled_connection(
        dsn, purpose=pool_module.GENERAL,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM public._chronos_branch_interval_checkpoints "
                " WHERE checkpoint_id = %s",
                (CHECKPOINT,),
            )
            cur.execute(
                "INSERT INTO public._chronos_branch_interval_checkpoints "
                "  (checkpoint_id, branch_id, segment_id, created_at, "
                "   metadata) VALUES (%s, %s, %s, %s, %s)",
                (
                    CHECKPOINT,
                    "default",
                    1,
                    "2026-09-04T04:22:46.366836+00:00",
                    json.dumps(_PG_METADATA),
                ),
            )

    def _state(**_kwargs):
        return {
            "staged_work_present": True,
            "latest_checkpoint_id": CHECKPOINT,
            "latest_checkpoint_status": "blocked",
            "latest_blocked_reason": ir.BLOCKED_INGEST_REASON,
            "latest_blocked_receipt": _PG_METADATA["blocked_reason"],
            "latest_published_checkpoint_id": None,
            "branch_mutation_token": "token",
        }

    monkeypatch.setattr(ir, "staged_source_redrive_state", _state)


def _read_metadata(dsn):
    with pool_module.pooled_connection(
        dsn, purpose=pool_module.GENERAL,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT metadata FROM "
                "public._chronos_branch_interval_checkpoints "
                " WHERE checkpoint_id = %s",
                (CHECKPOINT,),
            )
            return json.loads(cur.fetchone()[0])


@pytest.mark.postgres
def test_the_write_produces_the_intended_document(clean_runtime, monkeypatch):
    """The SQL is executed by Postgres, not asserted as a string."""
    _seed_blocked_checkpoint(clean_runtime, monkeypatch)

    report = ir.supersede_incomplete_checkpoint(
        dsn=clean_runtime,
        deployment=DEPLOYMENT,
        reason="agent-sessions cannot pass coverage; see #1599",
        actor="riley",
        apply=True,
    )
    assert report["applied"] is True

    after = _read_metadata(clean_runtime)
    recovery = after["blocked_reason"]["recovery"]
    assert recovery["state"] == ir.SUPERSEDED
    assert recovery["superseded_state"] == ir.REPAIR_REQUIRED
    assert recovery["superseded_by"] == "riley"
    assert recovery["supersede_reason"].startswith("agent-sessions cannot")
    assert recovery["superseded_at"] == report["superseded_at"]

    # Everything else survives byte-for-byte. The merge must not drop the
    # contract, the predecessor, or anything outside `recovery`.
    before = _PG_METADATA
    for key in ("schema", "predecessor_checkpoint_id", "source_contract",
                "required_sources", "admission_ids"):
        assert recovery[key] == before["blocked_reason"]["recovery"][key]
    for key in ("reason", "status", "completeness"):
        assert after["blocked_reason"][key] == before["blocked_reason"][key]
    for key in ("kind", "deployment_name", "branch", "status",
                "catalog_version_id", "created_at"):
        assert after[key] == before[key], key
    # The checkpoint is still blocked. Nothing here claims it published.
    assert after["status"] == "blocked"


@pytest.mark.postgres
def test_a_second_supersede_refuses_against_the_real_row(
    clean_runtime, monkeypatch,
):
    """The compare-and-swap sees the row it already changed."""
    _seed_blocked_checkpoint(clean_runtime, monkeypatch)
    ir.supersede_incomplete_checkpoint(
        dsn=clean_runtime, deployment=DEPLOYMENT, reason="first", apply=True,
    )
    # The stubbed reader still reports the pre-write recovery object, so this
    # is exactly the concurrent-change shape: a caller acting on a state that
    # has since moved.
    with pytest.raises(SubstrateError) as exc:
        ir.supersede_incomplete_checkpoint(
            dsn=clean_runtime, deployment=DEPLOYMENT, reason="second",
            apply=True,
        )
    assert exc.value.data["reason"] == (
        "incomplete_recovery_supersede_state_drift"
    )


@pytest.mark.postgres
def test_the_next_publish_after_a_supersede_shows_the_stranded_writes(
    tmp_path, clean_runtime, monkeypatch,
):
    """Real ingest, real refusal, real supersede, real publish.

    `first` finishes and writes `svc:partial`; `second` fails, so the run
    leaves a blocked checkpoint that demands both again. A run of `first`
    alone is refused, and the refusal's command -- a dry run -- changes
    nothing. After the applied supersede, the next run of `first` alone
    publishes, and `svc:partial` becomes readable in that publish even
    though that run wrote nothing new: it is the stranded run's own write.
    The stranded run's admission record stays `blocked` on the superseded
    checkpoint, and it is not counted as staged work to re-drive.
    """
    # A blocked repair_required checkpoint is what pre-#3342 code cut.
    freeze_partial_failures(monkeypatch)
    from dataclasses import replace

    import psycopg
    import yaml

    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.ingest.service import IngestOptions, run_ingest
    from tests.pytest.catalog_fixture_builders import seed_catalog
    from tests.substrate.test_ingest_service import (
        _claim_layered_fixture_catalog,
        _published_node_ids,
        _write_layered_fixture_deployment,
    )

    def latest_published():
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            return ChronosGraphStore.from_connection(
                conn, ensure_schema=False,
            ).latest_generation(deployment_name="svc", status="published")

    def envelopes():
        with psycopg.connect(clean_runtime, autocommit=True) as conn, \
                conn.cursor() as cur:
            cur.execute(
                "SELECT e.admission_id::text, e.source, e.status, "
                "       e.target_generation_ref, "
                "       (SELECT count(*) FROM okg.admission_record_refs r "
                "         WHERE r.admission_id = e.admission_id) "
                "  FROM okg.admission_envelopes e "
                " WHERE e.deployment_name = 'svc' "
                " ORDER BY e.created_at, e.admission_id",
            )
            return {row[0]: row[1:] for row in cur.fetchall()}

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    dep = _write_layered_fixture_deployment(tmp_path)
    _claim_layered_fixture_catalog(clean_runtime, dep)
    options = IngestOptions(
        dsn=clean_runtime, deployment="svc", deployment_dir=dep,
    )
    (baseline,) = run_ingest(options).report["publishes"]
    assert baseline["status"] == "published"

    # The stranded run: `first` finishes with one new node, `second` fails.
    first_fixture = dep / "first.json"
    payload = json.loads(first_fixture.read_text())
    payload["nodes"].append({
        "node_id": "svc:partial",
        "subtype": "document",
        "attrs": {"text": "written by the stranded run"},
        "source_record_id": {"path": "first.json", "record": "partial"},
    })
    first_fixture.write_text(json.dumps(payload))
    registry_path = dep / "source_registry.yaml"
    registry = yaml.safe_load(registry_path.read_text())
    registry["sources"]["second"]["params"]["fixture_path"] = str(
        dep / "missing-second.json",
    )
    registry_path.write_text(yaml.safe_dump(registry))
    _claim_layered_fixture_catalog(clean_runtime, dep)

    (blocked,) = run_ingest(
        replace(options, reset_cursor=True),
    ).report["publishes"]
    assert blocked["status"] == "blocked"
    recovery = blocked["blocked_reason"]["recovery"]
    assert recovery["state"] == ir.REPAIR_REQUIRED
    (stranded_admission,) = recovery["admission_ids"]
    assert envelopes()[stranded_admission][:3] == (
        "svc.first", "blocked", blocked["generation_id"],
    )
    assert latest_published().checkpoint_id == baseline["generation_id"]
    assert "svc:partial" not in _published_node_ids(
        clean_runtime, baseline["generation_id"],
    )

    # The wedge, and the way out it names.
    first_only = replace(options, include={"first"}, include_predecessors=False)
    with pytest.raises(SubstrateError) as wedge:
        run_ingest(first_only)
    assert wedge.value.data["reason"] == (
        "incomplete_recovery_contract_mismatch"
    )
    args = _parsed_escape_hatch(wedge.value.data["escape_hatch_command"])
    assert (args.deployment, args.apply) == ("svc", False)

    # Pasted as-is, the command is a dry run: it names the contract it would
    # retire and the applying command, and the row does not move.
    dry_run = ir.supersede_incomplete_checkpoint(
        dsn=clean_runtime, deployment=args.deployment, reason=args.reason,
        actor=args.actor, checkpoint_id=args.checkpoint_id, apply=args.apply,
    )
    assert dry_run["applied"] is False
    assert dry_run["checkpoint_id"] == blocked["generation_id"]
    assert dry_run["required_sources"] == ["second"]
    assert "--apply" in dry_run["next_command"]
    with pytest.raises(SubstrateError) as still_wedged:
        run_ingest(first_only)
    assert still_wedged.value.data["reason"] == (
        "incomplete_recovery_contract_mismatch"
    )

    ir.supersede_incomplete_checkpoint(
        dsn=clean_runtime, deployment="svc",
        reason="second's fixture is gone for good", apply=True,
    )
    assert ir.staged_source_redrive_state(
        dsn=clean_runtime, deployment="svc",
    )["staged_work_present"] is False

    after = run_ingest(first_only)
    assert after.any_failed is False
    (published,) = after.report["publishes"]
    assert published["status"] == "published"
    assert latest_published().checkpoint_id == published["generation_id"]
    assert "svc:partial" in _published_node_ids(
        clean_runtime, published["generation_id"],
    )
    rows = envelopes()
    # This run's own admission wrote nothing new, so `svc:partial` above is
    # the stranded run's write ...
    (new_admission,) = [
        admission for admission in rows
        if admission != stranded_admission
        and rows[admission][0] == "svc.first"
        and rows[admission][2] is None
    ]
    assert rows[new_admission][3] == 0
    # ... and the stranded run's record still says blocked, on the
    # superseded checkpoint.
    assert rows[stranded_admission][:3] == (
        "svc.first", "blocked", blocked["generation_id"],
    )


# --- Incomplete-recovery freeze and its own derived admissions -------------
# An incomplete-recovery freeze tolerates only its own publish cycle's output.
#
# A run whose source fails ends in a *freeze*: the final publish writes a
# blocked checkpoint that records the exact set of staged admissions it
# carries. That set is read before the publish takes the publisher and branch
# locks. The publish then runs enrichers, and the publisher admits their output
# inside the same locked cycle, before admission feedback re-reads the set.
#
# Before the fix the feedback re-read counted those derived admissions as a
# change and refused every freeze whose enrichers wrote new facts (a fresh
# okg-workspace provision: 20 recorded admissions, 30 re-read). Now the
# publisher re-reads the recorded set under its locks, before the cycle, and
# refuses there with nothing checkpointed; feedback then tolerates only
# non-source admissions absent from that locked set.


class _DerivedNodeEnricher:
    """Emits one new derived node every cycle, like a real enricher."""

    name = "recovery_set_derived"
    publisher_skip_when_clean = False
    reads_subtypes = ("document",)
    reads_edge_types = ()
    emits_edge_types = ()
    requires_narrowings = ()
    write_envelope = {
        "nodes": [{"subtype": "document"}],
        "nondeterministic": False,
    }
    declares_node_provenance = True

    def __init__(self) -> None:
        self._counter = itertools.count()
        self.during_enrich: Callable[[], None] | None = None

    def enrich(self, conn, *, generation_id, incremental=None):
        from okg.substrate.enrichers.base import EnrichResult
        from okg.substrate.library.enrichers._fact_emitters import (
            emit_node_fact,
        )

        del incremental
        if self.during_enrich is not None:
            self.during_enrich()
        index = next(self._counter)
        outcome = emit_node_fact(
            conn,
            enricher_name=self.name,
            node_id=f"svc:derived-{index}",
            subtype="document",
            attrs={"text": f"derived output {index}"},
            dedupe_key=f"recovery-set-derived:{index}",
            source_record_id={"fixture": f"derived-{index}"},
            source_revision={"generation_id": str(generation_id)},
            run_id=str(uuid.uuid4()),
            observed_at=datetime.now(timezone.utc),
            derived_from=["svc:first"],
            support_policy="all_members",
        )
        assert outcome.emitted, outcome
        return EnrichResult(self.name, n_edges_emitted=0)


def _with_derived_enricher(
    monkeypatch: pytest.MonkeyPatch,
) -> _DerivedNodeEnricher:
    from okg.substrate.publisher import deployment_stack

    enricher = _DerivedNodeEnricher()
    original = deployment_stack.build_deployment_publisher_stack

    def build_with_enricher(**kwargs):
        stack = original(**kwargs)
        return replace(stack, enrichers=[*stack.enrichers, enricher])

    monkeypatch.setattr(
        deployment_stack,
        "build_deployment_publisher_stack",
        build_with_enricher,
    )
    return enricher


def _envelope_rows(dsn: str) -> dict[str, tuple[str, str, str | None]]:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT admission_id::text, producer_kind, status, "
            "target_generation_ref FROM okg.admission_envelopes "
            "WHERE deployment_name = 'svc'"
        )
        return {
            str(row[0]): (str(row[1]), str(row[2]), row[3])
            for row in cur.fetchall()
        }


def _checkpoint_ids(dsn: str) -> list[str]:
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        return [
            generation.checkpoint_id
            for generation in store.list_generations(
                deployment_name="svc",
                status=None,
            )
        ]


def _published_baseline_then_incomplete_setup(
    tmp_path: Path,
    dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[IngestOptions, _DerivedNodeEnricher]:
    """Publish once, then stage new source work and break the second source.

    The partial run then freezes the pre-#3342 way (see
    ``tests/pytest/pre_3342_freeze.py``).
    """
    freeze_partial_failures(monkeypatch)
    from tests.substrate.test_ingest_service import (
        _claim_layered_fixture_catalog,
        _write_layered_fixture_deployment,
    )

    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    dep = _write_layered_fixture_deployment(tmp_path)
    _claim_layered_fixture_catalog(dsn, dep)
    enricher = _with_derived_enricher(monkeypatch)
    options = IngestOptions(dsn=dsn, deployment="svc", deployment_dir=dep)
    baseline = run_ingest(options)
    assert [item["status"] for item in baseline.report["publishes"]] == [
        "published"
    ], baseline.report["publishes"]

    first_fixture = dep / "first.json"
    payload = json.loads(first_fixture.read_text())
    payload["nodes"].append({
        "node_id": "svc:partial",
        "subtype": "document",
        "attrs": {"text": "staged by the incomplete run"},
        "source_record_id": {"path": "first.json", "record": "partial"},
    })
    first_fixture.write_text(json.dumps(payload))
    registry_path = dep / "source_registry.yaml"
    registry = yaml.safe_load(registry_path.read_text())
    registry["sources"]["second"]["params"]["fixture_path"] = str(
        dep / "missing-second.json",
    )
    registry_path.write_text(yaml.safe_dump(registry))
    _claim_layered_fixture_catalog(dsn, dep)
    return replace(options, reset_cursor=True), enricher


@pytest.mark.postgres
def test_incomplete_freeze_accepts_its_own_derived_admissions(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    options, _enricher = _published_baseline_then_incomplete_setup(
        tmp_path, clean_runtime, monkeypatch,
    )
    before = _envelope_rows(clean_runtime)

    partial = run_ingest(options)

    assert partial.any_failed is True
    (frozen,) = partial.report["publishes"]
    assert frozen["status"] == "blocked", frozen
    recovery = frozen["blocked_reason"]["recovery"]
    assert recovery["state"] == "repair_required"
    recorded = set(recovery["admission_ids"])
    # The publisher proved the recorded set under its locks.
    assert recovery["locked_admission_ids"] == sorted(recorded)
    after = _envelope_rows(clean_runtime)
    new_derived = {
        admission_id
        for admission_id in set(after) - set(before)
        if after[admission_id][0] != "source"
    }
    # The cycle really admitted derived output; this is the case under test.
    assert new_derived, after
    assert recorded
    assert not recorded & new_derived
    assert all(after[admission_id][0] == "source" for admission_id in recorded)
    bound = {
        admission_id
        for admission_id, (_kind, status, ref) in after.items()
        if status == "blocked" and ref == frozen["generation_id"]
    }
    assert bound == recorded
    # The cycle's own derived admissions stay unbound; a later publish that
    # contains their facts marks them.
    for admission_id in new_derived:
        assert after[admission_id][1] in {"accepted", "pending"}
        assert after[admission_id][2] is None
    # ... and the run report names each one it tolerated.
    tolerated = frozen["admission_feedback"]["tolerated_unbound_admissions"]
    assert {item["admission_id"] for item in tolerated} == new_derived
    assert {item["producer_kind"] for item in tolerated} == {"enricher"}


def _feedback_fixture(dsn: str, *, stamp: str) -> dict:
    """One recorded source admission, one extra non-source admission, and a
    blocked recovery checkpoint whose receipt carries no lock stamp
    (``stamp="none"``), a stamp that differs from the recorded set, or one
    that matches it."""
    from okg.substrate.admission import attach_payload_ref, ensure_envelope
    from okg.substrate.graph_store import ChronosGraphStore

    deployment = "test_freeze_feedback_stamp"
    ids = {}
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        for key, kind in (("source", "source"), ("extra", "enricher")):
            run_id = str(uuid.uuid4())
            envelope = ensure_envelope(
                conn,
                deployment_name=deployment,
                producer_id=f"{key}_producer",
                producer_kind=kind,
                intent="source_sync",
                admission_mode="fast_track",
                status="accepted",
                idempotency_key=f"{key}-key",
                source=f"{key}_producer",
                run_id=run_id,
            )
            attach_payload_ref(
                conn,
                admission_id=envelope.admission_id,
                ref_key=f"{key}-run",
                payload={
                    "kind": "chronos_graph_run",
                    "source": f"{key}_producer",
                    "run_id": run_id,
                },
            )
            ids[key] = str(envelope.admission_id)
        recovery = {
            "schema": ir.RECOVERY_SCHEMA,
            "state": ir.REPAIR_REQUIRED,
            "predecessor_checkpoint_id": "none",
            "admission_ids": [ids["source"]],
        }
        if stamp == "differs":
            recovery["locked_admission_ids"] = [str(uuid.uuid4())]
        elif stamp == "matches":
            recovery["locked_admission_ids"] = [ids["source"]]
        else:
            assert stamp == "none"
        ids["checkpoint"] = store.checkpoint_generation(
            deployment_name=deployment,
            status="blocked",
            metadata={
                "blocked_reason": {
                    "reason": ir.BLOCKED_INGEST_REASON,
                    "recovery": recovery,
                },
            },
        )
    ids["deployment"] = deployment
    return ids


def _apply_freeze_feedback(dsn: str, ids: dict) -> dict:
    from okg.substrate.admission import apply_generation_feedback

    with psycopg.connect(dsn, autocommit=True) as conn, conn.transaction():
        return apply_generation_feedback(
            conn,
            deployment_name=ids["deployment"],
            generation_id=ids["checkpoint"],
            expected_admission_ids=[ids["source"]],
            recovery_from_generation_id="none",
        )


@pytest.mark.postgres
@pytest.mark.parametrize("stamp", ["none", "differs"])
def test_freeze_feedback_without_a_matching_lock_stamp_tolerates_nothing(
    clean_runtime: str,
    stamp: str,
) -> None:
    """Only a receipt stamped with exactly the expected set earns tolerance."""
    ids = _feedback_fixture(clean_runtime, stamp=stamp)

    with pytest.raises(SubstrateError) as refused:
        _apply_freeze_feedback(clean_runtime, ids)

    assert refused.value.data["reason"] == "recovery_admission_set_changed"
    assert refused.value.data["phase"] == "freeze"
    assert ids["extra"] in refused.value.data["actual_admission_ids"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM okg.admission_envelopes "
            "WHERE admission_id = %s::uuid",
            (ids["source"],),
        )
        assert cur.fetchone()[0] == "accepted"


@pytest.mark.postgres
def test_freeze_feedback_with_matching_lock_stamp_reports_tolerated(
    clean_runtime: str,
) -> None:
    ids = _feedback_fixture(clean_runtime, stamp="matches")

    feedback = _apply_freeze_feedback(clean_runtime, ids)

    assert feedback["verified_admission_ids"] == [ids["source"]]
    assert feedback["tolerated_unbound_admissions"] == [
        {"admission_id": ids["extra"], "producer_kind": "enricher"}
    ]


@pytest.mark.postgres
def test_source_admission_between_snapshot_and_lock_refuses_and_commits_nothing(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The locked re-read refuses before any checkpoint or admission write."""
    import okg.substrate.ingest.service as ingest_service
    from tests.substrate.test_ingest_service import (
        _insert_staged_source_admission,
    )

    options, _enricher = _published_baseline_then_incomplete_setup(
        tmp_path, clean_runtime, monkeypatch,
    )
    original_snapshot = ingest_service._recovery_candidate_admission_ids
    late: list[str] = []
    checkpoints_before: list[list[str]] = []
    envelopes_before: list[dict[str, tuple[str, str, str | None]]] = []

    def snapshot_then_late_source_admission(options, **kwargs):
        snapshot = original_snapshot(options, **kwargs)
        # The snapshot is taken after every source ran; record the durable
        # state here, then land one more source admission before the locks.
        checkpoints_before.append(_checkpoint_ids(clean_runtime))
        late.append(
            _insert_staged_source_admission(
                clean_runtime,
                deployment="svc",
                source="svc.first",
            )
        )
        envelopes_before.append(_envelope_rows(clean_runtime))
        return snapshot

    monkeypatch.setattr(
        ingest_service,
        "_recovery_candidate_admission_ids",
        snapshot_then_late_source_admission,
    )

    partial = run_ingest(options)

    assert late
    (refused,) = partial.report["publishes"]
    assert refused["status"] == "fail", refused
    data = refused["error_data"]
    assert data["reason"] == "recovery_admission_set_changed"
    assert data["phase"] == "lock"
    assert data["checkpoint_committed"] is False
    assert late[0] in data["actual_admission_ids"]
    assert late[0] not in data["expected_admission_ids"]
    assert "nothing was checkpointed" in refused["fix_hint"]
    # Atomic refusal: no checkpoint and no admission transition committed.
    assert _checkpoint_ids(clean_runtime) == checkpoints_before[0]
    assert _envelope_rows(clean_runtime) == envelopes_before[0]


@pytest.mark.postgres
def test_source_admission_inside_locked_cycle_still_refuses_at_feedback(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only non-source extras are tolerated; the refusal names the checkpoint.

    A real source cannot write while the branch is locked, so this plants the
    envelope directly from inside the cycle to exercise the feedback check.
    """
    from tests.substrate.test_ingest_service import (
        _insert_staged_source_admission,
    )

    options, enricher = _published_baseline_then_incomplete_setup(
        tmp_path, clean_runtime, monkeypatch,
    )
    planted: list[str] = []

    def plant_source_admission() -> None:
        if not planted:
            planted.append(
                _insert_staged_source_admission(
                    clean_runtime,
                    deployment="svc",
                    source="svc.first",
                )
            )

    enricher.during_enrich = plant_source_admission

    partial = run_ingest(options)

    assert planted
    (refused,) = partial.report["publishes"]
    assert refused["status"] == "fail", refused
    data = refused["error_data"]
    assert data["reason"] == "recovery_admission_set_changed"
    assert data["phase"] == "freeze"
    assert data["checkpoint_committed"] is True
    assert planted[0] in data["actual_admission_ids"]
    # Derived extras were tolerated: the only unexpected id is the source one.
    assert set(data["actual_admission_ids"]) - set(
        data["expected_admission_ids"]
    ) == {planted[0]}
    assert data["generation_id"] in _checkpoint_ids(clean_runtime)
    assert "WERE committed" in refused["fix_hint"]
    assert "okg runtime supersede-checkpoint" in refused["fix_hint"]


# ---------------------------------------------------------------------------
# #3342: a repair_required checkpoint refuses every other source's run before
# it starts and records nothing. `okg status` and `okg doctor --check sources`
# must say so.
# ---------------------------------------------------------------------------


_HOLD = {
    "checkpoint_id": CHECKPOINT,
    "blocked_at": "2026-10-05T07:12:12+00:00",
    "legacy_receipt": False,
    "required_selection": ["submit-guide-files", "submit-guide-docs"],
    "failed_sources": ["submit-guide-docs"],
    "sources": [],
    "not_admitted_since_block": ["submit-slurm-snapshot", "submit-storage-catalog"],
    "escape_hatch_command": "okg runtime supersede-checkpoint --deployment d",
}


def test_the_hold_line_names_the_selection_the_stalled_sources_and_the_way_out():
    line = ir.source_sync_hold_line(_HOLD)
    assert CHECKPOINT in line
    assert (
        "does not repeat exactly submit-guide-files, submit-guide-docs "
        "(same run mode and scope) is refused before it starts" in line
    )
    assert "submit-slurm-snapshot, submit-storage-catalog have not been admitted since" in line
    assert "Repair submit-guide-docs, or retire the requirement (dry run first): " in line
    assert "okg runtime supersede-checkpoint --deployment d" in line
    # no time known: no "since None"
    assert "None" not in ir.source_sync_hold_line({**_HOLD, "blocked_at": None})


def test_a_legacy_receipt_points_at_its_own_fix_not_supersede():
    """Supersede refuses a receipt without recovery authority, so the hold
    must not send the operator there."""
    line = ir.source_sync_hold_line({
        **_HOLD, "legacy_receipt": True, "required_selection": [],
        "failed_sources": [], "escape_hatch_command": ir.operator_fix_hint("d"),
    })
    assert "every publishing source-sync run is refused" in line
    assert "supersede-checkpoint" not in line
    assert "okg run --once --inline --force-enrichers --deployment d --apply" in line


def test_doctor_sources_fails_while_source_sync_is_held(monkeypatch):
    from okg.substrate.cli import doctor

    seen = {}

    def _hold(**kwargs):
        seen.update(kwargs)
        return dict(_HOLD)

    monkeypatch.setattr(ir, "source_sync_hold", _hold)
    entry = argparse.Namespace(ownership_id="d.slurm", legacy=False)
    report = {"ok": True, "errors": [], "runtime_available": True}
    doctor._add_source_sync_hold(
        report, dsn="postgresql://x/y", deployment="d", registry={"slurm": entry},
    )
    assert seen["sources"] == {"slurm": "d.slurm"}
    assert report["ok"] is False
    [error] = report["errors"]
    assert error["code"] == "source_sync_held"
    assert error["not_admitted_since_block"] == _HOLD["not_admitted_since_block"]

    # non-vacuity: nothing held, nothing reported
    monkeypatch.setattr(ir, "source_sync_hold", lambda **_kw: None)
    clean = {"ok": True, "errors": [], "runtime_available": True}
    doctor._add_source_sync_hold(
        clean, dsn="postgresql://x/y", deployment="d", registry={"slurm": entry},
    )
    assert clean == {"ok": True, "errors": [], "runtime_available": True}


def test_doctor_sources_does_not_fail_on_a_database_it_cannot_read(monkeypatch):
    """Review finding: before this change an unreachable or unmigrated
    database left the check ok (`runtime_available: false`); the hold check
    must not turn that into a failure."""
    from okg.substrate.cli import doctor

    monkeypatch.setattr(
        ir, "source_sync_hold", lambda **_kw: pytest.fail("must not be read"),
    )
    entry = argparse.Namespace(ownership_id="d.slurm", legacy=False)
    for dsn in (None, "postgresql://x/y"):
        report = {"ok": True, "errors": [], "runtime_available": False}
        doctor._add_source_sync_hold(
            report, dsn=dsn, deployment="d", registry={"slurm": entry},
        )
        assert report["ok"] is True and report["errors"] == []
        assert report["warnings"][0]["code"] == "source_sync_hold_unchecked"


@pytest.mark.postgres
def test_a_repair_required_checkpoint_reports_the_sources_it_holds(
    tmp_path, clean_runtime, monkeypatch,
):
    """Real ingest, real blocked checkpoint, real refusal: the refused run
    records nothing, and `source_sync_hold` is the only place that says the
    other runs stop. After a supersede nothing is held."""
    # Since #3342 only a pre-upgrade freeze (or an unfinished undo) holds.
    freeze_partial_failures(monkeypatch)
    from dataclasses import replace

    import psycopg
    import yaml

    from okg.substrate.ingest.service import IngestOptions, run_ingest
    from tests.pytest.catalog_fixture_builders import seed_catalog
    from tests.substrate.test_ingest_service import (
        _claim_layered_fixture_catalog,
        _write_layered_fixture_deployment,
    )

    # `third` and `fourth` stand for independent sources the hold starves.
    sources = {
        "first": "svc.first", "second": "svc.second",
        "third": "svc.third", "fourth": "svc.fourth",
    }

    def envelope_count():
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            return conn.execute(
                "SELECT count(*) FROM okg.admission_envelopes "
                " WHERE deployment_name = 'svc'"
            ).fetchone()[0]

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    dep = _write_layered_fixture_deployment(tmp_path)
    _claim_layered_fixture_catalog(clean_runtime, dep)
    options = IngestOptions(dsn=clean_runtime, deployment="svc", deployment_dir=dep)
    (baseline,) = run_ingest(options).report["publishes"]
    assert baseline["status"] == "published"
    assert ir.source_sync_hold(
        dsn=clean_runtime, deployment="svc", sources=sources,
    ) is None

    registry_path = dep / "source_registry.yaml"
    registry = yaml.safe_load(registry_path.read_text())
    registry["sources"]["second"]["params"]["fixture_path"] = str(
        dep / "missing-second.json",
    )
    registry_path.write_text(yaml.safe_dump(registry))
    _claim_layered_fixture_catalog(clean_runtime, dep)
    (blocked,) = run_ingest(replace(options, reset_cursor=True)).report["publishes"]
    assert blocked["status"] == "blocked"

    # The refused run starts nothing and records nothing.
    before = envelope_count()
    with pytest.raises(SubstrateError) as wedge:
        run_ingest(replace(options, include={"first"}, include_predecessors=False))
    assert wedge.value.data["reason"] == "incomplete_recovery_contract_mismatch"
    assert envelope_count() == before

    hold = ir.source_sync_hold(dsn=clean_runtime, deployment="svc", sources=sources)
    assert hold is not None
    assert hold["checkpoint_id"] == blocked["generation_id"]
    assert hold["required_selection"] == ["first", "second"]
    assert hold["failed_sources"] == ["second"]
    assert hold["blocked_at"] is not None
    by_source = {item["source"]: item for item in hold["sources"]}
    assert set(by_source) == set(sources)
    # `first` was admitted by the blocked run itself, before the checkpoint
    # was cut: not since. A run of `first` alone was just refused, so it is
    # held like the independent sources.
    assert by_source["first"]["admitted_since_block"] is False
    assert hold["not_admitted_since_block"] == ["first", "fourth", "second", "third"]
    assert "--apply" not in hold["escape_hatch_command"]

    # An admission after the block counts (a non-publishing run is not
    # checked against the contract; here a row stands in for one).
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO okg.admission_envelopes (deployment_name, producer_id, "
            " producer_kind, intent, admission_mode, status, idempotency_key, "
            " source) VALUES ('svc', 'source:svc.third', 'source', 'source_sync', "
            " 'fast_track', 'accepted', 'after-the-block', 'svc.third')"
        )
    hold = ir.source_sync_hold(dsn=clean_runtime, deployment="svc", sources=sources)
    assert {i["source"]: i["admitted_since_block"] for i in hold["sources"]}["third"]
    assert hold["not_admitted_since_block"] == ["first", "fourth", "second"]

    ir.supersede_incomplete_checkpoint(
        dsn=clean_runtime, deployment="svc", reason="second is gone", apply=True,
    )
    assert ir.source_sync_hold(
        dsn=clean_runtime, deployment="svc", sources=sources,
    ) is None


def test_okg_status_says_source_sync_is_held(monkeypatch, capsys):
    """The plain `okg status` prints the hold beside the blocked publish."""
    from okg.substrate.cli import main as cli_main

    snap = {
        "deployment_name": "d", "stage": "running", "errors": [], "sources": [],
        "publishing": {
            "latest_published_generation": "okg:old",
            "latest_attempt": {
                "checkpoint_id": CHECKPOINT, "status": "blocked", "blocked": True,
                "blocked_detail": "ingest_source_incomplete; failed: submit-guide-docs",
            },
        },
        "source_sync_hold": dict(_HOLD),
    }
    monkeypatch.setattr(cli_main, "_collect_status", lambda *a, **k: dict(snap))
    rc = cli_main.main(["status", "--dsn", "postgresql://x/y", "--deployment", "d"])
    err = capsys.readouterr().err
    assert rc == 3
    assert "error: source_sync_held: source sync is held by blocked checkpoint" in err
    assert "submit-slurm-snapshot, submit-storage-catalog have not been admitted" in err
    # non-vacuity: without a hold, only the blocked-publish line
    snap.pop("source_sync_hold")
    cli_main.main(["status", "--dsn", "postgresql://x/y", "--deployment", "d"])
    assert "source_sync_held" not in capsys.readouterr().err
