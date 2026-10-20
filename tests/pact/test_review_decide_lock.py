"""`okg pact decide` must hold the manifest write lock (wave 4,
strengthen-formal-models).

decide_change loads the manifest, performs a decision-ledger round-trip, and
then saves a status projection. Before the wave-4 fix the save reused the
pre-round-trip snapshot with no lock: a locked verb (evidence attach, task
add, gate) committing during the round-trip was silently clobbered — the
exact lost-update MutationSafety.tla's NoLostCompletedMutation declares
impossible. The fixed contract re-loads and mutates under
manifest_write_lock, so the concurrent write survives.
"""
from __future__ import annotations

from argparse import Namespace
from pathlib import Path
from threading import Event, Thread

import pytest
import yaml

import okg.substrate.pact.operations as pact_operations
import okg.substrate.pact.review as review
from okg.substrate.pact import cli as pact_cli
from okg.substrate.pact import decisions as ledger
from okg.substrate.errors import SubstrateError
from okg.substrate.pact.store import load_manifest, save_manifest

from tests.pact.conftest import ingest_passing_named_plan
from tests.pact.test_review import _seed_change

pytestmark = pytest.mark.postgres


def test_decide_does_not_clobber_concurrent_locked_mutation(
    tmp_path: Path,
    clean_ledger,  # noqa: F811 - fixture
    monkeypatch,
) -> None:
    _seed_change(tmp_path, "rvw-race")

    decide_reached_ledger = Event()
    release_decide = Event()
    errors: list[BaseException] = []
    mutation_errors: list[BaseException] = []
    mutation_done = Event()
    original_derive = review.derive_change_state

    def pausing_derive(manifest, history, **kwargs):
        # decide holds the manifest lock through derivation and ledger write.
        # A concurrent mutation must wait instead of changing the digest.
        decide_reached_ledger.set()
        assert release_decide.wait(timeout=5)
        return original_derive(manifest, history, **kwargs)

    monkeypatch.setattr(review, "derive_change_state", pausing_derive)

    def run_decide() -> None:
        try:
            review.decide_change(
                tmp_path,
                "rvw-race",
                verdict="revise",
                note="race probe",
                explicit_dsn=clean_ledger,
            )
        except Exception as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    decider = Thread(target=run_decide)
    decider.start()
    assert decide_reached_ledger.wait(timeout=5)

    def run_mutation() -> None:
        try:
            pact_operations.add_task(
                tmp_path,
                "rvw-race",
                task_id="task.concurrent",
                title="Landed after decide",
                verification="uv run pytest tests/pact/test_review_decide_lock.py -q",
                requirements=["req.rvw-race.r1"],
            )
        except Exception as exc:  # pragma: no cover - surfaced below
            mutation_errors.append(exc)
        finally:
            mutation_done.set()

    mutator = Thread(target=run_mutation)
    mutator.start()
    assert not mutation_done.wait(timeout=0.2), (
        "a locked manifest mutation must wait for decide's transaction"
    )

    release_decide.set()
    decider.join(timeout=10)
    mutator.join(timeout=10)
    assert not decider.is_alive()
    assert not mutator.is_alive()
    assert not errors, errors
    assert not mutation_errors, mutation_errors

    manifest = load_manifest(tmp_path, "rvw-race")
    # revise projects awaiting_review -> in_progress; the manifest status
    # must reflect exactly that projection.
    from okg.substrate.pact import fsm

    assert manifest.change.status == fsm.STATUS_PROJECTION["in_progress"], (
        "decide's status projection must have been applied"
    )
    task_ids = {task.id for task in manifest.tasks}
    assert "task.concurrent" in task_ids, (
        "the serialized mutation committed after decide must survive "
        "(lost-update guard: both writers share the manifest lock)"
    )


def test_shared_ledger_serializes_same_boundary_across_worktrees(
    tmp_path: Path,
    clean_ledger,  # noqa: F811 - fixture
) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    _seed_change(first, "rvw-cross-worktree")
    _seed_change(second, "rvw-cross-worktree")
    # The test fixture starts from a blank database; initialize the shared
    # schema once so this test isolates decision serialization, not concurrent
    # bootstrap DDL.
    warm = ledger.require_ledger_connection(explicit_dsn=clean_ledger)
    warm.close()
    start = Event()
    outputs: list[dict] = []
    errors: list[BaseException] = []

    def run(root: Path) -> None:
        assert start.wait(timeout=5)
        try:
            outputs.append(review.decide_change(
                root,
                "rvw-cross-worktree",
                verdict="approve",
                principal="riley",
                explicit_dsn=clean_ledger,
            ))
        except Exception as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    threads = [Thread(target=run, args=(root,)) for root in (first, second)]
    for thread in threads:
        thread.start()
    start.set()
    for thread in threads:
        thread.join(timeout=15)

    assert not errors, errors
    assert len(outputs) == 2
    assert sum(bool(row.get("recovered_projection")) for row in outputs) == 1
    conn = ledger.require_ledger_connection(explicit_dsn=clean_ledger)
    try:
        decisions = ledger.load_decisions(conn, "rvw-cross-worktree")
    finally:
        conn.close()
    assert len(decisions) == 1
    assert all(
        (root / "pact" / "archive" / "rvw-cross-worktree" / "pact.yaml").is_file()
        for root in (first, second)
    )


def test_retry_recovers_archive_after_decision_commit(
    tmp_path: Path,
    clean_ledger,  # noqa: F811 - fixture
    monkeypatch,
) -> None:
    _seed_change(tmp_path, "rvw-recover")
    original_rename = Path.rename
    failed = False

    def fail_once(path: Path, target: Path):
        nonlocal failed
        if path.name == "rvw-recover" and not failed:
            failed = True
            raise OSError("induced archive failure")
        return original_rename(path, target)

    monkeypatch.setattr(Path, "rename", fail_once)
    with pytest.raises(OSError, match="induced archive failure"):
        review.decide_change(
            tmp_path,
            "rvw-recover",
            verdict="approve",
            principal="riley",
            explicit_dsn=clean_ledger,
        )
    monkeypatch.setattr(Path, "rename", original_rename)

    recovered = review.decide_change(
        tmp_path,
        "rvw-recover",
        verdict="approve",
        principal="riley",
        explicit_dsn=clean_ledger,
    )

    assert recovered["recovered_projection"] is True
    assert (tmp_path / "pact" / "archive" / "rvw-recover" / "pact.yaml").is_file()
    conn = ledger.require_ledger_connection(explicit_dsn=clean_ledger)
    try:
        decisions = ledger.load_decisions(conn, "rvw-recover")
    finally:
        conn.close()
    assert len(decisions) == 1


def test_retry_repairs_archive_metadata_after_post_rename_failure(
    tmp_path: Path,
    clean_ledger,  # noqa: F811 - fixture
    monkeypatch,
) -> None:
    change_id = "rvw-recover-metadata"
    _seed_change(tmp_path, change_id)
    original_rewrite = review._rewrite_pact_location_paths
    failed = False

    def fail_once(*args, **kwargs):
        nonlocal failed
        if kwargs.get("tree") == "archive" and not failed:
            failed = True
            raise OSError("induced metadata rewrite failure")
        return original_rewrite(*args, **kwargs)

    monkeypatch.setattr(review, "_rewrite_pact_location_paths", fail_once)
    with pytest.raises(OSError, match="induced metadata rewrite failure"):
        review.decide_change(
            tmp_path,
            change_id,
            verdict="approve",
            principal="riley",
            explicit_dsn=clean_ledger,
        )
    assert (tmp_path / "pact" / "archive" / change_id / "pact.yaml").is_file()

    recovered = review.decide_change(
        tmp_path,
        change_id,
        verdict="approve",
        principal="riley",
        explicit_dsn=clean_ledger,
    )

    assert recovered["recovered_projection"] is True
    assert recovered["archived"] is True
    conn = ledger.require_ledger_connection(explicit_dsn=clean_ledger)
    try:
        decisions = ledger.load_decisions(conn, change_id)
    finally:
        conn.close()
    assert len(decisions) == 1


@pytest.mark.parametrize("verdict", ["reject", "supersede"])
def test_retry_recovers_every_terminal_archive_after_decision_commit(
    tmp_path: Path,
    clean_ledger,  # noqa: F811 - fixture
    monkeypatch,
    verdict: str,
) -> None:
    change_id = f"rvw-recover-{verdict}"
    _seed_change(tmp_path, change_id)
    original_rename = Path.rename
    failed = False

    def fail_once(path: Path, target: Path):
        nonlocal failed
        if path.name == change_id and not failed:
            failed = True
            raise OSError("induced archive failure")
        return original_rename(path, target)

    monkeypatch.setattr(Path, "rename", fail_once)
    with pytest.raises(OSError, match="induced archive failure"):
        review.decide_change(
            tmp_path,
            change_id,
            verdict=verdict,
            target="replacement-change" if verdict == "supersede" else None,
            principal="riley",
            explicit_dsn=clean_ledger,
        )
    monkeypatch.setattr(Path, "rename", original_rename)

    recovered = review.decide_change(
        tmp_path,
        change_id,
        verdict=verdict,
        target="replacement-change" if verdict == "supersede" else None,
        principal="riley",
        explicit_dsn=clean_ledger,
    )

    assert recovered["recovered_projection"] is True
    assert recovered["state"] == ("rejected" if verdict == "reject" else "superseded")
    assert (tmp_path / "pact" / "archive" / change_id / "pact.yaml").is_file()
    conn = ledger.require_ledger_connection(explicit_dsn=clean_ledger)
    try:
        decisions = ledger.load_decisions(conn, change_id)
    finally:
        conn.close()
    assert len(decisions) == 1


def test_direct_intent_edit_after_ledger_read_is_not_overwritten_or_archived(
    tmp_path: Path,
    clean_ledger,  # noqa: F811 - fixture
    monkeypatch,
) -> None:
    _seed_change(tmp_path, "rvw-direct-edit")
    manifest_path = tmp_path / "pact" / "changes" / "rvw-direct-edit" / "pact.yaml"
    original_record = ledger.record_decision

    def edit_then_record(conn, **kwargs):
        document = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
        document["requirements"][0]["statement"] = "MUST preserve this direct edit."
        manifest_path.write_text(
            yaml.safe_dump(document, sort_keys=False),
            encoding="utf-8",
        )
        return original_record(conn, **kwargs)

    monkeypatch.setattr(ledger, "record_decision", edit_then_record)
    result = review.decide_change(
        tmp_path,
        "rvw-direct-edit",
        verdict="approve",
        principal="riley",
        explicit_dsn=clean_ledger,
    )

    assert result["projection_skipped"] is True
    assert result["state"] == "awaiting_review"
    assert "pact gate post-edit" in result["next_action"]["command"]
    assert manifest_path.is_file()
    assert not (tmp_path / "pact" / "archive" / "rvw-direct-edit").exists()
    persisted = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    assert persisted["requirements"][0]["statement"] == (
        "MUST preserve this direct edit."
    )
    conn = ledger.require_ledger_connection(explicit_dsn=clean_ledger)
    try:
        history = ledger.load_decisions(conn, "rvw-direct-edit")
    finally:
        conn.close()
    assert review.derive_change_state(
        load_manifest(tmp_path, "rvw-direct-edit"),
        history,
        repo_root=tmp_path,
    ).state == "awaiting_review"


def test_direct_intent_edit_during_archive_is_restored_to_active_tree(
    tmp_path: Path,
    clean_ledger,  # noqa: F811 - fixture
    monkeypatch,
) -> None:
    change_id = "rvw-direct-archive-edit"
    _seed_change(tmp_path, change_id)
    manifest_path = tmp_path / "pact" / "changes" / change_id / "pact.yaml"
    original_rename = Path.rename
    edited = False

    def edit_at_archive(path: Path, target: Path):
        nonlocal edited
        if path.name == change_id and not edited:
            edited = True
            document = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
            document["requirements"][0]["statement"] = (
                "MUST preserve the edit that landed at archive time."
            )
            manifest_path.write_text(
                yaml.safe_dump(document, sort_keys=False),
                encoding="utf-8",
            )
        return original_rename(path, target)

    monkeypatch.setattr(Path, "rename", edit_at_archive)
    result = review.decide_change(
        tmp_path,
        change_id,
        verdict="approve",
        principal="riley",
        explicit_dsn=clean_ledger,
    )

    assert result["projection_skipped"] is True
    assert result["archived"] is False
    assert manifest_path.is_file()
    assert not (tmp_path / "pact" / "archive" / change_id).exists()
    persisted = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    assert persisted["requirements"][0]["statement"] == (
        "MUST preserve the edit that landed at archive time."
    )


def test_direct_intent_edit_at_path_rewrite_is_not_overwritten(
    tmp_path: Path,
    clean_ledger,  # noqa: F811 - fixture
    monkeypatch,
) -> None:
    change_id = "rvw-direct-rewrite-edit"
    _seed_change(tmp_path, change_id)
    archived_path = tmp_path / "pact" / "archive" / change_id / "pact.yaml"
    active_path = tmp_path / "pact" / "changes" / change_id / "pact.yaml"
    original_rewrite = review._rewrite_location_file_in_place
    edited = False

    def edit_at_rewrite(
        path: Path,
        source_prefix: str,
        target_prefix: str,
        *,
        yaml_top_level_key: str | None = None,
    ) -> bool:
        nonlocal edited
        if path == archived_path and not edited:
            edited = True
            document = yaml.safe_load(path.read_text(encoding="utf-8"))
            document["requirements"][0]["statement"] = (
                "MUST preserve the edit concurrent with metadata rewriting."
            )
            path.write_text(
                yaml.safe_dump(document, sort_keys=False),
                encoding="utf-8",
            )
        return original_rewrite(
            path,
            source_prefix,
            target_prefix,
            yaml_top_level_key=yaml_top_level_key,
        )

    monkeypatch.setattr(review, "_rewrite_location_file_in_place", edit_at_rewrite)
    result = review.decide_change(
        tmp_path,
        change_id,
        verdict="approve",
        principal="riley",
        explicit_dsn=clean_ledger,
    )

    assert result["projection_skipped"] is True
    assert result["archived"] is False
    assert active_path.is_file()
    assert not archived_path.exists()
    persisted = yaml.safe_load(active_path.read_text(encoding="utf-8"))
    assert persisted["requirements"][0]["statement"] == (
        "MUST preserve the edit concurrent with metadata rewriting."
    )


def test_reopen_reverses_archived_formal_evidence_and_report_paths(
    tmp_path: Path,
    clean_ledger,  # noqa: F811 - fixture
) -> None:
    change_id = "rvw-reopen-formal"
    _seed_change(tmp_path, change_id)
    active_dir = tmp_path / "pact" / "changes" / change_id
    manifest_path = active_dir / "pact.yaml"
    document = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    document["evidence"][0]["data"] = {
        "artifact_path": (
            f"pact/changes/{change_id}/reports/formal/safety/tlc.stdout.log"
        )
    }
    manifest_path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    report = active_dir / "reports" / "formal" / "formal-report.yaml"
    report.parent.mkdir(parents=True)
    report.write_text(
        f"artifact: pact/changes/{change_id}/reports/formal/safety/tlc.stdout.log\n",
        encoding="utf-8",
    )
    review.decide_change(
        tmp_path,
        change_id,
        verdict="approve",
        principal="riley",
        explicit_dsn=clean_ledger,
    )
    archived_dir = tmp_path / "pact" / "archive" / change_id
    assert f"pact/archive/{change_id}/" in (
        archived_dir / "reports" / "formal" / "formal-report.yaml"
    ).read_text(encoding="utf-8")

    reopened = review.decide_change(
        tmp_path,
        change_id,
        verdict="reopen",
        principal="riley",
        explicit_dsn=clean_ledger,
    )

    assert reopened["restored_from_archive"] is True
    restored = load_manifest(tmp_path, change_id)
    assert restored.evidence[0].data["artifact_path"].startswith(
        f"pact/changes/{change_id}/"
    )
    assert f"pact/changes/{change_id}/" in report.read_text(encoding="utf-8")


def test_reopen_retries_after_transient_location_rewrite_failure(
    tmp_path: Path,
    clean_ledger,  # noqa: F811 - fixture
    monkeypatch,
) -> None:
    change_id = "rvw-reopen-rewrite-retry"
    _seed_change(tmp_path, change_id)
    review.decide_change(
        tmp_path,
        change_id,
        verdict="approve",
        principal="riley",
        explicit_dsn=clean_ledger,
    )
    original_rewrite = review._rewrite_pact_location_paths
    failed = False

    def fail_first_restore(*args, **kwargs):
        nonlocal failed
        if kwargs.get("tree") == "changes" and not failed:
            failed = True
            return False
        return original_rewrite(*args, **kwargs)

    monkeypatch.setattr(review, "_rewrite_pact_location_paths", fail_first_restore)
    with pytest.raises(SubstrateError, match="could not be rewritten safely"):
        review.decide_change(
            tmp_path,
            change_id,
            verdict="reopen",
            principal="riley",
            explicit_dsn=clean_ledger,
        )
    assert (tmp_path / "pact" / "archive" / change_id / "pact.yaml").is_file()
    assert not (tmp_path / "pact" / "changes" / change_id).exists()

    retried = review.decide_change(
        tmp_path,
        change_id,
        verdict="reopen",
        principal="riley",
        explicit_dsn=clean_ledger,
    )
    assert retried["restored_from_archive"] is True
    assert (tmp_path / "pact" / "changes" / change_id / "pact.yaml").is_file()


def test_changed_archived_intent_retries_after_location_rewrite_failure(
    tmp_path: Path,
    clean_ledger,  # noqa: F811 - fixture
    monkeypatch,
) -> None:
    change_id = "rvw-changed-rewrite-retry"
    _seed_change(tmp_path, change_id, tier_label="tier-2")
    active = tmp_path / "pact" / "changes" / change_id / "pact.yaml"
    document = yaml.safe_load(active.read_text(encoding="utf-8"))
    document["pact_version"] = 4
    document["target_pact_version"] = 4
    document["change"]["tier"] = 2
    active.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    for _ in range(2):
        review.decide_change(
            tmp_path,
            change_id,
            verdict="approve",
            principal="riley",
            explicit_dsn=clean_ledger,
        )
    archived = tmp_path / "pact" / "archive" / change_id / "pact.yaml"
    document = yaml.safe_load(archived.read_text(encoding="utf-8"))
    document["requirements"][0]["statement"] = "MUST retry changed intent safely."
    archived.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    original_rewrite = review._rewrite_pact_location_paths
    failed = False

    def fail_first_restore(*args, **kwargs):
        nonlocal failed
        if kwargs.get("tree") == "changes" and not failed:
            failed = True
            return False
        return original_rewrite(*args, **kwargs)

    monkeypatch.setattr(review, "_rewrite_pact_location_paths", fail_first_restore)
    with pytest.raises(SubstrateError, match="could not be rewritten safely"):
        review.decide_change(
            tmp_path,
            change_id,
            verdict="approve",
            principal="riley",
            explicit_dsn=clean_ledger,
        )
    assert archived.is_file()
    assert not active.exists()

    retried = review.decide_change(
        tmp_path,
        change_id,
        verdict="approve",
        principal="riley",
        explicit_dsn=clean_ledger,
    )
    assert retried["restored_changed_intent"] is True
    restored = yaml.safe_load(active.read_text(encoding="utf-8"))
    assert restored["tasks"][0]["status"] == "todo"
    assert restored["tasks"][0]["evidence"] == []


def test_proposal_revision_names_required_intent_edit_wait_condition(
    tmp_path: Path,
    clean_ledger,  # noqa: F811 - fixture
    monkeypatch,
) -> None:
    change_id = "rvw-proposal-revise"
    _seed_change(tmp_path, change_id, tier_label="tier-2")
    manifest_path = tmp_path / "pact" / "changes" / change_id / "pact.yaml"
    document = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    document["pact_version"] = 4
    document["target_pact_version"] = 4
    document["change"]["tier"] = 2
    manifest_path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    monkeypatch.setenv("OKG_DSN", clean_ledger)

    revised = review.decide_change(
        tmp_path,
        change_id,
        verdict="revise",
        note="narrow the intent",
        principal="riley",
        explicit_dsn=clean_ledger,
    )
    assert revised["state"] == "draft"
    assert revised["blocked_by"] == ["intent_edit_required"]
    assert revised["next_action"]["wait_condition"] == (
        "intent_digest_changes_after_direct_pact_edit"
    )

    repeated = pact_operations.post_edit_check(tmp_path, change_id)
    assert repeated["state"] == "draft"
    assert "intent_edit_required" in repeated["blocked_by"]
    assert repeated["next_action"]["wait_condition"] == (
        "intent_digest_changes_after_direct_pact_edit"
    )


def test_v4_review_revision_reopens_task_work(
    tmp_path: Path,
    clean_ledger,  # noqa: F811 - fixture
) -> None:
    _seed_change(tmp_path, "rvw-v4-revise")
    manifest_path = tmp_path / "pact" / "changes" / "rvw-v4-revise" / "pact.yaml"
    document = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    document["pact_version"] = 4
    document["target_pact_version"] = 4
    document["change"]["tier"] = 1
    manifest_path.write_text(
        yaml.safe_dump(document, sort_keys=False),
        encoding="utf-8",
    )

    result = review.decide_change(
        tmp_path,
        "rvw-v4-revise",
        verdict="revise",
        principal="riley",
        explicit_dsn=clean_ledger,
    )

    assert result["state"] == "in_progress"
    manifest = load_manifest(tmp_path, "rvw-v4-revise")
    assert manifest.tasks[0].status == "todo"
    assert manifest.tasks[0].evidence == []
    conn = ledger.require_ledger_connection(explicit_dsn=clean_ledger)
    try:
        history = ledger.load_decisions(conn, "rvw-v4-revise")
    finally:
        conn.close()
    assert review.derive_change_state(
        manifest,
        history,
        repo_root=tmp_path,
    ).state == "in_progress"


def test_direct_intent_edit_at_task_proof_reset_is_preserved(
    tmp_path: Path,
    clean_ledger,  # noqa: F811 - fixture
    monkeypatch,
) -> None:
    change_id = "rvw-v4-revise-direct-edit"
    _seed_change(tmp_path, change_id)
    manifest_path = tmp_path / "pact" / "changes" / change_id / "pact.yaml"
    document = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    document["pact_version"] = 4
    document["target_pact_version"] = 4
    document["change"]["tier"] = 1
    manifest_path.write_text(
        yaml.safe_dump(document, sort_keys=False),
        encoding="utf-8",
    )
    original_reset = review._reset_done_task_proof_in_place
    edited = False

    def edit_before_reset(path: Path, expected_intent_digest: str) -> bool:
        nonlocal edited
        if not edited:
            edited = True
            current = yaml.safe_load(path.read_text(encoding="utf-8"))
            current["requirements"][0]["statement"] = (
                "MUST preserve the intent edit concurrent with proof reset."
            )
            path.write_text(
                yaml.safe_dump(current, sort_keys=False),
                encoding="utf-8",
            )
        return original_reset(path, expected_intent_digest)

    monkeypatch.setattr(review, "_reset_done_task_proof_in_place", edit_before_reset)
    result = review.decide_change(
        tmp_path,
        change_id,
        verdict="revise",
        principal="riley",
        explicit_dsn=clean_ledger,
    )

    assert result["projection_skipped"] is True
    persisted = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    assert persisted["requirements"][0]["statement"] == (
        "MUST preserve the intent edit concurrent with proof reset."
    )
    assert persisted["tasks"][0]["status"] == "done"


@pytest.mark.parametrize("quoted_status", ["'done'", '"done"'])
def test_v4_revision_resets_quoted_done_status(
    quoted_status: str,
    tmp_path: Path,
    clean_ledger,  # noqa: F811 - fixture
) -> None:
    change_id = f"rvw-v4-quoted-{quoted_status[0] == chr(39)}"
    _seed_change(tmp_path, change_id)
    manifest_path = tmp_path / "pact" / "changes" / change_id / "pact.yaml"
    document = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    document["pact_version"] = 4
    document["target_pact_version"] = 4
    document["change"]["tier"] = 1
    rendered = yaml.safe_dump(document, sort_keys=False)
    rendered = rendered.replace("  status: done\n", f"  status: {quoted_status}\n")
    manifest_path.write_text(rendered, encoding="utf-8")

    result = review.decide_change(
        tmp_path,
        change_id,
        verdict="revise",
        principal="riley",
        explicit_dsn=clean_ledger,
    )

    assert result["state"] == "in_progress"
    persisted = load_manifest(tmp_path, change_id)
    assert persisted.tasks[0].status == "todo"
    assert persisted.tasks[0].evidence == []


def test_v4_tier_two_real_decision_journey(
    tmp_path: Path,
    clean_ledger,  # noqa: F811 - fixture
    monkeypatch,
) -> None:
    change_id = "rvw-v4-journey"
    test_ref = (
        "tests/pact/test_review_decide_lock.py::"
        "test_v4_tier_two_real_decision_journey"
    )
    monkeypatch.setenv("OKG_DSN", clean_ledger)
    manifest = pact_operations.create_fast_path_change(
        tmp_path,
        change_id,
        title="V4 lifecycle journey",
        intent="The tier-two lifecycle must use one decision at each boundary.",
        acceptance="The real ledger journey reaches a retired decision gate.",
        test_id=test_ref,
    )
    manifest.change.tier = 2
    save_manifest(tmp_path, manifest)
    task_id = manifest.tasks[0].id

    post_edit = pact_operations.post_edit_check(tmp_path, change_id)
    assert post_edit["verdict"] == "recorded"
    assert post_edit["state"] == "proposed"
    assert "pact decide" in post_edit["next_action"]["command"]

    proposal = review.decide_change(
        tmp_path,
        change_id,
        verdict="approve",
        principal="riley",
        explicit_dsn=clean_ledger,
    )
    assert proposal["state"] == "in_progress"

    ingest_passing_named_plan(
        tmp_path,
        change_id,
        stem="v4-lifecycle-journey",
    )
    task_done = pact_cli._dispatch(Namespace(
        root=str(tmp_path),
        pact_subcmd="gate",
        gate_subcmd="task-done",
        change=change_id,
        task=task_id,
    ))
    assert task_done["state"] == "awaiting_review"
    assert "pact decide" in task_done["next_action"]["command"]

    final = review.decide_change(
        tmp_path,
        change_id,
        verdict="approve",
        principal="riley",
        explicit_dsn=clean_ledger,
    )
    assert final["state"] == "retired"
    assert final["archived"] is True
    fresh_graph = {
        "ok": True,
        "status": "fresh",
        "stale": False,
        "projected_generation_id": 1,
        "authority_scope": "default_graph",
    }
    monkeypatch.setattr(pact_cli, "_graph_projection_status", lambda *_: fresh_graph)
    monkeypatch.setattr(
        pact_cli,
        "_workflow_graph_authority_status",
        lambda *_: fresh_graph,
    )
    decision_gate = pact_cli._ledger_decision_gate(tmp_path, change_id)
    assert decision_gate["ok"] is True, decision_gate
    assert decision_gate["status"] == "approved"
