"""End-to-end: an empty source proves its authoritative replay (issue #2892).

`okg deployment add-source` updates the deployment's access policy, which
changes the Nomos policy fingerprint inside every existing source's pipeline
identity. The runner then asks each managed source for an authoritative full
replay. A source whose scope is empty -- a repository with no `openspec/`
directory, or Python files with no data-flow facts -- emitted nothing and
carried its prior record set forward instead of returning a complete one, so
the runner refused to acknowledge the run (`authoritative_replay_unproven`)
and every later ingest stayed blocked.

These tests run the real adapters through a registered, Chronos-writing
`SourceRunner`, then change only the Nomos policy fingerprint (what
add-source does) and run again. The replay must be proven, the change
identity acknowledged, and the replay latch cleared, so a third run
short-circuits on the unchanged probe.

Uses a migrated disposable test DB (``okg_migrated``).
"""
from __future__ import annotations

import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.ingest.full_retraction_guard import FullRetractionOverride
from okg.substrate.ingest.runner import SourceRunner
from okg.substrate.library.sources.code_graph_dataflow import CodeDataFlowSource
from okg.substrate.library.sources.openspec import OpenSpecSource


pytestmark = pytest.mark.postgres

_POLICY_BEFORE = "sha256:" + "1" * 64
_POLICY_AFTER_ADD_SOURCE = "sha256:" + "2" * 64


def _git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
    )


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    root.mkdir()
    _git(root, "init", "-q")
    _git(root, "config", "user.email", "t@example.com")
    _git(root, "config", "user.name", "Tester")
    # A Python file with no data-flow facts, and no openspec/ directory:
    # the fresh workspace-graph install from the issue.
    (root / "a.py").write_text("def hello():\n    return 1\n")
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", "init")
    return root


def _set_policy(monkeypatch: pytest.MonkeyPatch, fingerprint: str) -> None:
    """Stand in for the deployment's active Nomos policy bundle."""

    def _active(self: SourceRunner) -> str:
        snapshot = self._active_pipeline_snapshot
        if snapshot is not None:
            return snapshot.nomos_policy_fingerprint
        return fingerprint

    monkeypatch.setattr(SourceRunner, "_active_nomos_policy_fingerprint", _active)


def _runner(
    okg_migrated: str,
    registry_name: str,
    *,
    write_fact_log: bool = True,
) -> SourceRunner:
    source_name = f"w2892-{registry_name}-{uuid.uuid4().hex[:8]}"
    return SourceRunner(
        okg_migrated,
        source_name=source_name,
        deployment_name=None,
        executable_profile="discovery_crawl",
        registry_name=registry_name,
        ownership_id=source_name,
        registry_binding_fingerprint=f"{registry_name}-fixture-v1",
        deletion_semantics="missing_from_completed_scope",
        write_chronos_graph=True,
        write_fact_log=write_fact_log,
    )


def _replay_required(runner: SourceRunner) -> dict:
    import psycopg

    with psycopg.connect(runner.dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT replay_required FROM okg.source_watermarks "
            "WHERE source = %s",
            (runner.source_name,),
        )
        row = cur.fetchone()
    assert row is not None, "the runner recorded no watermark"
    return dict(row[0] or {})


def _allow_full_retraction(runner: SourceRunner) -> None:
    """What `okg ingest --allow-full-retraction <source>` gives one run."""
    runner.full_retraction_override = FullRetractionOverride(
        source=runner.source_name,
        requested_by="replay-test-operator",
        requested_at=datetime(2026, 10, 1, tzinfo=timezone.utc),
    )


def _refuses_full_retraction(runner: SourceRunner, adapter) -> None:
    """Issue #2818 (a): a source with published rows that now reads as
    empty is refused, and nothing is retracted, until the operator passes
    `--allow-full-retraction` for one run."""
    before = _retraction_rows(runner)
    with pytest.raises(SubstrateError) as refused:
        runner.run(adapter, mode="scope_complete")
    assert refused.value.code == "source_run_full_retraction_refused"
    assert refused.value.data["kind"] == "whole_source"
    assert refused.value.data["live_rows"] > 0
    assert _retraction_rows(runner) == before


class _Counting:
    """Count how often the adapter's reader runs."""

    run_count = 0

    def run(
        self,
        run_id,
        *,
        mode="cursor",
        cursor=None,
        sync_scope=None,
        source_has_published_facts=None,
    ):
        import inspect

        self.run_count += 1
        extra = {}
        if "source_has_published_facts" in inspect.signature(
            super().run  # type: ignore[misc]
        ).parameters:
            extra["source_has_published_facts"] = source_has_published_facts
        return super().run(  # type: ignore[misc]
            run_id,
            mode=mode,
            cursor=cursor,
            sync_scope=sync_scope,
            **extra,
        )


class _CountingOpenSpec(_Counting, OpenSpecSource):
    pass


class _CountingCodeDataFlow(_Counting, CodeDataFlowSource):
    pass


def _adapter(kind: str, repo: Path):
    if kind == "openspec":
        return _CountingOpenSpec(
            openspec_root=str(repo / "openspec"),
            repo="demo",
        )
    return _CountingCodeDataFlow(repo="demo", repo_root=str(repo))


@pytest.mark.parametrize("kind", ["openspec", "code_data_flow"])
def test_empty_source_publishes_after_policy_change(
    okg_migrated: str,
    repo: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    runner = _runner(okg_migrated, kind)
    adapter = _adapter(kind, repo)

    # Baseline: a plain ingest before add-source.
    _set_policy(monkeypatch, _POLICY_BEFORE)
    first = runner.run(adapter, mode="scope_complete")
    assert first.completed_scope is True
    assert first.nodes_appended == 0
    assert _replay_required(runner) == {}

    # add-source rewrites the access policy. The next plain ingest is an
    # authoritative replay; before the fix it raised
    # "source run cannot acknowledge change identity:
    # reason=authoritative_replay_unproven".
    _set_policy(monkeypatch, _POLICY_AFTER_ADD_SOURCE)
    replay = runner.run(adapter, mode="scope_complete")
    assert replay.completed_scope is True
    assert adapter.run_count == 2
    assert _replay_required(runner) == {}

    # The replay stamped the new identity: an unchanged input now
    # short-circuits on the probe instead of replaying again.
    runner.run(adapter, mode="scope_complete")
    assert adapter.run_count == 2


def test_code_data_flow_replay_retracts_facts_of_an_emptied_file(
    okg_migrated: str,
    repo: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A complete re-read replaces the prior record set, it does not merge.

    Before the fix, a changed file that no longer yields facts left the run
    with no record set, so the prior record for that file survived. The
    fixed adapter returns the complete (here empty) record set, and the
    runner retracts what the file used to contribute.
    """
    (repo / "a.py").write_text("import os\nDB = os.getenv('DB_URL')\n")
    _git(repo, "commit", "-qam", "config read")
    runner = _runner(okg_migrated, "code_data_flow")
    adapter = _adapter("code_data_flow", repo)

    _set_policy(monkeypatch, _POLICY_BEFORE)
    first = runner.run(adapter, mode="scope_complete")
    assert first.nodes_appended > 0

    (repo / "a.py").write_text("def hello():\n    return 1\n")
    _git(repo, "commit", "-qam", "drop config read")
    # The only file's facts are gone, so the whole source reads as empty.
    _refuses_full_retraction(runner, adapter)
    _allow_full_retraction(runner)
    second = runner.run(adapter, mode="scope_complete")
    assert second.completed_scope is True
    assert second.retractions_node + second.retractions_edge > 0


# --- Review of #2896: an unreadable scope is an error, never empty ----------


def _write_change(root: Path, slug: str) -> None:
    change = root / "openspec" / "changes" / slug
    change.mkdir(parents=True)
    (change / "proposal.md").write_text(
        f"# Change: {slug}\n\n## Why\nBecause.\n\n## What Changes\n- {slug}\n"
    )
    (change / "tasks.md").write_text("## 1. Impl\n- [ ] 1.1 do it\n")


def _retraction_rows(runner: SourceRunner) -> int:
    import psycopg

    with psycopg.connect(runner.dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT (SELECT count(*) FROM okg.node_facts "
            "         WHERE source = %s AND op = 'R') "
            "     + (SELECT count(*) FROM okg.edge_facts "
            "         WHERE source = %s AND op = 'R')",
            (runner.source_name, runner.source_name),
        )
        row = cur.fetchone()
    assert row is not None
    return int(row[0])


@pytest.fixture
def spec_repo(repo: Path) -> Path:
    _write_change(repo, "add-foo")
    _write_change(repo, "add-bar")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "specs")
    return repo


@pytest.mark.parametrize("how", ["unreadable", "missing"])
def test_openspec_replay_refuses_an_unavailable_tree(
    okg_migrated: str,
    spec_repo: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    how: str,
) -> None:
    """A tree that cannot be read, or vanished after publishing, retracts
    nothing: the replay refuses loudly instead of reporting an empty scope."""
    from okg.substrate.errors import SubstrateError

    runner = _runner(okg_migrated, "openspec")
    adapter = _adapter("openspec", spec_repo)
    _set_policy(monkeypatch, _POLICY_BEFORE)
    first = runner.run(adapter, mode="scope_complete")
    assert first.nodes_appended > 0

    tree = spec_repo / "openspec"
    if how == "unreadable":
        tree.chmod(0)
        expected = "cannot be read"
    else:
        tree.rename(tmp_path / "moved-away")
        expected = "refusing to retract"
    try:
        _set_policy(monkeypatch, _POLICY_AFTER_ADD_SOURCE)
        with pytest.raises(SubstrateError, match=expected):
            runner.run(adapter, mode="scope_complete")
    finally:
        if how == "unreadable":
            tree.chmod(0o755)
    assert _retraction_rows(runner) == 0


def test_openspec_optional_tree_removal_retracts(
    okg_migrated: str,
    spec_repo: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """`openspec_root_optional: true` makes a removed tree an empty scope,
    and says so with the configured path."""
    runner = _runner(okg_migrated, "openspec")
    adapter = OpenSpecSource(
        openspec_root=str(spec_repo / "openspec"),
        repo="demo",
        openspec_root_optional=True,
    )
    _set_policy(monkeypatch, _POLICY_BEFORE)
    runner.run(adapter, mode="scope_complete")
    (spec_repo / "openspec").rename(tmp_path / "moved-away")
    _set_policy(monkeypatch, _POLICY_AFTER_ADD_SOURCE)
    _refuses_full_retraction(runner, adapter)
    _allow_full_retraction(runner)
    caplog.set_level("WARNING", logger="okg.substrate.library.sources.openspec")
    replay = runner.run(adapter, mode="scope_complete")
    assert replay.completed_scope is True
    assert replay.retractions_node > 0
    warnings = [
        record.getMessage()
        for record in caplog.records
        if record.name == "okg.substrate.library.sources.openspec"
    ]
    assert any(
        str(spec_repo / "openspec") in message
        and "openspec_root_optional" in message
        for message in warnings
    ), warnings


def test_openspec_replay_replaces_a_non_empty_prior(
    okg_migrated: str,
    spec_repo: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The replay's complete list replaces the prior one; it does not merge.

    A change deleted while the policy changes must be retracted by the
    replay, which no longer has the file hashes that would name it.
    """
    runner = _runner(okg_migrated, "openspec")
    adapter = _adapter("openspec", spec_repo)
    _set_policy(monkeypatch, _POLICY_BEFORE)
    runner.run(adapter, mode="scope_complete")

    _git(spec_repo, "rm", "-rq", "openspec/changes/add-bar")
    _git(spec_repo, "commit", "-qm", "drop add-bar")
    _set_policy(monkeypatch, _POLICY_AFTER_ADD_SOURCE)
    replay = runner.run(adapter, mode="scope_complete")
    assert replay.completed_scope is True
    assert replay.retractions_node > 0
    assert _retraction_rows(runner) > 0


def test_code_data_flow_replay_refuses_a_missing_repo(
    okg_migrated: str,
    repo: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.errors import SubstrateError

    (repo / "a.py").write_text("import os\nDB = os.getenv('DB_URL')\n")
    _git(repo, "commit", "-qam", "config read")
    runner = _runner(okg_migrated, "code_data_flow")
    adapter = _adapter("code_data_flow", repo)
    _set_policy(monkeypatch, _POLICY_BEFORE)
    assert runner.run(adapter, mode="scope_complete").nodes_appended > 0

    repo.rename(tmp_path / "moved-away")
    _set_policy(monkeypatch, _POLICY_AFTER_ADD_SOURCE)
    with pytest.raises(SubstrateError, match="repo_root .* is missing"):
        runner.run(adapter, mode="scope_complete")
    assert _retraction_rows(runner) == 0


def test_code_data_flow_emptied_file_retracts_while_others_unchanged(
    okg_migrated: str,
    repo: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One file stops yielding facts while another stays the same.

    The walk re-reads only the changed file. Its (empty) contribution is
    exact for its key, so it replaces that key's prior record; the unchanged
    file's record is kept.
    """
    (repo / "a.py").write_text("import os\nDB = os.getenv('DB_URL')\n")
    (repo / "b.py").write_text("import os\nQ = os.getenv('QUEUE_URL')\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "two config reads")
    runner = _runner(okg_migrated, "code_data_flow")
    adapter = _adapter("code_data_flow", repo)
    _set_policy(monkeypatch, _POLICY_BEFORE)
    assert runner.run(adapter, mode="scope_complete").nodes_appended > 0

    (repo / "a.py").write_text("def hello():\n    return 1\n")
    _git(repo, "commit", "-qam", "drop one config read")
    second = runner.run(adapter, mode="scope_complete")
    assert adapter.last_run_stats["files_scanned"] < (
        adapter.last_run_stats["files_seen"]
    )
    assert second.completed_scope is True
    assert second.retractions_node + second.retractions_edge > 0

    import psycopg

    with psycopg.connect(runner.dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.node_facts "
            "WHERE source = %s AND op = 'R' "
            "AND source_record_id->>'path' = 'b.py'",
            (runner.source_name,),
        )
        row = cur.fetchone()
    assert row is not None and row[0] == 0


def test_code_data_flow_replay_replaces_a_non_empty_prior(
    okg_migrated: str,
    repo: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A file deleted while the policy changes is retracted by the replay."""
    (repo / "a.py").write_text("import os\nDB = os.getenv('DB_URL')\n")
    (repo / "c.py").write_text("import os\nC = os.getenv('C_URL')\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "two config reads")
    runner = _runner(okg_migrated, "code_data_flow")
    adapter = _adapter("code_data_flow", repo)
    _set_policy(monkeypatch, _POLICY_BEFORE)
    runner.run(adapter, mode="scope_complete")

    _git(repo, "rm", "-q", "c.py")
    _git(repo, "commit", "-qm", "drop c")
    _set_policy(monkeypatch, _POLICY_AFTER_ADD_SOURCE)
    replay = runner.run(adapter, mode="scope_complete")
    assert replay.completed_scope is True
    assert replay.retractions_node + replay.retractions_edge > 0


# --- Second review of #2896 --------------------------------------------------


def _published_openspec(okg_migrated, spec_repo, monkeypatch, **kwargs):
    runner = _runner(okg_migrated, "openspec")
    adapter = OpenSpecSource(
        openspec_root=str(spec_repo / "openspec"),
        repo="demo",
        **kwargs,
    )
    _set_policy(monkeypatch, _POLICY_BEFORE)
    assert runner.run(adapter, mode="scope_complete").nodes_appended > 0
    return runner, adapter


def test_openspec_reset_cursor_refuses_a_missing_tree(
    okg_migrated: str,
    spec_repo: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`--reset-cursor` hides the prior record set, not the published facts.

    "Nothing published" comes from the fact log, so a reset cursor cannot
    turn a missing tree into an empty scope that retires every live row.
    """
    from okg.substrate.errors import SubstrateError

    runner, adapter = _published_openspec(okg_migrated, spec_repo, monkeypatch)
    (spec_repo / "openspec").rename(tmp_path / "moved-away")
    with pytest.raises(SubstrateError, match="has published facts"):
        runner.run(adapter, mode="scope_complete", reset_cursor=True)
    assert _retraction_rows(runner) == 0


def test_openspec_unverified_record_set_refuses_a_missing_tree(
    okg_migrated: str,
    spec_repo: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A record set the runner strips as unverified is not "nothing
    published" either."""
    import psycopg

    from okg.substrate.errors import SubstrateError

    runner, adapter = _published_openspec(okg_migrated, spec_repo, monkeypatch)
    with psycopg.connect(runner.dsn) as conn:
        updated = conn.execute(
            "UPDATE okg.source_watermarks SET last_applied_rev = "
            "jsonb_set(last_applied_rev, '{record_set_chronos_proof,ok}', "
            "'false'::jsonb) WHERE source = %s "
            "AND last_applied_rev ? 'record_set_chronos_proof'",
            (runner.source_name,),
        ).rowcount
    assert updated == 1, "the fixture must reach the stored proof"
    (spec_repo / "openspec").rename(tmp_path / "moved-away")
    with pytest.raises(SubstrateError, match="has published facts"):
        runner.run(adapter, mode="scope_complete")
    assert _retraction_rows(runner) == 0


def test_openspec_tree_vanishing_during_capture_refuses(
    okg_migrated: str,
    spec_repo: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The missing-tree rule is decided on what the capture saw."""
    from okg.substrate.errors import SubstrateError
    from okg.substrate.library.sources import openspec as openspec_module

    runner, adapter = _published_openspec(okg_migrated, spec_repo, monkeypatch)
    probe_class = openspec_module._OpenSpecSnapshotProbe
    original = probe_class._read_live_openspec

    def _vanish_then_read(self):
        tree = spec_repo / "openspec"
        if tree.exists():
            tree.rename(tmp_path / "vanished")
        return original(self)

    monkeypatch.setattr(probe_class, "_read_live_openspec", _vanish_then_read)
    _set_policy(monkeypatch, _POLICY_AFTER_ADD_SOURCE)
    with pytest.raises(SubstrateError, match="has published facts"):
        runner.run(adapter, mode="scope_complete")
    assert _retraction_rows(runner) == 0


def test_openspec_optional_tree_still_needs_a_readable_repo(
    okg_migrated: str,
    spec_repo: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An optional tree is empty only inside a repository that is there.

    A missing (unmounted) repository must not pass for a removed tree.
    """
    from okg.substrate.errors import SubstrateError

    runner, adapter = _published_openspec(
        okg_migrated, spec_repo, monkeypatch, openspec_root_optional=True,
    )
    spec_repo.rename(tmp_path / "unmounted")
    _set_policy(monkeypatch, _POLICY_AFTER_ADD_SOURCE)
    with pytest.raises(SubstrateError, match="repository root"):
        runner.run(adapter, mode="scope_complete")
    assert _retraction_rows(runner) == 0


def test_openspec_unreadable_subdirectory_refuses(
    okg_migrated: str,
    spec_repo: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A subdirectory that cannot be listed raises; `rglob` skipped it."""
    runner, adapter = _published_openspec(okg_migrated, spec_repo, monkeypatch)
    sub = spec_repo / "openspec" / "changes" / "add-bar"
    sub.chmod(0)
    try:
        _set_policy(monkeypatch, _POLICY_AFTER_ADD_SOURCE)
        with pytest.raises(PermissionError):
            runner.run(adapter, mode="scope_complete")
    finally:
        sub.chmod(0o755)
    assert _retraction_rows(runner) == 0


def test_code_data_flow_unreadable_subdirectory_refuses(
    okg_migrated: str,
    repo: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """In a git work tree, a stat error other than "not found" is not a
    deletion: the replay raises and the subdirectory keeps its facts."""
    from okg.substrate.library.sources.change_probe import ChangeProbeError

    (repo / "sub").mkdir()
    (repo / "sub" / "b.py").write_text("import os\nX = os.getenv('X_URL')\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "sub")
    runner = _runner(okg_migrated, "code_data_flow")
    adapter = _adapter("code_data_flow", repo)
    _set_policy(monkeypatch, _POLICY_BEFORE)
    assert runner.run(adapter, mode="scope_complete").nodes_appended > 0

    (repo / "sub").chmod(0)
    try:
        _set_policy(monkeypatch, _POLICY_AFTER_ADD_SOURCE)
        with pytest.raises(ChangeProbeError, match="could not stat"):
            runner.run(adapter, mode="scope_complete")
    finally:
        (repo / "sub").chmod(0o755)
    assert _retraction_rows(runner) == 0


def test_openspec_published_is_read_from_the_live_graph(
    okg_migrated: str,
    repo: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With the fact log off (as installed deployments run), a source that
    never published accepts a missing tree, and one that did refuses even
    with a reset cursor. The answer comes from the live graph."""
    from okg.substrate.errors import SubstrateError

    runner = _runner(okg_migrated, "openspec", write_fact_log=False)
    adapter = OpenSpecSource(openspec_root=str(repo / "openspec"), repo="demo")
    _set_policy(monkeypatch, _POLICY_BEFORE)
    first = runner.run(adapter, mode="scope_complete")
    assert first.completed_scope is True
    assert first.nodes_appended == 0

    _write_change(repo, "add-foo")
    assert runner.run(adapter, mode="scope_complete").nodes_appended > 0
    (repo / "openspec").rename(tmp_path / "moved-away")
    with pytest.raises(SubstrateError, match="has published facts"):
        runner.run(adapter, mode="scope_complete", reset_cursor=True)
