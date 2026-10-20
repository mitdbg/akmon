"""Watcher debounce + glob filtering tests.

Tests the in-memory state machine without spinning watchdog
observers (which would be flaky under pytest's process model).

Per `add-okg-watch-mode` (2026-04-26).
"""
from __future__ import annotations

import time
from pathlib import Path

import pytest

from okg.substrate.source_sync import (
    SourceSyncCompaction,
    SourceSyncJob,
)
from okg.substrate.watch import Watcher, WatchSource


def _capture_dbos_enqueues(monkeypatch: pytest.MonkeyPatch) -> list[dict]:
    calls: list[dict] = []

    def fake_enqueue_compacted_source_sync_via_dbos(**kwargs):
        calls.append(dict(kwargs))
        return f"wf-source-sync-{len(calls)}"

    monkeypatch.setattr(
        "okg.substrate.watch.watcher."
        "enqueue_compacted_source_sync_via_dbos",
        fake_enqueue_compacted_source_sync_via_dbos,
    )
    return calls


def _watcher(tmp_path, *, debounce=5.0, includes=("**/*",), excludes=()):
    return Watcher(
        dsn="postgres://unused",
        deployment_name="test",
        roots=[tmp_path],
        include_globs=includes,
        exclude_globs=excludes,
        debounce_s=debounce,
    )


def test_excluded_paths_are_filtered(tmp_path):
    w = _watcher(
        tmp_path,
        excludes=("**/__pycache__/**", "**/*.pyc"),
    )
    sub = tmp_path / "__pycache__"
    sub.mkdir()
    f = sub / "foo.pyc"
    f.write_text("x")
    w._on_event(str(f))
    assert w.stats.events_seen == 1
    assert w.stats.events_filtered == 1
    assert w._pending_at is None  # not pending


def test_included_paths_set_pending(tmp_path):
    w = _watcher(tmp_path, includes=("**/*.md",))
    f = tmp_path / "doc.md"
    f.write_text("x")
    w._on_event(str(f))
    assert w._pending_at is not None
    assert w.stats.events_filtered == 0


@pytest.mark.parametrize("event_type", ["opened", "closed_no_write"])
def test_read_only_events_do_not_queue_a_sync(tmp_path, event_type):
    """A sync's own read of a watched file must not queue the next sync.

    Linux inotify reports `opened` and `closed_no_write` (#3295).
    """
    pytest.importorskip("watchdog")
    w = _watcher(tmp_path, includes=("**/*.md",))
    f = tmp_path / "doc.md"
    f.write_text("x")

    class Event:
        is_directory = False
        src_path = str(f)

    Event.event_type = event_type
    w._make_handler().on_any_event(Event())

    assert w._pending_at is None
    assert w.stats.events_seen == 0


@pytest.mark.parametrize(
    "event_type", ["created", "modified", "deleted", "closed"]
)
def test_content_events_queue_a_sync(tmp_path, event_type):
    pytest.importorskip("watchdog")
    w = _watcher(tmp_path, includes=("**/*.md",))
    f = tmp_path / "doc.md"
    f.write_text("x")

    class Event:
        is_directory = False
        src_path = str(f)

    Event.event_type = event_type
    w._make_handler().on_any_event(Event())

    assert w._pending_at is not None


def test_path_outside_roots_is_filtered(tmp_path):
    w = _watcher(tmp_path)
    # Synth a path that's NOT under tmp_path.
    w._on_event("/tmp/outside-anywhere.md")
    assert w.stats.events_seen == 1
    assert w.stats.events_filtered == 1
    assert w._pending_at is None


def test_glob_dispatch_against_python_files(tmp_path):
    """Sanity: a .py event is matched by `**/*.py` include
    even when nested deep."""
    w = _watcher(tmp_path, includes=("**/*.py",))
    deep = tmp_path / "a" / "b" / "c"
    deep.mkdir(parents=True)
    f = deep / "mod.py"
    f.write_text("pass")
    w._on_event(str(f))
    assert w._pending_at is not None
    assert w.stats.events_filtered == 0


def test_git_head_fsevent_suppressed_but_head_poll_event_matches(
    tmp_path: Path,
) -> None:
    root = tmp_path / "repo"
    git_dir = root / ".git"
    git_dir.mkdir(parents=True)
    head = git_dir / "HEAD"
    head.write_text("ref: refs/heads/main\n")
    w = Watcher(
        dsn="postgres://unused",
        deployment_name="test",
        roots=[root],
        source_scopes=(
            WatchSource(
                source_name="git-files",
                roots=(root,),
                include_globs=(".git/HEAD",),
            ),
        ),
        debounce_s=1.0,
    )

    w._on_event(str(head))
    assert w.stats.events_seen == 1
    assert w.stats.events_filtered == 1
    assert w._pending_events == []

    w._on_event(str(head), event_type="head_poll")
    assert w.stats.events_seen == 2
    assert w.stats.events_filtered == 1
    assert [event.scope_key for event in w._pending_events] == [".git/HEAD"]
    assert w._pending_events[0].reason == "watch:head_poll"


def test_watch_failed_enqueue_keeps_pending_events_for_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    w = Watcher(
        dsn="postgres://unused",
        deployment_name="test",
        roots=[root],
        source_scopes=(
            WatchSource(
                source_name="docs",
                roots=(root,),
                include_globs=("**/*.md",),
            ),
        ),
        debounce_s=0.1,
    )
    changed = root / "guide.md"
    changed.write_text("text")
    w._on_event(str(changed))
    assert w._pending_events

    monkeypatch.setattr(w, "_enqueue_ingest", lambda: False)
    w._flush_pending_if_ready(time.time() + 1)

    assert w._pending_at is not None
    assert w._pending_events


def test_watch_heartbeat_loop_records_idle_sources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    w = Watcher(
        dsn="postgres://unused",
        deployment_name="test",
        roots=[root],
        source_scopes=(
            WatchSource(
                source_name="docs",
                roots=(root,),
                include_globs=("**/*.md",),
            ),
        ),
        heartbeat_s=1.0,
    )
    calls: list[float] = []

    def record_once() -> None:
        calls.append(time.time())
        w._stop.set()

    monkeypatch.setattr(w, "record_heartbeat", record_once)
    w._heartbeat_loop()

    assert len(calls) == 1


def test_watcher_stop_gives_observer_the_shared_shutdown_budget(
    tmp_path: Path,
) -> None:
    watcher = _watcher(tmp_path)
    joins: list[float] = []

    class Observer:
        def stop(self) -> None:
            return None

        def join(self, *, timeout: float) -> None:
            joins.append(timeout)

    watcher._observer = Observer()
    watcher.stop(timeout_s=7.0)

    assert joins
    assert joins[0] > 6.0


@pytest.mark.postgres
def test_watch_successful_enqueue_preserves_concurrent_new_events(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    w = Watcher(
        dsn=clean_runtime,
        deployment_name="test",
        roots=[root],
        source_scopes=(
            WatchSource(
                source_name="docs",
                roots=(root,),
                include_globs=("**/*.md",),
            ),
        ),
    )
    first = root / "first.md"
    second = root / "second.md"
    first.write_text("first")
    second.write_text("second")
    w._on_event(str(first))

    def enqueue_with_concurrent_event(*_args, **_kwargs):
        w._on_event(str(second))
        return "wf-source-sync"

    monkeypatch.setattr(
        "okg.substrate.watch.watcher."
        "enqueue_compacted_source_sync_via_dbos",
        enqueue_with_concurrent_event,
    )
    assert w._enqueue_ingest() is True

    assert [event.scope_key for event in w._pending_events] == ["second.md"]
    assert w._pending_modes == {"docs": "scope_complete"}
    assert w._pending_at is not None


@pytest.mark.postgres
def test_watch_enqueues_ingest_job_for_matching_source_scope(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _capture_dbos_enqueues(monkeypatch)
    src = tmp_path / "src"
    docs = tmp_path / "docs"
    src.mkdir()
    docs.mkdir()
    w = Watcher(
        dsn=clean_runtime,
        deployment_name="test",
        roots=[src, docs],
        source_scopes=(
            WatchSource(
                source_name="code",
                roots=(src,),
                include_globs=("**/*.py",),
                run_mode="scope_complete",
            ),
            WatchSource(
                source_name="docs",
                roots=(docs,),
                include_globs=("**/*.md",),
                run_mode="scope_complete",
            ),
        ),
        debounce_s=1.0,
    )
    changed = src / "pkg" / "mod.py"
    changed.parent.mkdir()
    changed.write_text("pass")

    w._on_event(str(changed))
    w._enqueue_ingest()

    assert len(calls) == 1
    job = calls[0]["job"]
    assert isinstance(job, SourceSyncJob)
    payload = job.to_dict()
    assert calls[0]["deduplication_id"] is None
    assert payload["deployment"] == "test"
    assert payload["sources"] == ["code"]
    assert payload["run_mode"] == "scope_complete"
    assert payload["events"][0]["source_name"] == "code"
    assert payload["events"][0]["scope_key"] == "pkg/mod.py"


@pytest.mark.postgres
def test_watch_maps_multiple_roots_to_different_sources(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _capture_dbos_enqueues(monkeypatch)
    src = tmp_path / "src"
    docs = tmp_path / "docs"
    src.mkdir()
    docs.mkdir()
    w = Watcher(
        dsn=clean_runtime,
        deployment_name="test",
        roots=[src, docs],
        source_scopes=(
            WatchSource(
                source_name="code",
                roots=(src,),
                include_globs=("**/*.py",),
            ),
            WatchSource(
                source_name="docs",
                roots=(docs,),
                include_globs=("**/*.md",),
            ),
        ),
        debounce_s=1.0,
    )
    doc = docs / "guide.md"
    doc.write_text("text")

    w._on_event(str(doc))
    w._enqueue_ingest()

    payload = calls[0]["job"].to_dict()

    assert payload["sources"] == ["docs"]
    assert payload["events"][0]["scope_key"] == "guide.md"


@pytest.mark.postgres
def test_watch_compacts_compatible_queued_sync_jobs(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _capture_dbos_enqueues(monkeypatch)
    root = tmp_path / "repo"
    root.mkdir()
    w = Watcher(
        dsn=clean_runtime,
        deployment_name="test",
        roots=[root],
        source_scopes=(
            WatchSource(
                source_name="docs",
                roots=(root,),
                include_globs=("**/*.md",),
                run_mode="scope_incomplete",
                dependent_fanout="direct",
                compaction_window_seconds=60,
                compaction_max_events=10,
                compaction_max_scope_keys=10,
            ),
        ),
        debounce_s=0.1,
    )
    first = root / "first.md"
    second = root / "second.md"
    first.write_text("first")
    second.write_text("second")

    w._on_event(str(first))
    w._enqueue_ingest()
    w._on_event(str(second))
    w._enqueue_ingest()

    assert len(calls) == 2
    assert all(
        ":scope_incomplete:docs:direct:" in call["deduplication_id"]
        for call in calls
    )
    assert calls[0]["compaction"] == SourceSyncCompaction(
        window_seconds=60,
        max_events=10,
        max_scope_keys=10,
    )
    assert calls[0]["job"].events[0].scope_key == "first.md"
    assert calls[1]["job"].events[0].scope_key == "second.md"


def test_watch_dbos_path_passes_compaction_and_dedupe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    captured: dict[str, object] = {}

    def fake_enqueue_compacted_source_sync_via_dbos(**kwargs):
        captured.update(kwargs)
        return "wf-source-sync"

    monkeypatch.setattr(
        "okg.substrate.watch.watcher."
        "enqueue_compacted_source_sync_via_dbos",
        fake_enqueue_compacted_source_sync_via_dbos,
    )
    w = Watcher(
        dsn="postgres://unused",
        deployment_name="test",
        roots=[root],
        source_scopes=(
            WatchSource(
                source_name="docs",
                roots=(root,),
                include_globs=("**/*.md",),
                run_mode="scope_incomplete",
                dependent_fanout="direct",
                compaction_window_seconds=60,
                compaction_max_events=10,
                compaction_max_scope_keys=10,
            ),
        ),
        debounce_s=0.1,
    )
    changed = root / "guide.md"
    changed.write_text("text")

    w._on_event(str(changed))
    assert w._enqueue_ingest() is True

    job = captured["job"]
    assert isinstance(job, SourceSyncJob)
    assert captured["deduplication_id"].startswith(
        "sync:test:watch:scope_incomplete:docs:direct:"
    )
    assert captured["compaction"] == SourceSyncCompaction(
        window_seconds=60,
        max_events=10,
        max_scope_keys=10,
    )
    assert job.events[0].scope_key == "guide.md"


@pytest.mark.postgres
def test_watch_excluded_source_scope_produces_no_job(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _capture_dbos_enqueues(monkeypatch)
    root = tmp_path / "repo"
    root.mkdir()
    w = Watcher(
        dsn=clean_runtime,
        deployment_name="test",
        roots=[root],
        source_scopes=(
            WatchSource(
                source_name="code",
                roots=(root,),
                include_globs=("**/*.py",),
                exclude_globs=("**/__pycache__/**",),
            ),
        ),
    )
    excluded = root / "__pycache__" / "mod.py"
    excluded.parent.mkdir()
    excluded.write_text("pass")

    w._on_event(str(excluded))
    w._enqueue_ingest()

    assert calls == []


@pytest.mark.postgres
def test_watch_git_ref_scope_uses_git_ref_change_trigger(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _capture_dbos_enqueues(monkeypatch)
    git_dir = tmp_path / ".git"
    refs = git_dir / "refs" / "heads"
    refs.mkdir(parents=True)
    head = refs / "main"
    head.write_text("abc")
    w = Watcher(
        dsn=clean_runtime,
        deployment_name="test",
        roots=[git_dir],
        source_scopes=(
            WatchSource(
                source_name="git-history",
                roots=(git_dir,),
                include_globs=("HEAD", "refs/**", "packed-refs"),
                trigger_kind="git_ref_change",
                run_mode="scope_complete",
            ),
        ),
    )

    w._on_event(str(head))
    w._enqueue_ingest()

    payload = calls[0]["job"].to_dict()

    assert payload["sources"] == ["git-history"]
    assert payload["events"][0]["trigger_kind"] == "git_ref_change"
    assert payload["events"][0]["scope_key"] == "refs/heads/main"


@pytest.mark.postgres
def test_watch_git_ref_scope_accepts_repo_root_with_git_pointer(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _capture_dbos_enqueues(monkeypatch)
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / ".git").write_text("gitdir: ../common/worktrees/repo\n")
    scope = WatchSource(
        source_name="git-history",
        roots=(repo,),
        include_globs=(".git/HEAD", ".git/refs/**", ".git/packed-refs"),
        trigger_kind="git_ref_change",
        run_mode="scope_complete",
    )
    w = Watcher(
        dsn=clean_runtime,
        deployment_name="test",
        roots=[repo],
        source_scopes=(scope,),
    )

    assert w._git_repo_roots() == [(scope, repo)]
    w._on_event(str(repo / ".git" / "HEAD"), event_type="head_poll")
    w._enqueue_ingest()

    payload = calls[0]["job"].to_dict()

    assert payload["sources"] == ["git-history"]
    assert payload["events"][0]["trigger_kind"] == "git_ref_change"
    assert payload["events"][0]["scope_key"] == ".git/HEAD"
    assert payload["events"][0].get("revision_hint") is None


@pytest.mark.postgres
def test_delta_safe_delete_keeps_exact_path_scope_complete(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _capture_dbos_enqueues(monkeypatch)
    root = tmp_path / "repo"
    root.mkdir()
    target = root / "docs" / "gone.md"
    target.parent.mkdir()
    target.write_text("old")
    w = Watcher(
        dsn=clean_runtime,
        deployment_name="test",
        roots=[root],
        source_scopes=(
            WatchSource(
                source_name="git-files",
                roots=(root,),
                include_globs=("**/*.md",),
                run_mode="scope_incomplete",
                deletes_require_reconcile=True,
                reconcile_mode="scope_complete",
                scoped_delete_mode="scope_complete",
                scoped_delete_dependent_fanout="direct",
                dependent_fanout="none",
                reconcile_dependent_fanout="closure",
            ),
        ),
    )

    w._on_event(str(target), event_type="deleted")
    w._enqueue_ingest()

    payload = calls[0]["job"].to_dict()
    assert payload["sources"] == ["git-files"]
    assert payload["run_mode"] == "scope_complete"
    assert payload["dependent_fanout"] == "direct"
    assert payload["events"][0]["trigger_kind"] == "fs_event"
    assert payload["events"][0]["scope_key"] == "docs/gone.md"
    assert payload["events"][0]["reason"] == "watch:deleted"


@pytest.mark.postgres
def test_watch_moved_event_enqueues_source_and_destination_modes(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _capture_dbos_enqueues(monkeypatch)
    pytest.importorskip("watchdog")
    old_root = tmp_path / "old"
    new_root = tmp_path / "new"
    old_root.mkdir()
    new_root.mkdir()
    old_path = old_root / "guide.md"
    new_path = new_root / "guide.md"
    old_path.write_text("old")
    new_path.write_text("new")
    w = Watcher(
        dsn=clean_runtime,
        deployment_name="test",
        roots=[old_root, new_root],
        source_scopes=(
            WatchSource(
                source_name="old-docs",
                roots=(old_root,),
                include_globs=("**/*.md",),
                run_mode="cursor",
                deletes_require_reconcile=True,
                reconcile_mode="scope_complete",
            ),
            WatchSource(
                source_name="new-docs",
                roots=(new_root,),
                include_globs=("**/*.md",),
                run_mode="cursor",
            ),
        ),
        debounce_s=1.0,
    )

    class Event:
        is_directory = False
        event_type = "moved"
        src_path = str(old_path)
        dest_path = str(new_path)

    w._make_handler().on_any_event(Event())
    w._enqueue_ingest()

    payloads = [call["job"].to_dict() for call in calls]

    assert len(payloads) == 2
    by_mode = {payload["run_mode"]: payload for payload in payloads}
    assert by_mode["scope_complete"]["sources"] == ["old-docs"]
    assert by_mode["scope_complete"]["events"][0]["trigger_kind"] == "reconcile"
    assert by_mode["scope_complete"]["events"][0]["scope_key"] == "guide.md"
    assert by_mode["cursor"]["sources"] == ["new-docs"]
    assert by_mode["cursor"]["events"][0]["trigger_kind"] == "fs_event"
    assert by_mode["cursor"]["events"][0]["scope_key"] == "guide.md"
