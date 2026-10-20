from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest

import okg.substrate.source_sync_producers as source_sync_producers
from okg.substrate.ingest.change_identity import EFFECTIVE_TOKEN_SCHEMA
from okg.substrate.ingest.runner import PollProbeDecision
from okg.substrate.source_sync import SourceSyncCapabilities
from okg.substrate.sources.registry import (
    STRICT_ADMISSION_CONTRACT,
    SourceRegistryAdmission,
)

_iter_stat_tree_files = source_sync_producers._iter_stat_tree_files
_run_probe = source_sync_producers._run_probe
_stat_tree_dir_could_match_include = (
    source_sync_producers._stat_tree_dir_could_match_include
)
run_poll_once = source_sync_producers.run_poll_once


def _test_effective_token(
    content: str,
    *,
    config: str = "test-config",
    pipeline: str = "test-pipeline",
) -> dict[str, object]:
    return {
        "schema": EFFECTIVE_TOKEN_SCHEMA,
        "raw_token": {
            "schema": "test-raw-token",
            "components": {
                "content": {"kind": "content_hash", "value": content},
                "config": {"kind": "config_hash", "value": config},
                "emit_version": {
                    "kind": "emit_code_hash",
                    "value": "test-emit",
                },
            },
        },
        "pipeline_fingerprint": pipeline,
        "registry_binding_fingerprint": "test-registry-binding",
    }


def _stat_tree_caps(root: Path) -> SourceSyncCapabilities:
    return SourceSyncCapabilities.from_dict({
        "watch": {
            "roots": [str(root)],
            "include_globs": ["**/*"],
            "exclude_globs": [
                "**/.venv/**",
                ".venv/**",
                "**/__pycache__/**",
            ],
        },
        "triggers": ["poll"],
        "default_event_mode": "scope_complete",
        "poll": {
            "probe": "change_probe",
            "interval_seconds": 300,
        },
        "compaction": {
            "window_seconds": 60,
            "max_events": 100,
            "max_scope_keys": 100,
        },
    })


def _scoped_stat_tree_caps(root: Path) -> SourceSyncCapabilities:
    return SourceSyncCapabilities.from_dict({
        "watch": {
            "roots": [str(root)],
            "include_globs": ["**/*"],
            "exclude_globs": [],
        },
        "triggers": ["poll", "manual", "reconcile"],
        "default_event_mode": "scope_incomplete",
        "reconcile_mode": "scope_complete",
        "poll": {
            "probe": "change_probe",
            "interval_seconds": 300,
        },
        "dependents": {
            "on_event": "direct",
            "on_reconcile": "closure",
        },
        "compaction": {
            "window_seconds": 60,
            "max_events": 100,
            "max_scope_keys": 100,
        },
    })


def _watch_scoped_stat_tree_caps(root: Path) -> SourceSyncCapabilities:
    return SourceSyncCapabilities.from_dict({
        "watch": {
            "roots": [str(root)],
            "include_globs": ["**/*"],
            "exclude_globs": [],
        },
        "triggers": ["fs_event", "poll", "manual", "reconcile"],
        "default_event_mode": "scope_incomplete",
        "reconcile_mode": "scope_complete",
        "poll": {
            "probe": "change_probe",
            "interval_seconds": 300,
        },
        "dependents": {
            "on_event": "none",
            "on_reconcile": "closure",
        },
        "compaction": {
            "window_seconds": 60,
            "max_events": 100,
            "max_scope_keys": 100,
        },
    })


def _watch_complete_stat_tree_caps(root: Path) -> SourceSyncCapabilities:
    return SourceSyncCapabilities.from_dict({
        "watch": {
            "roots": [str(root)],
            "include_globs": ["**/*"],
            "exclude_globs": [],
        },
        "triggers": ["fs_event", "poll", "manual", "reconcile"],
        "default_event_mode": "scope_complete",
        "reconcile_mode": "scope_complete",
        "poll": {
            "probe": "change_probe",
            "interval_seconds": 300,
        },
        "dependents": {
            "on_event": "closure",
            "on_reconcile": "closure",
        },
        "compaction": {
            "window_seconds": 60,
            "max_events": 100,
            "max_scope_keys": 100,
        },
    })


def _scheduled_caps(probe: str) -> SourceSyncCapabilities:
    return SourceSyncCapabilities.from_dict({
        "triggers": ["poll"],
        "default_event_mode": "scope_complete",
        "poll": {
            "probe": probe,
            "interval_seconds": 1,
        },
        "compaction": {
            "window_seconds": 60,
            "max_events": 100,
            "max_scope_keys": 100,
        },
    })


def _poll_entry(
    source_name: str,
    sync: SourceSyncCapabilities,
    *changes: bool,
) -> SimpleNamespace:
    return SimpleNamespace(
        sync=sync,
        ownership_id=f"test.{source_name}",
        poll_changes=list(changes or (True, False)),
    )


@pytest.fixture(autouse=True)
def _adapter_backed_change_probe_harness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep producer tests focused on wakeups, not adapter construction.

    SourceRunner owns cursor equality and skip eligibility.  These tests drive
    that seam explicitly and assert that the poll producer only enqueues the
    resulting decision; it no longer owns a second fingerprint baseline.
    """

    def fake_construct(
        entry,
        *,
        dsn,
        deployment,
    ):  # noqa: ANN001, ARG001
        del deployment
        return SimpleNamespace(
            adapter=entry,
            graph_branch="default",
            registry_binding_fingerprint="test-registry-binding",
        )

    class FakeRunner:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def evaluate_poll(
            self,
            adapter,
            *,
            mode: str,
            reconcile_due: bool,
        ) -> PollProbeDecision:
            del mode
            changed = (
                bool(adapter.poll_changes.pop(0))
                if adapter.poll_changes else False
            )
            contents = getattr(adapter, "poll_effective_contents", None)
            effective_content = (
                str(contents.pop(0))
                if contents
                else str(int(changed))
            )
            return PollProbeDecision(
                enqueue=changed or reconcile_due,
                changed=changed,
                reason=(
                    "change_probe_changed"
                    if changed
                    else (
                        "reconcile_due"
                        if reconcile_due
                        else "change_probe_unchanged"
                    )
                ),
                revision_hint=f"test-revision-{effective_content}",
                effective_token=_test_effective_token(effective_content),
            )

    monkeypatch.setattr(
        "okg.substrate.source_sync_producers.construct_source_adapter",
        fake_construct,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_producers.SourceRunner",
        FakeRunner,
    )


def test_stat_tree_prunes_excluded_directories_before_stat(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    included = root / "src"
    excluded = root / ".venv" / "pkg"
    included.mkdir(parents=True)
    excluded.mkdir(parents=True)
    (included / "app.py").write_text("print('ok')\n")
    (excluded / "library.py").write_text("should not be statted\n")

    caps = _stat_tree_caps(root)
    original_stat = Path.stat

    def guarded_stat(self: Path, *args, **kwargs):
        if ".venv" in self.parts:
            raise AssertionError(f"excluded path was statted: {self}")
        return original_stat(self, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", guarded_stat)

    files = _iter_stat_tree_files(caps)

    assert [rel for _root, _path, rel in files] == ["src/app.py"]
    assert _run_probe("stat_tree", caps)


def test_stat_tree_prunes_directories_that_cannot_match_includes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    included = root / "reports"
    skipped = root / ".claude" / "worktrees" / "local" / "src"
    included.mkdir(parents=True)
    skipped.mkdir(parents=True)
    (included / "scan-semgrep.json").write_text("{}", encoding="utf-8")
    (skipped / "scan-semgrep.json").write_text("{}", encoding="utf-8")

    caps = SourceSyncCapabilities.from_dict({
        "watch": {
            "roots": [str(root)],
            "include_globs": ["reports/**/*semgrep*.json"],
            "exclude_globs": ["**/.git/**"],
        },
        "triggers": ["poll"],
        "default_event_mode": "scope_complete",
        "poll": {"probe": "change_probe", "interval_seconds": 300},
        "compaction": {
            "window_seconds": 60,
            "max_events": 100,
            "max_scope_keys": 100,
        },
    })
    original_scandir = os.scandir
    claude_root = (root / ".claude").resolve()

    def guarded_scandir(path):  # noqa: ANN001
        scan_path = Path(path).resolve()
        if scan_path == claude_root or claude_root in scan_path.parents:
            raise AssertionError(f"stat_tree descended into excluded cache: {scan_path}")
        return original_scandir(path)

    monkeypatch.setattr(os, "scandir", guarded_scandir)

    files = _iter_stat_tree_files(caps)

    assert [rel for _root, _path, rel in files] == ["reports/scan-semgrep.json"]


def test_stat_tree_prunes_repo_local_claude_without_config_exclude(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    included = root / "src"
    skipped = root / ".claude" / "worktrees" / "local" / "src"
    included.mkdir(parents=True)
    skipped.mkdir(parents=True)
    (included / "app.py").write_text("print('ok')\n", encoding="utf-8")
    (skipped / "slow.py").write_text("print('slow')\n", encoding="utf-8")

    caps = SourceSyncCapabilities.from_dict({
        "watch": {
            "roots": [str(root)],
            "include_globs": ["**/*"],
            "exclude_globs": ["**/.git/**"],
        },
        "triggers": ["poll"],
        "default_event_mode": "scope_complete",
        "poll": {"probe": "change_probe", "interval_seconds": 300},
        "compaction": {
            "window_seconds": 60,
            "max_events": 100,
            "max_scope_keys": 100,
        },
    })
    original_scandir = os.scandir
    claude_root = (root / ".claude").resolve()

    def guarded_scandir(path):  # noqa: ANN001
        scan_path = Path(path).resolve()
        if scan_path == claude_root or claude_root in scan_path.parents:
            raise AssertionError(f"stat_tree descended into local cache: {scan_path}")
        return original_scandir(path)

    monkeypatch.setattr(os, "scandir", guarded_scandir)

    files = _iter_stat_tree_files(caps)

    assert [rel for _root, _path, rel in files] == ["src/app.py"]


def test_stat_tree_prunes_encoded_claude_worktree_project_dirs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "projects"
    included = root / "-home-example-projects-demo-repo"
    excluded = root / "-home-example-projects-demo-repo--claude-worktrees-demo"
    included.mkdir(parents=True)
    (included / "session.jsonl").write_text("{}\n")
    workflows = excluded / "session-id" / "workflows"
    workflows.mkdir(parents=True)
    (workflows / "wf.json").write_text("{}\n")

    caps = SourceSyncCapabilities.from_dict({
        "watch": {
            "roots": [str(root)],
            "include_globs": ["**/*.jsonl", "**/*.json"],
            "exclude_globs": ["*--claude-worktrees*/**"],
        },
        "triggers": ["poll"],
        "default_event_mode": "scope_complete",
        "poll": {"probe": "change_probe", "interval_seconds": 300},
    })
    original_stat = Path.stat

    def guarded_stat(self: Path, *args, **kwargs):
        if "--claude-worktrees-" in str(self):
            raise AssertionError(f"excluded Claude project was statted: {self}")
        return original_stat(self, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", guarded_stat)

    files = _iter_stat_tree_files(caps)

    assert [rel for _root, _path, rel in files] == [
        "-home-example-projects-demo-repo/session.jsonl"
    ]


def test_stat_tree_prunes_unmatched_top_level_wildcard_segments(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "projects"
    allowed = root / "-home-example-projects-demo-repo--claude-worktrees-demo"
    unrelated = root / "-home-example-projects-other--claude-worktrees-demo"
    subagents = allowed / "session" / "subagents"
    subagents.mkdir(parents=True)
    unrelated.mkdir(parents=True)
    (allowed / "session.jsonl").write_text("{}\n", encoding="utf-8")
    (subagents / "agent-child.jsonl").write_text("{}\n", encoding="utf-8")
    (subagents / "agent-child.meta.json").write_text("{}\n", encoding="utf-8")
    (unrelated / "must-not-scan.jsonl").write_text("{}\n", encoding="utf-8")

    caps = SourceSyncCapabilities.from_dict({
        "watch": {
            "roots": [str(root)],
            "include_globs": [
                "-home-example-projects-demo-repo--claude-worktrees-*/*.jsonl",
                (
                    "-home-example-projects-demo-repo--claude-worktrees-*"
                    "/*/subagents/*.meta.json"
                ),
            ],
            "exclude_globs": [],
        },
        "triggers": ["poll"],
        "default_event_mode": "scope_complete",
        "poll": {"probe": "change_probe", "interval_seconds": 300},
    })
    real_scandir = os.scandir
    unrelated_root = unrelated.resolve()

    def guarded_scandir(path):  # noqa: ANN001
        scan_path = Path(path).resolve()
        if scan_path == unrelated_root or unrelated_root in scan_path.parents:
            raise AssertionError(f"stat_tree descended into unrelated project: {scan_path}")
        return real_scandir(path)

    monkeypatch.setattr(os, "scandir", guarded_scandir)

    files = _iter_stat_tree_files(caps)

    assert [rel for _root, _path, rel in files] == [
        (
            "-home-example-projects-demo-repo--claude-worktrees-demo/"
            "session.jsonl"
        ),
        (
            "-home-example-projects-demo-repo--claude-worktrees-demo/"
            "session/subagents/agent-child.jsonl"
        ),
        (
            "-home-example-projects-demo-repo--claude-worktrees-demo/"
            "session/subagents/agent-child.meta.json"
        ),
    ]


@pytest.mark.parametrize(
    ("pattern", "relative_path"),
    [
        ("*foo/bar.json", "unrelated/foo/bar.json"),
        ("requirements*.txt", "requirements-vendor/file.txt"),
    ],
)
def test_stat_tree_stays_conservative_for_slash_spanning_wildcards(
    tmp_path: Path,
    pattern: str,
    relative_path: str,
) -> None:
    from okg.substrate.watch.watcher import Watcher

    root = tmp_path / "root"
    target = root / relative_path
    target.parent.mkdir(parents=True)
    target.write_text("included\n", encoding="utf-8")
    assert Watcher._glob_matches(relative_path, pattern)

    caps = SourceSyncCapabilities.from_dict({
        "watch": {
            "roots": [str(root)],
            "include_globs": [pattern],
            "exclude_globs": [],
        },
        "triggers": ["poll"],
        "default_event_mode": "scope_complete",
        "poll": {"probe": "change_probe", "interval_seconds": 300},
    })

    assert [rel for _root, _path, rel in _iter_stat_tree_files(caps)] == [
        relative_path
    ]


def test_stat_tree_keeps_deeper_slash_spanning_wildcard_match(
    tmp_path: Path,
) -> None:
    from okg.substrate.watch.watcher import Watcher

    pattern = "root/prefix*/tail.json"
    relative_path = "root/prefix-x/nested/tail.json"
    root = tmp_path / "watch"
    target = root / relative_path
    target.parent.mkdir(parents=True)
    target.write_text("included\n", encoding="utf-8")

    assert Watcher._glob_matches(relative_path, pattern)
    assert _stat_tree_dir_could_match_include("root/prefix-x", [pattern])

    caps = SourceSyncCapabilities.from_dict({
        "watch": {
            "roots": [str(root)],
            "include_globs": [pattern],
            "exclude_globs": [],
        },
        "triggers": ["poll"],
        "default_event_mode": "scope_complete",
        "poll": {"probe": "change_probe", "interval_seconds": 300},
    })

    assert [rel for _root, _path, rel in _iter_stat_tree_files(caps)] == [
        relative_path
    ]


def test_stat_tree_mixed_agent_session_patterns_still_prune_unrelated_roots(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "projects"
    claude = root / "-home-example-projects-demo-repo--claude-worktrees-demo"
    codex = root / "-home-example--codex-worktrees-81b8-okg"
    unrelated = root / "-home-example-projects-other--claude-worktrees-demo"
    for directory in (claude, codex, unrelated):
        directory.mkdir(parents=True)
        (directory / "session.jsonl").write_text("{}\n", encoding="utf-8")

    patterns = [
        "-home-example-projects-demo-repo--claude-worktrees-*/*.jsonl",
        "-home-example--codex-worktrees-*-okg/*.jsonl",
    ]
    assert _stat_tree_dir_could_match_include(claude.name, patterns)
    assert _stat_tree_dir_could_match_include(codex.name, patterns)
    assert not _stat_tree_dir_could_match_include(unrelated.name, patterns)
    assert not _stat_tree_dir_could_match_include("totally-unrelated", patterns)

    real_scandir = os.scandir
    unrelated_root = unrelated.resolve()

    def guarded_scandir(path):  # noqa: ANN001
        scan_path = Path(path).resolve()
        if scan_path == unrelated_root or unrelated_root in scan_path.parents:
            raise AssertionError(f"stat_tree descended into unrelated project: {scan_path}")
        return real_scandir(path)

    monkeypatch.setattr(os, "scandir", guarded_scandir)
    caps = SourceSyncCapabilities.from_dict({
        "watch": {
            "roots": [str(root)],
            "include_globs": patterns,
            "exclude_globs": [],
        },
        "triggers": ["poll"],
        "default_event_mode": "scope_complete",
        "poll": {"probe": "change_probe", "interval_seconds": 300},
    })

    assert [rel for _root, _path, rel in _iter_stat_tree_files(caps)] == [
        f"{codex.name}/session.jsonl",
        f"{claude.name}/session.jsonl",
    ]


@pytest.mark.postgres
def test_change_probe_does_not_enqueue_without_content_change(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    caps = _scheduled_caps("change_probe")
    entry = _poll_entry("scheduled", caps, True, False)
    calls: list[dict[str, object]] = []
    now = {"value": 1000.0}

    def fake_enqueue_compacted_source_sync_via_dbos(**kwargs):
        calls.append(dict(kwargs))
        return f"wf-source-sync-{len(calls)}"

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "enqueue_compacted_source_sync_via_dbos",
        fake_enqueue_compacted_source_sync_via_dbos,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_producers.time.time",
        lambda: now["value"],
    )

    first = run_poll_once(
        clean_runtime,
        deployment="test",
        registry={"scheduled": entry},
        source="scheduled",
        backstop_multiplier=1,
    )
    assert first["errors"] == []
    assert first["results"][0]["changed"] is True

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.source_watermarks
              (source, last_applied_rev, last_reconcile_at)
            VALUES (%s, %s::jsonb, now())
            ON CONFLICT (source)
            DO UPDATE SET last_applied_rev = EXCLUDED.last_applied_rev,
                          last_reconcile_at = EXCLUDED.last_reconcile_at
            """,
            (
                "test.scheduled",
                json.dumps({"source": "test.scheduled"}),
            ),
        )

    now["value"] += 10.0
    second = run_poll_once(
        clean_runtime,
        deployment="test",
        registry={"scheduled": entry},
        source="scheduled",
        backstop_multiplier=1,
    )

    assert second["errors"] == []
    assert second["results"][0]["changed"] is False
    assert second["results"][0]["backstop_reconcile"] is None
    assert len(calls) == 1


@pytest.mark.postgres
def test_change_probe_error_enqueues_without_acknowledging_source_state(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    caps = _scheduled_caps("change_probe")
    entry = _poll_entry("scheduled", caps)
    calls: list[dict[str, object]] = []
    prior = {"sentinel": "accepted-cursor"}

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            """
            INSERT INTO okg.source_watermarks
              (source, last_applied_rev, last_reconcile_at)
            VALUES ('test.scheduled', %s::jsonb, now())
            """,
            (json.dumps(prior),),
        )

    def fail_construction(
        _entry,
        *,
        dsn,
        deployment,
    ):  # noqa: ARG001
        raise ValueError("sensitive adapter detail")

    def fake_enqueue_compacted_source_sync_via_dbos(**kwargs):
        calls.append(dict(kwargs))
        return "wf-probe-repair"

    monkeypatch.setattr(
        "okg.substrate.source_sync_producers.construct_source_adapter",
        fail_construction,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "enqueue_compacted_source_sync_via_dbos",
        fake_enqueue_compacted_source_sync_via_dbos,
    )

    out = run_poll_once(
        clean_runtime,
        deployment="test",
        registry={"scheduled": entry},
        source="scheduled",
    )

    assert out["errors"] == []
    assert out["results"][0]["decision_reason"] == "error:ValueError"
    assert "sensitive adapter detail" not in json.dumps(out)
    event = calls[0]["job"].events[0]
    assert event.trigger_kind == "poll"
    assert event.reason == "poll:change_probe:error:ValueError"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cursor, reconciled = conn.execute(
            """
            SELECT last_applied_rev, last_reconcile_at IS NOT NULL
              FROM okg.source_watermarks
             WHERE source = 'test.scheduled'
            """
        ).fetchone()
    assert cursor == prior
    assert reconciled is True


@pytest.mark.postgres
def test_always_changed_never_constructs_adapter(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    caps = _scheduled_caps("always_changed")
    entry = _poll_entry("live-api", caps)
    calls: list[dict[str, object]] = []

    def fail_construction(*_args, **_kwargs):
        raise AssertionError("always_changed must not construct an adapter")

    def fake_enqueue_compacted_source_sync_via_dbos(**kwargs):
        calls.append(dict(kwargs))
        return "wf-always-changed"

    monkeypatch.setattr(
        "okg.substrate.source_sync_producers.construct_source_adapter",
        fail_construction,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "enqueue_compacted_source_sync_via_dbos",
        fake_enqueue_compacted_source_sync_via_dbos,
    )

    out = run_poll_once(
        clean_runtime,
        deployment="test",
        registry={"live-api": entry},
        source="live-api",
    )

    assert out["errors"] == []
    assert out["results"][0]["decision_reason"] == "always_changed"
    assert out["results"][0]["changed"] is True
    assert len(calls) == 1


def test_strict_always_changed_constructs_before_poll_state_or_enqueue(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entry = _poll_entry("live-api", _scheduled_caps("always_changed"))
    entry.admission_contract = STRICT_ADMISSION_CONTRACT
    entry.is_registry_only = False
    entry.raw = {"module": "tests.strict", "class": "Adapter"}
    admission = SourceRegistryAdmission(
        contract=STRICT_ADMISSION_CONTRACT,
        entries={"live-api": entry},
        registry_path=tmp_path / "source_registry.yaml",
    )
    monkeypatch.setattr(
        source_sync_producers,
        "plan_source_registry_bindings",
        lambda *_args, **_kwargs: SimpleNamespace(
            plans={"live-api": object()}
        ),
    )
    monkeypatch.setattr(
        source_sync_producers,
        "construct_source_adapter_from_plan",
        lambda _plan: (_ for _ in ()).throw(RuntimeError("constructor failed")),
    )
    monkeypatch.setattr(
        source_sync_producers,
        "_poll_backstop_reconcile_due",
        lambda *_args, **_kwargs: pytest.fail("poll state read before barrier"),
    )

    with pytest.raises(RuntimeError, match="constructor failed"):
        run_poll_once(
            "postgresql://unused",
            deployment="test",
            registry=admission,
            source="live-api",
        )


def test_strict_poll_planning_retry_preserves_repo_root_override(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entry = _poll_entry("scheduled", _scheduled_caps("change_probe"))
    entry.admission_contract = STRICT_ADMISSION_CONTRACT
    entry.is_registry_only = False
    entry.raw = {"module": "tests.strict", "class": "Adapter"}
    admission = SourceRegistryAdmission(
        contract=STRICT_ADMISSION_CONTRACT,
        entries={"scheduled": entry},
        registry_path=tmp_path / "source_registry.yaml",
    )
    calls: list[str] = []

    def plan(_admission, *, dsn, deployment):
        del dsn, deployment
        calls.append("plan")
        if len(calls) == 1:
            raise ValueError("source_param_unconsumed: stale signature")
        return SimpleNamespace(plans={"scheduled": object()})

    overrides: list[Path | str | None] = []

    def reload_admission(*, admission, registry_path, repo_root_override):
        del registry_path
        overrides.append(repo_root_override)
        return admission

    monkeypatch.setattr(
        source_sync_producers,
        "plan_source_registry_bindings",
        plan,
    )
    monkeypatch.setattr(
        source_sync_producers,
        "_reload_strict_poll_admission",
        reload_admission,
    )

    monkeypatch.setattr(
        source_sync_producers,
        "construct_source_adapter_from_plan",
        lambda _plan: SimpleNamespace(adapter=object()),
    )
    registry_path = tmp_path / "source_registry.yaml"
    registry_path.write_text("admission_contract: strict_v1\nsources: {}\n")

    _, constructed = source_sync_producers._construct_strict_poll_bindings(
        admission=admission,
        dsn="postgresql://unused",
        deployment="test",
        selected_names={"scheduled"},
        registry_path=registry_path,
        repo_root_override=tmp_path,
        reselect_names=set,
    )

    assert calls == ["plan", "plan"]
    assert overrides == [tmp_path]
    assert set(constructed) == {"scheduled"}


def test_strict_poll_constructor_stale_retry_reselects_fresh_document(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    old_entry = _poll_entry("old", _scheduled_caps("change_probe"))
    old_entry.admission_contract = STRICT_ADMISSION_CONTRACT
    old_entry.is_registry_only = False
    old_entry.raw = {"module": "tests.strict", "class": "Adapter"}
    fresh_entry = _poll_entry("fresh", _scheduled_caps("change_probe"))
    fresh_entry.admission_contract = STRICT_ADMISSION_CONTRACT
    fresh_entry.is_registry_only = False
    fresh_entry.raw = {"module": "tests.strict", "class": "Adapter"}
    registry_path = tmp_path / "source_registry.yaml"
    registry_path.write_text("admission_contract: strict_v1\nsources: {}\n")
    old = SourceRegistryAdmission(
        contract=STRICT_ADMISSION_CONTRACT,
        entries={"old": old_entry},
        registry_path=registry_path,
    )
    fresh = SourceRegistryAdmission(
        contract=STRICT_ADMISSION_CONTRACT,
        entries={"fresh": fresh_entry},
        registry_path=registry_path,
    )
    constructed: list[str] = []

    def plan(admission, *, dsn, deployment):
        del dsn, deployment
        return SimpleNamespace(plans={
            name: SimpleNamespace(
                entry=entry,
                init_params={},
                marker=name,
            )
            for name, entry in admission.entries.items()
        })

    def construct(plan):
        constructed.append(plan.marker)
        if plan.marker == "old":
            raise TypeError("unexpected keyword argument 'stale'")
        return SimpleNamespace(adapter=object())

    monkeypatch.setattr(
        source_sync_producers,
        "plan_source_registry_bindings",
        plan,
    )
    monkeypatch.setattr(
        source_sync_producers,
        "construct_source_adapter_from_plan",
        construct,
    )
    monkeypatch.setattr(
        source_sync_producers,
        "classify_source_construction_failure",
        lambda **_kwargs: {"cause": "stale_loaded_code"},
    )

    monkeypatch.setattr(
        source_sync_producers,
        "_reload_strict_poll_admission",
        lambda **_kwargs: fresh,
    )

    admission, bindings = (
        source_sync_producers._construct_strict_poll_bindings(
            admission=old,
            dsn="postgresql://unused",
            deployment="test",
            selected_names={"old"},
            registry_path=registry_path,
            repo_root_override=tmp_path,
            reselect_names=set,
        )
    )

    assert admission is fresh
    assert constructed == ["old", "fresh"]
    assert set(bindings) == {"fresh"}


@pytest.mark.postgres
def test_poll_dbos_enqueue_uses_deduplication_key(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    (root / "doc.md").write_text("hello\n")
    caps = _stat_tree_caps(root)
    captured: dict[str, object] = {}

    def fake_enqueue_compacted_source_sync_via_dbos(**kwargs):
        captured.update(kwargs)
        return "wf-source-sync"

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "enqueue_compacted_source_sync_via_dbos",
        fake_enqueue_compacted_source_sync_via_dbos,
    )

    out = run_poll_once(
        clean_runtime,
        deployment="test",
        registry={"docs": _poll_entry("docs", caps)},
        source="docs",
    )

    assert out["errors"] == []
    assert out["results"][0]["changed"] is True
    assert captured["deduplication_id"].startswith(
        "sync:test:poll:scope_complete:_poll_batch:closure:"
    )
    assert captured["compaction"] == caps.compaction


@pytest.mark.postgres
def test_poll_does_not_advance_legacy_producer_fingerprint(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    (root / "doc.md").write_text("hello\n")
    caps = _watch_scoped_stat_tree_caps(root)
    old_fingerprint = "legacy-producer-baseline"
    entry = _poll_entry("docs", caps, False)

    def fail_if_enqueue_called(**_kwargs):
        raise AssertionError("watch-covered poll drift enqueued work")

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "enqueue_compacted_source_sync_via_dbos",
        fail_if_enqueue_called,
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.source_progress
              (deployment, source_name, key, value, updated_at)
            VALUES (
              'test', 'docs', 'sync.poll.stat_tree.fingerprint',
              %s, now() - interval '5 minutes'
            )
            """,
            (old_fingerprint,),
        )
        cur.execute(
            """
            INSERT INTO okg.source_watermarks (
              source, last_applied_rev, last_reconcile_at
            )
            VALUES ('test.docs', '{}'::jsonb, now())
            """
        )

    out = run_poll_once(
        clean_runtime,
        deployment="test",
        registry={"docs": entry},
        source="docs",
    )

    assert out["errors"] == []
    result = out["results"][0]
    assert result["changed"] is False
    assert result["backstop_reconcile"] is None
    assert "poll_baseline_advanced" not in result
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            SELECT value
              FROM okg.source_progress
             WHERE deployment = 'test'
               AND source_name = 'docs'
               AND key = 'sync.poll.stat_tree.fingerprint'
            """
        )
        assert cur.fetchone()[0] == old_fingerprint


@pytest.mark.postgres
def test_watch_progress_without_finished_at_does_not_advance_poll_baseline(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    doc = root / "doc.md"
    doc.write_text("before\n")
    caps = _watch_scoped_stat_tree_caps(root)
    old_fingerprint = "prior-change-probe-baseline"
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.source_progress
              (deployment, source_name, key, value, updated_at)
            VALUES ('test', 'docs', %s, %s, now() - interval '5 minutes')
            """,
            ("sync.poll.change_probe.fingerprint", old_fingerprint),
        )
        cur.execute(
            """
            INSERT INTO okg.source_progress
              (deployment, source_name, key, value, updated_at)
            VALUES (
              'test', 'docs', 'sync.poll.change_probe.suppressed_observation',
              %s, now() - interval '1 minute'
            )
            """,
            (
                json.dumps({
                    "schema": "okg.source_sync.poll_suppression.v1",
                    "workflow_id": "wf-watch-metadata-only",
                    "revision_hint": "test-revision-1",
                    "effective_token": _test_effective_token("1"),
                }),
            ),
        )
        cur.execute(
            """
            INSERT INTO okg.source_progress
              (deployment, source_name, key, value, updated_at)
            VALUES (
              'test', 'test.docs', 'sync.last_run',
              %s, now()
            )
            """,
            (
                json.dumps({
                    "status": "success",
                    "run_id": "metadata-only",
                    "workflow_id": "wf-watch-metadata-only",
                }),
            ),
        )
        cur.execute(
            """
            INSERT INTO okg.source_watermarks
              (source, last_applied_rev, last_reconcile_at)
            VALUES ('test.docs', '{}'::jsonb, now())
            """
        )
    doc.write_text("after\n")
    calls: list[dict] = []
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "active_dbos_source_sync_workflow_status_for_source",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "enqueue_compacted_source_sync_via_dbos",
        lambda **kwargs: calls.append(kwargs) or "wf-poll",
    )

    out = run_poll_once(
        clean_runtime,
        deployment="test",
        registry={"docs": _poll_entry("docs", caps, True)},
        source="docs",
    )

    assert out["errors"] == []
    assert len(calls) == 1
    assert calls[0]["job"].events[0].trigger_kind == "poll"


@pytest.mark.postgres
def test_successful_watch_advances_current_probe_baseline_without_enqueue(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    (root / "doc.md").write_text("after\n")
    caps = _watch_scoped_stat_tree_caps(root)
    old_fingerprint = "prior-change-probe-baseline"
    prior_cursor = {
        "change_probe_token": _test_effective_token("1"),
    }
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.source_progress
              (deployment, source_name, key, value, updated_at)
            VALUES (
              'test', 'docs', 'sync.poll.change_probe.fingerprint',
              %s, now() - interval '5 minutes'
            )
            """,
            (old_fingerprint,),
        )
        cur.execute(
            """
            INSERT INTO okg.source_progress
              (deployment, source_name, key, value, updated_at)
            VALUES (
              'test', 'docs', 'sync.poll.change_probe.suppressed_observation',
              %s, now() - interval '1 minute'
            )
            """,
            (
                json.dumps({
                    "schema": "okg.source_sync.poll_suppression.v1",
                    "workflow_id": "wf-watch-complete",
                    "revision_hint": "test-revision-1",
                    "effective_token": _test_effective_token("1"),
                }),
            ),
        )
        cur.execute(
            """
            INSERT INTO okg.source_progress
              (deployment, source_name, key, value, updated_at)
            VALUES (
              'test', 'test.docs', 'sync.last_run',
              jsonb_build_object(
                'workflow_id', 'wf-watch-complete',
                'finished_at', now()::text
              ), now()
            )
            """
        )
        cur.execute(
            """
            INSERT INTO okg.source_watermarks
              (source, last_applied_rev, last_reconcile_at)
            VALUES ('test.docs', %s::jsonb, now())
            """,
            (json.dumps(prior_cursor),),
        )
    monkeypatch.setattr(
        "okg.substrate.source_sync_producers."
        "_dbos_watch_workflow_completed_after",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "active_dbos_source_sync_workflow_status_for_source",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "enqueue_compacted_source_sync_via_dbos",
        lambda **_kwargs: pytest.fail("completed watch change was double-fired"),
    )

    out = run_poll_once(
        clean_runtime,
        deployment="test",
        registry={"docs": _poll_entry("docs", caps, True)},
        source="docs",
    )

    assert out["errors"] == []
    result = out["results"][0]
    assert result["changed"] is False
    assert result["poll_baseline_advanced"]["reason"] == (
        "watch_backed_source_run"
    )
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        fingerprint = cur.execute(
            """
            SELECT value FROM okg.source_progress
             WHERE deployment = 'test' AND source_name = 'docs'
               AND key = 'sync.poll.change_probe.fingerprint'
            """
        ).fetchone()[0]
        cursor = cur.execute(
            "SELECT last_applied_rev FROM okg.source_watermarks "
            "WHERE source = 'test.docs'"
        ).fetchone()[0]
        receipt_count = cur.execute(
            """
            SELECT count(*) FROM okg.source_progress
             WHERE deployment = 'test' AND source_name = 'docs'
               AND key = 'sync.poll.change_probe.suppressed_observation'
            """
        ).fetchone()[0]
    assert fingerprint == "test-revision-1"
    assert cursor["change_probe_token"] == _test_effective_token("1")
    assert receipt_count == 0


@pytest.mark.postgres
def test_unconfirmed_watch_receipt_enqueues_when_probe_is_unchanged(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    (root / "doc.md").write_text("after\n")
    caps = _watch_scoped_stat_tree_caps(root)
    applied_cursor = {
        "change_probe_token": _test_effective_token("1"),
    }
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.source_progress
              (deployment, source_name, key, value, updated_at)
            VALUES
              ('test', 'docs', 'sync.poll.change_probe.fingerprint',
               'prior-change-probe-baseline', now() - interval '5 minutes'),
              ('test', 'docs',
               'sync.poll.change_probe.suppressed_observation',
               %s, now() - interval '1 minute'),
              ('test', 'test.docs', 'sync.last_run',
               jsonb_build_object(
                 'workflow_id', 'wf-watch-not-successful',
                 'finished_at', now()::text
               ), now())
            """,
            (
                json.dumps({
                    "schema": "okg.source_sync.poll_suppression.v1",
                    "workflow_id": "wf-watch-not-successful",
                    "revision_hint": "test-revision-1",
                    "effective_token": _test_effective_token("1"),
                }),
            ),
        )
        cur.execute(
            """
            INSERT INTO okg.source_watermarks
              (source, last_applied_rev, last_reconcile_at)
            VALUES ('test.docs', %s::jsonb, now())
            """,
            (json.dumps(applied_cursor),),
        )
    monkeypatch.setattr(
        "okg.substrate.source_sync_producers."
        "_dbos_watch_workflow_completed_after",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "active_dbos_source_sync_workflow_status_for_source",
        lambda **_kwargs: None,
    )
    calls: list[dict] = []
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "enqueue_compacted_source_sync_via_dbos",
        lambda **kwargs: calls.append(kwargs) or "wf-poll-retry",
    )
    entry = _poll_entry("docs", caps, False)
    entry.poll_effective_contents = ["1"]

    out = run_poll_once(
        clean_runtime,
        deployment="test",
        registry={"docs": entry},
        source="docs",
    )

    assert out["errors"] == []
    assert len(calls) == 1
    result = out["results"][0]
    assert result["changed"] is True
    assert result["decision_reason"] == "suppressed_watch_unconfirmed"
    assert result["enqueue"]["workflow_id"] == "wf-poll-retry"


@pytest.mark.postgres
def test_finished_watch_without_suppressed_token_applied_enqueues_poll(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    caps = _watch_scoped_stat_tree_caps(root)
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.source_progress
              (deployment, source_name, key, value, updated_at)
            VALUES
              ('test', 'docs', 'sync.poll.change_probe.fingerprint',
               'test-revision-A', now() - interval '5 minutes'),
              ('test', 'docs',
               'sync.poll.change_probe.suppressed_observation',
               %s, now() - interval '1 minute')
            """,
            (
                json.dumps({
                    "schema": "okg.source_sync.poll_suppression.v1",
                    "workflow_id": "wf-watch-A",
                    "revision_hint": "test-revision-1",
                    "effective_token": _test_effective_token("1"),
                }),
            ),
        )
        cur.execute(
            """
            INSERT INTO okg.source_progress
              (deployment, source_name, key, value, updated_at)
            VALUES (
              'test', 'test.docs', 'sync.last_run',
              jsonb_build_object(
                'workflow_id', 'wf-watch-A',
                'finished_at', now()::text
              ), now()
            )
            """
        )
        cur.execute(
            """
            INSERT INTO okg.source_watermarks
              (source, last_applied_rev, last_reconcile_at)
            VALUES ('test.docs', %s::jsonb, now())
            """,
            (
                json.dumps({
                    "change_probe_token": _test_effective_token("0"),
                }),
            ),
        )
    calls: list[dict] = []
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "active_dbos_source_sync_workflow_status_for_source",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "enqueue_compacted_source_sync_via_dbos",
        lambda **kwargs: calls.append(kwargs) or "wf-poll-B",
    )

    out = run_poll_once(
        clean_runtime,
        deployment="test",
        registry={"docs": _poll_entry("docs", caps, True)},
        source="docs",
    )

    assert out["errors"] == []
    assert len(calls) == 1
    assert calls[0]["job"].events[0].revision_hint == "test-revision-1"


@pytest.mark.postgres
def test_active_watch_work_suppresses_poll_without_advancing_fingerprint(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    doc = root / "doc.md"
    doc.write_text("before\n")
    caps = _watch_scoped_stat_tree_caps(root)
    old_fingerprint = "prior-change-probe-baseline"
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.source_progress
              (deployment, source_name, key, value, updated_at)
            VALUES ('test', 'docs', %s, %s, now() - interval '5 minutes')
            """,
            ("sync.poll.change_probe.fingerprint", old_fingerprint),
        )
        cur.execute(
            """
            INSERT INTO okg.source_watermarks
              (source, last_applied_rev, last_reconcile_at)
            VALUES ('test.docs', '{}'::jsonb, now())
            """
        )
    doc.write_text("after\n")
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "active_dbos_source_sync_workflow_status_for_source",
        lambda **_kwargs: {
            "workflow_id": "wf-watch-active",
            "status": "PENDING",
            "stale": False,
            "run_mode": caps.default_event_mode,
            "dependent_fanout": caps.dependents.on_event,
            "publish": True,
            "trigger_kinds": ["fs_event"],
        },
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "enqueue_compacted_source_sync_via_dbos",
        lambda **_kwargs: pytest.fail("compatible active watch double-fired"),
    )

    out = run_poll_once(
        clean_runtime,
        deployment="test",
        registry={"docs": _poll_entry("docs", caps, True)},
        source="docs",
    )

    assert out["errors"] == []
    result = out["results"][0]
    assert result["changed"] is True
    assert result["enqueue_suppressed"]["reason"] == (
        "watch_backed_active_source_sync"
    )
    assert result["enqueue_suppressed"]["workflow_id"] == "wf-watch-active"
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT value FROM okg.source_progress
             WHERE deployment = 'test' AND source_name = 'docs'
               AND key = 'sync.poll.change_probe.fingerprint'
            """
        )
        assert cur.fetchone()[0] == old_fingerprint
        receipt = cur.execute(
            """
            SELECT value::jsonb FROM okg.source_progress
             WHERE deployment = 'test' AND source_name = 'docs'
               AND key = 'sync.poll.change_probe.suppressed_observation'
            """
        ).fetchone()[0]
    assert receipt["workflow_id"] == "wf-watch-active"
    assert receipt["revision_hint"] == "test-revision-1"
    assert receipt["effective_token"] == _test_effective_token("1")


@pytest.mark.postgres
def test_new_change_after_completed_watch_is_enqueued(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    caps = _watch_scoped_stat_tree_caps(root)
    entry = _poll_entry("docs", caps, True)
    entry.poll_effective_contents = ["C"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.source_progress
              (deployment, source_name, key, value, updated_at)
            VALUES
              ('test', 'docs', 'sync.poll.change_probe.fingerprint',
               'test-revision-A', now() - interval '5 minutes'),
              ('test', 'docs',
               'sync.poll.change_probe.suppressed_observation',
               %s, now() - interval '1 minute')
            """,
            (
                json.dumps({
                    "schema": "okg.source_sync.poll_suppression.v1",
                    "workflow_id": "wf-watch-B",
                    "revision_hint": "test-revision-B",
                    "effective_token": _test_effective_token("B"),
                }),
            ),
        )
        cur.execute(
            """
            INSERT INTO okg.source_progress
              (deployment, source_name, key, value, updated_at)
            VALUES (
              'test', 'test.docs', 'sync.last_run',
              jsonb_build_object(
                'workflow_id', 'wf-watch-B',
                'finished_at', now()::text
              ), now()
            )
            """
        )
        cur.execute(
            """
            INSERT INTO okg.source_watermarks
              (source, last_applied_rev, last_reconcile_at)
            VALUES ('test.docs', '{}'::jsonb, now())
            """
        )
    calls: list[dict] = []
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "active_dbos_source_sync_workflow_status_for_source",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "enqueue_compacted_source_sync_via_dbos",
        lambda **kwargs: calls.append(kwargs) or "wf-poll-C",
    )

    out = run_poll_once(
        clean_runtime,
        deployment="test",
        registry={"docs": entry},
        source="docs",
    )

    assert out["errors"] == []
    assert len(calls) == 1
    assert calls[0]["job"].events[0].revision_hint == "test-revision-C"
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        receipt_count = cur.execute(
            """
            SELECT count(*) FROM okg.source_progress
             WHERE deployment = 'test' AND source_name = 'docs'
               AND key = 'sync.poll.change_probe.suppressed_observation'
            """
        ).fetchone()[0]
    assert receipt_count == 0


@pytest.mark.postgres
@pytest.mark.parametrize(
    "active",
    [
        {
            "workflow_id": "wf-watch-stale",
            "status": "PENDING",
            "stale": True,
        },
        {
            "workflow_id": "wf-watch-failed",
            "status": "FAILED",
            "stale": False,
        },
    ],
)
def test_stale_or_failed_watch_work_does_not_suppress_poll(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    active: dict,
) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    (root / "doc.md").write_text("changed\n")
    caps = _watch_scoped_stat_tree_caps(root)
    active.update({
        "run_mode": caps.default_event_mode,
        "dependent_fanout": caps.dependents.on_event,
        "publish": True,
        "trigger_kinds": ["fs_event"],
    })
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.source_watermarks
              (source, last_applied_rev, last_reconcile_at)
            VALUES ('test.docs', '{}'::jsonb, now())
            """
        )
    calls: list[dict] = []
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "active_dbos_source_sync_workflow_status_for_source",
        lambda **_kwargs: dict(active),
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "enqueue_compacted_source_sync_via_dbos",
        lambda **kwargs: calls.append(kwargs) or "wf-poll",
    )

    out = run_poll_once(
        clean_runtime,
        deployment="test",
        registry={"docs": _poll_entry("docs", caps, True)},
        source="docs",
    )

    assert out["errors"] == []
    assert len(calls) == 1
    assert calls[0]["job"].events[0].trigger_kind == "poll"


@pytest.mark.postgres
def test_poll_backstop_dominates_compatible_active_watch_work(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    (root / "doc.md").write_text("changed\n")
    caps = _watch_complete_stat_tree_caps(root)
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "active_dbos_source_sync_workflow_status_for_source",
        lambda **_kwargs: {
            "workflow_id": "wf-watch-active",
            "status": "PENDING",
            "stale": False,
            "run_mode": caps.default_event_mode,
            "dependent_fanout": caps.dependents.on_event,
            "publish": True,
            "trigger_kinds": ["fs_event"],
        },
    )
    calls: list[dict] = []
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "enqueue_compacted_source_sync_via_dbos",
        lambda **kwargs: calls.append(kwargs) or "wf-reconcile",
    )

    out = run_poll_once(
        clean_runtime,
        deployment="test",
        registry={"docs": _poll_entry("docs", caps, True)},
        source="docs",
    )

    assert out["errors"] == []
    assert len(calls) == 1
    job = calls[0]["job"]
    assert job.events[0].trigger_kind == "reconcile"
    assert job.run_mode == "scope_complete"


@pytest.mark.postgres
def test_poll_backstop_reconcile_dominates_changed_probe_when_watermark_missing(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    (root / "doc.md").write_text("hello\n")
    caps = _scoped_stat_tree_caps(root)
    calls: list[dict[str, object]] = []

    def fake_enqueue_compacted_source_sync_via_dbos(**kwargs):
        calls.append(dict(kwargs))
        return "wf-source-sync"

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "enqueue_compacted_source_sync_via_dbos",
        fake_enqueue_compacted_source_sync_via_dbos,
    )

    out = run_poll_once(
        clean_runtime,
        deployment="test",
        registry={"docs": _poll_entry("docs", caps)},
        source="docs",
    )

    assert out["errors"] == []
    result = out["results"][0]
    assert result["changed"] is True
    assert result["backstop_reconcile"]["reason"] == "missing_watermark"
    job = calls[-1]["job"].to_dict()
    assert job["run_mode"] == "scope_complete"
    assert job["dependent_fanout"] == "closure"
    assert job["events"][0]["trigger_kind"] == "reconcile"
    assert job["events"][0]["reason"] == (
        "poll_backstop:change_probe:missing_watermark:changed"
    )


@pytest.mark.postgres
def test_poll_dbos_enqueue_batches_compatible_sources(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    docs_root = tmp_path / "docs"
    notes_root = tmp_path / "notes"
    docs_root.mkdir()
    notes_root.mkdir()
    (docs_root / "doc.md").write_text("hello\n")
    (notes_root / "note.md").write_text("hello\n")
    docs_caps = _stat_tree_caps(docs_root)
    notes_caps = _stat_tree_caps(notes_root)
    calls: list[dict[str, object]] = []

    def fake_enqueue_compacted_source_sync_via_dbos(**kwargs):
        calls.append(dict(kwargs))
        return "wf-source-sync"

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "enqueue_compacted_source_sync_via_dbos",
        fake_enqueue_compacted_source_sync_via_dbos,
    )

    out = run_poll_once(
        clean_runtime,
        deployment="test",
        registry={
            "docs": _poll_entry("docs", docs_caps),
            "notes": _poll_entry("notes", notes_caps),
        },
    )

    assert out["errors"] == []
    assert len(calls) == 2
    assert {
        call["deduplication_id"].rsplit(":", 1)[0]
        for call in calls
    } == {"sync:test:poll:scope_complete:_poll_batch:closure"}


@pytest.mark.postgres
def test_poll_backstop_reconciles_stale_watermark_when_fingerprint_unchanged(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    (root / "doc.md").write_text("hello\n")
    caps = _stat_tree_caps(root)
    entry = _poll_entry("docs", caps, True, False)
    calls: list[dict[str, object]] = []

    def fake_enqueue_compacted_source_sync_via_dbos(**kwargs):
        calls.append(dict(kwargs))
        return f"wf-source-sync-{len(calls)}"

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "enqueue_compacted_source_sync_via_dbos",
        fake_enqueue_compacted_source_sync_via_dbos,
    )

    first = run_poll_once(
        clean_runtime,
        deployment="test",
        registry={"docs": entry},
        source="docs",
        backstop_multiplier=1,
    )
    assert first["errors"] == []
    assert first["results"][0]["changed"] is True

    stale = datetime.now(timezone.utc) - timedelta(minutes=10)
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.source_watermarks
              (source, last_applied_rev, last_reconcile_at)
            VALUES (%s, %s::jsonb, %s)
            ON CONFLICT (source)
            DO UPDATE SET last_applied_rev = EXCLUDED.last_applied_rev,
                          last_reconcile_at = EXCLUDED.last_reconcile_at
            """,
            ("test.docs", json.dumps({"source": "test.docs"}), stale),
        )

    second = run_poll_once(
        clean_runtime,
        deployment="test",
        registry={"docs": entry},
        source="docs",
        backstop_multiplier=1,
    )

    assert second["errors"] == []
    result = second["results"][0]
    assert result["changed"] is False
    assert result["backstop_reconcile"]["reason"] == "stale_watermark"
    job = calls[-1]["job"].to_dict()
    assert job["run_mode"] == "scope_complete"
    assert job["dependent_fanout"] == "closure"
    assert job["events"][0]["trigger_kind"] == "reconcile"


@pytest.mark.postgres
def test_poll_backstop_skips_when_dbos_source_sync_is_active(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    (root / "doc.md").write_text("hello\n")
    caps = _stat_tree_caps(root)
    entry = _poll_entry("docs", caps, True, False)
    calls: list[dict[str, object]] = []

    def fake_enqueue_compacted_source_sync_via_dbos(**kwargs):
        calls.append(dict(kwargs))
        return f"wf-source-sync-{len(calls)}"

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "enqueue_compacted_source_sync_via_dbos",
        fake_enqueue_compacted_source_sync_via_dbos,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "active_dbos_source_sync_workflow_status_for_source",
        lambda **_kwargs: {
            "workflow_id": "wf-active",
            "status": "ENQUEUED",
            "age_seconds": 30.0,
            "stale_after_seconds": 300,
            "stale": False,
            "run_mode": "scope_complete",
        },
    )

    first = run_poll_once(
        clean_runtime,
        deployment="test",
        registry={"docs": entry},
        source="docs",
        backstop_multiplier=1,
    )
    assert first["errors"] == []
    assert first["results"][0]["changed"] is True
    assert len(calls) == 1

    stale = datetime.now(timezone.utc) - timedelta(minutes=10)
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.source_watermarks
              (source, last_applied_rev, last_reconcile_at)
            VALUES (%s, %s::jsonb, %s)
            ON CONFLICT (source)
            DO UPDATE SET last_applied_rev = EXCLUDED.last_applied_rev,
                          last_reconcile_at = EXCLUDED.last_reconcile_at
            """,
            ("test.docs", json.dumps({"source": "test.docs"}), stale),
        )

    second = run_poll_once(
        clean_runtime,
        deployment="test",
        registry={"docs": entry},
        source="docs",
        backstop_multiplier=1,
    )

    assert second["errors"] == []
    result = second["results"][0]
    assert result["changed"] is False
    assert result["backstop_reconcile"] is None
    assert len(calls) == 1


@pytest.mark.postgres
def test_poll_backstop_reconciles_when_active_work_is_scope_incomplete(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    (root / "doc.md").write_text("hello\n")
    caps = _scoped_stat_tree_caps(root)
    entry = _poll_entry("docs", caps, True, False)
    calls: list[dict[str, object]] = []
    active_work = {
        "workflow_id": "wf-incomplete",
        "status": "ENQUEUED",
        "age_seconds": 30.0,
        "stale_after_seconds": 300,
        "stale": False,
        "run_mode": "scope_incomplete",
    }

    def fake_enqueue_compacted_source_sync_via_dbos(**kwargs):
        calls.append(dict(kwargs))
        return f"wf-source-sync-{len(calls)}"

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "enqueue_compacted_source_sync_via_dbos",
        fake_enqueue_compacted_source_sync_via_dbos,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "active_dbos_source_sync_workflow_status_for_source",
        lambda **_kwargs: dict(active_work),
    )

    first = run_poll_once(
        clean_runtime,
        deployment="test",
        registry={"docs": entry},
        source="docs",
        backstop_multiplier=1,
    )
    assert first["errors"] == []
    assert first["results"][0]["changed"] is True
    assert len(calls) == 1

    stale = datetime.now(timezone.utc) - timedelta(minutes=10)
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.source_watermarks
              (source, last_applied_rev, last_reconcile_at)
            VALUES (%s, %s::jsonb, %s)
            ON CONFLICT (source)
            DO UPDATE SET last_applied_rev = EXCLUDED.last_applied_rev,
                          last_reconcile_at = EXCLUDED.last_reconcile_at
            """,
            ("test.docs", json.dumps({"source": "test.docs"}), stale),
        )

    second = run_poll_once(
        clean_runtime,
        deployment="test",
        registry={"docs": entry},
        source="docs",
        backstop_multiplier=1,
    )

    assert second["errors"] == []
    result = second["results"][0]
    assert result["changed"] is False
    assert result["backstop_reconcile"]["reason"] == "stale_watermark"
    assert result["backstop_reconcile"]["incompatible_active_workflow"] == active_work
    assert result["backstop_reconcile"]["stale_active_workflow"] is None
    job = calls[-1]["job"].to_dict()
    assert job["run_mode"] == "scope_complete"
    assert job["dependent_fanout"] == "closure"
    assert job["events"][0]["trigger_kind"] == "reconcile"
    assert len(calls) == 2


@pytest.mark.postgres
def test_poll_backstop_reconciles_when_active_dbos_work_is_stale(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    (root / "doc.md").write_text("hello\n")
    caps = _stat_tree_caps(root)
    entry = _poll_entry("docs", caps, True, False)
    calls: list[dict[str, object]] = []

    def fake_enqueue_compacted_source_sync_via_dbos(**kwargs):
        calls.append(dict(kwargs))
        return f"wf-source-sync-{len(calls)}"

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "enqueue_compacted_source_sync_via_dbos",
        fake_enqueue_compacted_source_sync_via_dbos,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "active_dbos_source_sync_workflow_status_for_source",
        lambda **_kwargs: {
            "workflow_id": "wf-stale",
            "status": "ENQUEUED",
            "age_seconds": 700.0,
            "stale_after_seconds": 300,
            "stale": True,
        },
    )

    first = run_poll_once(
        clean_runtime,
        deployment="test",
        registry={"docs": entry},
        source="docs",
        backstop_multiplier=1,
    )
    assert first["errors"] == []
    assert first["results"][0]["changed"] is True
    assert len(calls) == 1

    stale = datetime.now(timezone.utc) - timedelta(minutes=10)
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.source_watermarks
              (source, last_applied_rev, last_reconcile_at)
            VALUES (%s, %s::jsonb, %s)
            ON CONFLICT (source)
            DO UPDATE SET last_applied_rev = EXCLUDED.last_applied_rev,
                          last_reconcile_at = EXCLUDED.last_reconcile_at
            """,
            ("test.docs", json.dumps({"source": "test.docs"}), stale),
        )

    second = run_poll_once(
        clean_runtime,
        deployment="test",
        registry={"docs": entry},
        source="docs",
        backstop_multiplier=1,
    )

    assert second["errors"] == []
    result = second["results"][0]
    assert result["changed"] is False
    assert result["backstop_reconcile"]["reason"] == "stale_watermark"
    assert result["backstop_reconcile"]["stale_active_workflow"] == {
        "workflow_id": "wf-stale",
        "status": "ENQUEUED",
        "age_seconds": 700.0,
        "stale_after_seconds": 300,
        "stale": True,
    }
    assert len(calls) == 2


@pytest.mark.postgres
def test_poll_backstop_can_override_dependent_fanout(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    (root / "doc.md").write_text("hello\n")
    caps = _stat_tree_caps(root)
    entry = _poll_entry("docs", caps, True, False)
    calls: list[dict[str, object]] = []

    def fake_enqueue_compacted_source_sync_via_dbos(**kwargs):
        calls.append(dict(kwargs))
        return f"wf-source-sync-{len(calls)}"

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "enqueue_compacted_source_sync_via_dbos",
        fake_enqueue_compacted_source_sync_via_dbos,
    )

    first = run_poll_once(
        clean_runtime,
        deployment="test",
        registry={"docs": entry},
        source="docs",
        backstop_multiplier=1,
    )
    assert first["errors"] == []

    stale = datetime.now(timezone.utc) - timedelta(minutes=10)
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.source_watermarks
              (source, last_applied_rev, last_reconcile_at)
            VALUES (%s, %s::jsonb, %s)
            ON CONFLICT (source)
            DO UPDATE SET last_applied_rev = EXCLUDED.last_applied_rev,
                          last_reconcile_at = EXCLUDED.last_reconcile_at
            """,
            ("test.docs", json.dumps({"source": "test.docs"}), stale),
        )

    second = run_poll_once(
        clean_runtime,
        deployment="test",
        registry={"docs": entry},
        source="docs",
        backstop_multiplier=1,
        backstop_dependent_fanout="none",
    )

    assert second["errors"] == []
    result = second["results"][0]
    assert result["changed"] is False
    assert result["backstop_reconcile"]["reason"] == "stale_watermark"
    job = calls[-1]["job"].to_dict()
    assert job["run_mode"] == "scope_complete"
    assert job["dependent_fanout"] == "none"
    assert job["events"][0]["trigger_kind"] == "reconcile"


def _ownership_entry(
    sync: SourceSyncCapabilities,
    ownership_id: str,
    *changes: bool,
) -> SimpleNamespace:
    # The registry name and the ownership id differ, as on SubMIT
    # (`slurm` -> `submit-cluster.scheduler.slurm`); `_poll_entry` makes
    # them coincide for deployment "test".
    return SimpleNamespace(
        sync=sync,
        ownership_id=ownership_id,
        poll_changes=list(changes),
    )


@pytest.mark.postgres
def test_poll_backstop_reads_the_watermark_under_the_ownership_id(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A fresh watermark under the ownership id is found (#3295).

    The poll looked the watermark up as `<deployment>.<name>` or `<name>`,
    so a source whose ownership id has another shape always read
    `missing_watermark` and was re-synced on every poll. A stale row under
    the name-shaped key must not be read in its place.
    """
    root = tmp_path / "repo"
    root.mkdir()
    (root / "doc.md").write_text("hello\n")
    caps = _scoped_stat_tree_caps(root)
    stale = datetime.now(timezone.utc) - timedelta(days=1)
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.source_watermarks
              (source, last_applied_rev, last_reconcile_at)
            VALUES (%s, '{}'::jsonb, now()), (%s, '{}'::jsonb, %s)
            """,
            ("acme.scheduler.slurm", "test.slurm", stale),
        )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "enqueue_compacted_source_sync_via_dbos",
        lambda **_kwargs: pytest.fail("fresh source was re-synced"),
    )

    out = run_poll_once(
        clean_runtime,
        deployment="test",
        registry={
            "slurm": _ownership_entry(caps, "acme.scheduler.slurm", False),
        },
        source="slurm",
    )

    assert out["errors"] == []
    result = out["results"][0]
    assert result["changed"] is False
    assert result["backstop_reconcile"] is None



@pytest.mark.postgres
def test_poll_backstop_does_not_read_a_name_keyed_watermark(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only the ownership id is looked up: a fresh row under `test.slurm`
    may belong to another source and must not stand in for this one's."""
    root = tmp_path / "repo"
    root.mkdir()
    (root / "doc.md").write_text("hello\n")
    caps = _scoped_stat_tree_caps(root)
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.source_watermarks
              (source, last_applied_rev, last_reconcile_at)
            VALUES ('test.slurm', '{}'::jsonb, now()), ('slurm', '{}'::jsonb, now())
            """
        )
    calls: list[dict[str, object]] = []

    def fake_enqueue_compacted_source_sync_via_dbos(**kwargs):
        calls.append(dict(kwargs))
        return "wf-source-sync"

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "enqueue_compacted_source_sync_via_dbos",
        fake_enqueue_compacted_source_sync_via_dbos,
    )

    out = run_poll_once(
        clean_runtime,
        deployment="test",
        registry={
            "slurm": _ownership_entry(caps, "acme.scheduler.slurm", False),
        },
        source="slurm",
    )

    assert out["errors"] == []
    result = out["results"][0]
    assert result["backstop_reconcile"]["reason"] == "missing_watermark"
    assert result["backstop_reconcile"]["source"] == "acme.scheduler.slurm"
    assert calls[-1]["job"].to_dict()["events"][0]["trigger_kind"] == (
        "reconcile"
    )

@pytest.mark.postgres
def test_watch_receipt_under_the_ownership_id_advances_the_poll_baseline(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The runner's `sync.last_run` and watermark use the ownership id (#3295)."""
    root = tmp_path / "repo"
    root.mkdir()
    (root / "doc.md").write_text("after\n")
    caps = _watch_scoped_stat_tree_caps(root)
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.source_progress
              (deployment, source_name, key, value, updated_at)
            VALUES (
              'test', 'docs', 'sync.poll.change_probe.fingerprint',
              'prior-change-probe-baseline', now() - interval '5 minutes'
            ), (
              'test', 'docs', 'sync.poll.change_probe.suppressed_observation',
              %s, now() - interval '1 minute'
            ), (
              'test', 'acme.docs.guide', 'sync.last_run',
              jsonb_build_object(
                'workflow_id', 'wf-watch-complete',
                'finished_at', now()::text
              ), now()
            )
            """,
            (
                json.dumps({
                    "schema": "okg.source_sync.poll_suppression.v1",
                    "workflow_id": "wf-watch-complete",
                    "revision_hint": "test-revision-1",
                    "effective_token": _test_effective_token("1"),
                }),
            ),
        )
        cur.execute(
            """
            INSERT INTO okg.source_watermarks
              (source, last_applied_rev, last_reconcile_at)
            VALUES ('acme.docs.guide', %s::jsonb, now())
            """,
            (json.dumps({"change_probe_token": _test_effective_token("1")}),),
        )
    monkeypatch.setattr(
        "okg.substrate.source_sync_producers."
        "_dbos_watch_workflow_completed_after",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "active_dbos_source_sync_workflow_status_for_source",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "enqueue_compacted_source_sync_via_dbos",
        lambda **_kwargs: pytest.fail("completed watch change was double-fired"),
    )

    out = run_poll_once(
        clean_runtime,
        deployment="test",
        registry={"docs": _ownership_entry(caps, "acme.docs.guide", True)},
        source="docs",
    )

    assert out["errors"] == []
    assert out["results"][0]["poll_baseline_advanced"]["reason"] == (
        "watch_backed_source_run"
    )
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cursor = cur.execute(
            "SELECT last_applied_rev FROM okg.source_watermarks "
            "WHERE source = 'acme.docs.guide'"
        ).fetchone()[0]
    assert cursor["change_probe_token"] == _test_effective_token("1")
