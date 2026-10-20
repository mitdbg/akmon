"""Correctness-first replacement epochs for mutable Codex transcripts."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.library.sources.base import NodeFact
from okg.substrate.library.sources.change_probe import ProbeVerdict
from okg.substrate.library.sources.codex_sessions import CodexSessionsSource


def _rows(repo: Path, session_id: str, turns: int = 8) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = [{
        "type": "session_meta",
        "timestamp": "2026-07-25T00:00:00Z",
        "payload": {
            "id": session_id,
            "cwd": str(repo),
            "model": "gpt-5",
            "timestamp": "2026-07-25T00:00:00Z",
        },
    }]
    for turn in range(turns):
        rows.append({
            "type": "response_item",
            "timestamp": f"2026-07-25T00:00:{turn + 1:02d}Z",
            "payload": {
                "type": "message",
                "role": "assistant",
                "content": f"turn {turn} " + ("x" * 300),
            },
        })
    return rows


def _write(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n",
        encoding="utf-8",
    )


def _source(
    repo: Path,
    root: Path,
    *,
    max_transcript_bytes: int = 25_000_000,
) -> CodexSessionsSource:
    return CodexSessionsSource(
        repo="okg",
        codex_sessions_root=root,
        archived_sessions_root=None,
        cwd_filter=[repo],
        since="7d",
        max_transcript_bytes=max_transcript_bytes,
    )


def _drain(
    source: CodexSessionsSource,
    cursor: dict[str, Any],
) -> tuple[list[Any], dict[str, Any]]:
    facts: list[Any] = []
    for run_no in range(1, 30):
        run = source.run(
            f"drain-{run_no}",
            mode="scope_complete",
            cursor=cursor,
        )
        facts.extend(list(run.facts))
        cursor = dict(run.next_cursor or {})
        if not cursor.get("outstanding_backlog"):
            return facts, cursor
    raise AssertionError("replacement epoch did not converge")


def _node_ops(facts: list[Any], op: str) -> set[str]:
    return {
        fact.node_id
        for fact in facts
        if isinstance(fact, NodeFact) and fact.op == op
    }


def _footprint_node_ids(raw: dict[str, Any] | None) -> set[str]:
    if not isinstance(raw, dict):
        return set()
    out = {
        str(item["node_id"])
        for item in raw.get("descendants") or ()
        if isinstance(item, dict) and item.get("node_id")
    }
    primary = raw.get("primary")
    if isinstance(primary, dict) and primary.get("node_id"):
        out.add(str(primary["node_id"]))
    return out


def test_same_size_rewrite_preserves_old_ownership_until_epoch_completes(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    root = tmp_path / "sessions"
    path = root / "rollout.jsonl"
    _write(path, _rows(repo, "session-old"))

    baseline = _source(repo, root).run("baseline", cursor={})
    list(baseline.facts)
    cursor = dict(baseline.next_cursor or {})
    old_entry = cursor["transcripts"][str(path)]
    old_owned = _footprint_node_ids(old_entry["owned_footprint"])
    assert "session:codex:session-old" in old_owned

    old_stat = path.stat()
    old_size = old_stat.st_size
    _write(path, _rows(repo, "session-new"))
    assert path.stat().st_size == old_size
    os.utime(path, ns=(old_stat.st_atime_ns, old_stat.st_mtime_ns))

    source = _source(repo, root, max_transcript_bytes=1)
    first = source.run(
        "replacement-1",
        mode="scope_complete",
        cursor=cursor,
        changed_transcript_keys={f"{root}::rollout.jsonl"},
    )
    first_facts = list(first.facts)
    partial = dict(first.next_cursor or {})
    epoch = partial["replacement_epochs"][str(path)]

    assert partial.get("outstanding_backlog")
    assert not _node_ops(first_facts, "R")
    assert "session:codex:session-old" in _footprint_node_ids(epoch["old"])
    assert "session:codex:session-new" in _footprint_node_ids(epoch["current"])

    final_facts, final_cursor = _drain(source, partial)
    retracted = _node_ops(final_facts, "R")

    assert "session:codex:session-old" in retracted
    assert "session:codex:session-new" not in retracted
    assert not final_cursor["replacement_epochs"]
    final_owned = _footprint_node_ids(
        final_cursor["transcripts"][str(path)]["owned_footprint"],
    )
    assert "session:codex:session-new" in final_owned
    assert "session:codex:session-old" not in final_owned


def test_second_mutation_keeps_abandoned_epoch_cleanup_authority(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    root = tmp_path / "sessions"
    path = root / "rollout.jsonl"
    _write(path, _rows(repo, "session-old"))

    baseline = _source(repo, root).run("baseline", cursor={})
    list(baseline.facts)
    cursor = dict(baseline.next_cursor or {})

    _write(path, _rows(repo, "session-one"))
    source = _source(repo, root, max_transcript_bytes=1)
    first = source.run(
        "replacement-one",
        mode="scope_complete",
        cursor=cursor,
        changed_transcript_keys={str(path)},
    )
    list(first.facts)
    cursor = dict(first.next_cursor or {})

    _write(path, _rows(repo, "session-two"))
    second = source.run(
        "replacement-two",
        mode="scope_complete",
        cursor=cursor,
        changed_transcript_keys={str(path)},
    )
    second_facts = list(second.facts)
    cursor = dict(second.next_cursor or {})
    epoch = cursor["replacement_epochs"][str(path)]

    assert epoch["sequence"] == 2
    assert not _node_ops(second_facts, "R")
    assert "session:codex:session-old" in _footprint_node_ids(epoch["old"])
    assert "session:codex:session-one" in _footprint_node_ids(epoch["cleanup"])
    assert "session:codex:session-two" in _footprint_node_ids(epoch["current"])

    final_facts, final_cursor = _drain(source, cursor)
    retracted = _node_ops(final_facts, "R")

    assert "session:codex:session-old" in retracted
    assert "session:codex:session-one" in retracted
    assert "session:codex:session-two" not in retracted
    assert not final_cursor["replacement_epochs"]


def test_evaluated_empty_identity_ignores_timestamp_only_churn(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    root = tmp_path / "sessions"
    path = root / "rollout.jsonl"
    _write(path, _rows(repo, "session-old"))

    source = _source(repo, root)
    baseline = source.run("baseline", cursor={})
    list(baseline.facts)
    cursor = dict(baseline.next_cursor or {})
    stat = path.stat()
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000))

    unchanged = source.run(
        "mtime-only",
        mode="scope_complete",
        cursor=cursor,
    )
    facts = list(unchanged.facts)

    assert not _node_ops(facts, "R")
    assert not unchanged.next_cursor["replacement_epochs"]
    assert source.last_run_stats["replacement_epochs_completed"] == 0
    assert source.last_run_stats["files_skipped_unchanged"] == 1
    assert source.last_run_stats["full_parse_files"] == 0
    assert source.last_run_stats["append_delta_files"] == 0


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ("session-old", "session-new"),
        ("turn 3 ", "TURN 3 "),
        ("turn 7 ", "TURN 7 "),
    ],
    ids=["prefix", "middle", "tail"],
)
def test_complete_identity_invalidates_same_size_rewrites(
    tmp_path: Path,
    old: str,
    new: str,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    root = tmp_path / "sessions"
    path = root / "rollout.jsonl"
    _write(path, _rows(repo, "session-old"))

    source = _source(repo, root)
    baseline = source.run("baseline", cursor={})
    list(baseline.facts)
    cursor = dict(baseline.next_cursor or {})
    before = path.read_text(encoding="utf-8")
    stat = path.stat()
    path.write_text(before.replace(old, new, 1), encoding="utf-8")
    assert path.stat().st_size == stat.st_size
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns))

    rewritten = source.run(
        f"rewrite-{old}",
        mode="scope_complete",
        cursor=cursor,
    )
    list(rewritten.facts)

    assert source.last_run_stats["full_parse_files"] == 1
    assert source.last_run_stats["append_delta_files"] == 0
    assert source.last_run_stats["replacement_epochs_completed"] == 1
    assert not rewritten.next_cursor["replacement_epochs"]


def test_complete_identity_rejects_rewritten_prefix_plus_append(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    root = tmp_path / "sessions"
    path = root / "rollout.jsonl"
    _write(path, _rows(repo, "session-old"))

    source = _source(repo, root)
    baseline = source.run("baseline", cursor={})
    list(baseline.facts)
    cursor = dict(baseline.next_cursor or {})
    before = path.read_text(encoding="utf-8")
    appended = _rows(repo, "unused", turns=1)[-1]
    path.write_text(
        before.replace("session-old", "session-new", 1)
        + json.dumps(appended)
        + "\n",
        encoding="utf-8",
    )

    rewritten = source.run(
        "rewrite-plus-append",
        mode="scope_complete",
        cursor=cursor,
    )
    list(rewritten.facts)

    assert source.last_run_stats["full_parse_files"] == 1
    assert source.last_run_stats["append_delta_files"] == 0
    assert source.last_run_stats["replacement_epochs_completed"] == 1
    assert not rewritten.next_cursor["replacement_epochs"]


def test_scoped_rewrite_plus_append_invalidates_codex_resume(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    root = tmp_path / "sessions"
    path = root / "rollout.jsonl"
    _write(path, _rows(repo, "session-old"))

    source = _source(repo, root)
    baseline = source.run("baseline", cursor={})
    list(baseline.facts)
    cursor = dict(baseline.next_cursor or {})
    appended = _rows(repo, "unused", turns=1)[-1]
    path.write_text(
        path.read_text(encoding="utf-8").replace(
            "session-old",
            "session-new",
            1,
        )
        + json.dumps(appended)
        + "\n",
        encoding="utf-8",
    )

    rewritten = source.run(
        "scoped-rewrite",
        mode="cursor",
        cursor=cursor,
        sync_scope={"scope_keys": [path.name]},
    )
    list(rewritten.facts)

    assert source.last_run_stats["full_parse_files"] == 1
    assert source.last_run_stats["append_delta_files"] == 0
    assert rewritten.next_cursor["replacement_epochs"]


def test_complete_identity_invalidates_codex_truncation(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    root = tmp_path / "sessions"
    path = root / "rollout.jsonl"
    _write(path, _rows(repo, "session-old"))

    source = _source(repo, root)
    baseline = source.run("baseline", cursor={})
    list(baseline.facts)
    cursor = dict(baseline.next_cursor or {})
    _write(path, _rows(repo, "session-old", turns=2))

    truncated = source.run(
        "truncation",
        mode="scope_complete",
        cursor=cursor,
    )
    list(truncated.facts)

    assert source.last_run_stats["full_parse_files"] == 1
    assert source.last_run_stats["append_delta_files"] == 0
    assert source.last_run_stats["replacement_epochs_completed"] == 1


def test_composite_probe_error_cannot_reuse_cached_codex_manifest(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    root = tmp_path / "sessions"
    path = root / "rollout.jsonl"
    _write(path, _rows(repo, "session-old"))

    source = _source(repo, root)
    baseline = source.run("baseline", cursor={})
    list(baseline.facts)
    cursor = dict(baseline.next_cursor or {})
    stat = path.stat()
    path.write_text(
        path.read_text(encoding="utf-8").replace(
            "session-old",
            "session-new",
            1,
        ),
        encoding="utf-8",
    )
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns))

    rewritten = source.run(
        "probe-error",
        mode="scope_complete",
        cursor=cursor,
        change_probe_verdict=ProbeVerdict(
            changed=True,
            token={},
            reason="probe_error:ChangeProbeError",
        ),
    )
    list(rewritten.facts)

    assert source.last_run_stats["full_parse_files"] == 1
    assert source.last_run_stats["append_delta_files"] == 0
    assert source.last_run_stats["replacement_epochs_completed"] == 1


def test_complete_identity_preserves_verified_pure_append_resume(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    root = tmp_path / "sessions"
    path = root / "rollout.jsonl"
    _write(path, _rows(repo, "session-old"))

    source = _source(repo, root)
    baseline = source.run("baseline", cursor={})
    list(baseline.facts)
    cursor = dict(baseline.next_cursor or {})
    appended = _rows(repo, "unused", turns=1)[-1]
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(appended) + "\n")

    incremental = source.run(
        "pure-append",
        mode="scope_complete",
        cursor=cursor,
    )
    list(incremental.facts)

    assert source.last_run_stats["append_delta_files"] == 1
    assert source.last_run_stats["full_parse_files"] == 0
    assert source.last_run_stats["replacement_epochs_completed"] == 0
    assert not incremental.next_cursor["replacement_epochs"]


def test_legacy_bounded_cursor_rebuilds_per_transcript_ownership(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    root = tmp_path / "sessions"
    path = root / "rollout.jsonl"
    _write(path, _rows(repo, "session-old"))

    source = _source(repo, root)
    baseline = source.run("baseline", cursor={})
    list(baseline.facts)
    cursor = dict(baseline.next_cursor or {})
    # Production since=7d cursors written before this change have parse state
    # but no record-set or per-transcript ownership authority.
    cursor["transcripts"][str(path)].pop("owned_footprint")

    migrated = source.run(
        "ownership-migration",
        mode="scope_complete",
        cursor=cursor,
        changed_transcript_keys=set(),
    )
    facts = list(migrated.facts)
    owned = _footprint_node_ids(
        migrated.next_cursor["transcripts"][str(path)]["owned_footprint"],
    )

    assert "session:codex:session-old" in owned
    assert not _node_ops(facts, "R")
    assert not migrated.next_cursor["replacement_epochs"]


@pytest.mark.parametrize("completion_mode", ["scope_complete", "reconcile"])
def test_scope_incomplete_defers_retractions_until_completed_scope(
    tmp_path: Path,
    completion_mode: str,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    root = tmp_path / "sessions"
    path = root / "rollout.jsonl"
    _write(path, _rows(repo, "session-old"))

    baseline = _source(repo, root).run("baseline", cursor={})
    list(baseline.facts)
    cursor = dict(baseline.next_cursor or {})
    _write(path, _rows(repo, "session-new"))

    source = _source(repo, root)
    incomplete = source.run(
        "incomplete",
        mode="scope_incomplete",
        cursor=cursor,
        changed_transcript_keys={str(path)},
    )
    incomplete_facts = list(incomplete.facts)
    cursor = dict(incomplete.next_cursor or {})

    assert not _node_ops(incomplete_facts, "R")
    assert cursor["replacement_epochs"]
    assert cursor["outstanding_backlog"] == [{
        "path": str(path),
        "consumed_bytes": path.stat().st_size,
        "size_bytes": path.stat().st_size,
    }]
    assert source.last_run_stats[
        "replacement_epochs_ready_for_completed_scope"
    ] == 1

    completed = source.run(
        "complete",
        mode=completion_mode,
        cursor=cursor,
        changed_transcript_keys=set(),
    )
    completed_facts = list(completed.facts)

    assert "session:codex:session-old" in _node_ops(completed_facts, "R")
    assert not completed.next_cursor["replacement_epochs"]
