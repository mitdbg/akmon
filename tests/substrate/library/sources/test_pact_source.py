from __future__ import annotations

from pathlib import Path

import yaml

from okg.substrate.library.sources.base import NodeFact


def _write_pact(root: Path, change_id: str, *, title: str | None = None) -> Path:
    path = root / "changes" / change_id / "pact.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump({
            "pact_version": 0,
            "change": {
                "id": change_id,
                "title": title or change_id,
                "rationale": "test pact",
            },
            "requirements": [{
                "id": f"req.{change_id}",
                "statement": "test requirement",
            }],
            "tasks": [{
                "id": f"task.{change_id}",
                "title": "test task",
                "verification": "test verification",
            }],
        }),
        encoding="utf-8",
    )
    return path


def test_pact_source_full_run_declares_record_set(tmp_path: Path) -> None:
    from okg.substrate.library.sources.pact import PACTSource

    pact_root = tmp_path / "pact"
    _write_pact(pact_root, "alpha")
    _write_pact(pact_root, "beta")

    run = PACTSource(repo="demo", pact_root=pact_root).run(
        "run-1",
        mode="scope_complete",
    )
    facts = list(run.facts)

    assert run.completed_scope is True
    assert run.record_set is not None
    assert run.next_cursor is not None
    assert run.next_cursor["fact_hashes"]
    assert {"__cap__:pact", "alpha", "beta"} <= set(run.record_set.records)
    assert any(
        isinstance(fact, NodeFact) and fact.node_id == "cap:pact"
        for fact in facts
    )

    repeated = PACTSource(repo="demo", pact_root=pact_root).run(
        "different-acquisition",
        mode="scope_complete",
    )
    repeated_facts = list(repeated.facts)
    assert repeated_facts == facts
    assert all("run_id" not in fact.source_revision for fact in facts)


def test_pact_coordination_event_revision_uses_row_identity(
    monkeypatch,
) -> None:
    from okg.substrate.library.sources.pact import PACTCoordinationEventSource

    row = {
        "event_id": 17,
        "change_id": "alpha",
        "event_kind": "task_claimed",
        "task_id": "task.alpha",
        "occurred_at": "2026-07-25T12:00:00Z",
        "payload": {},
    }
    source = PACTCoordinationEventSource(repo="demo")
    identity = "17:1:sha256-exact-row-snapshot"
    monkeypatch.setattr(
        source,
        "_load_ledger_coordination_event_rows",
        lambda: (
            [row],
            {
                "status": "ok",
                "watermark": identity,
                "identity": identity,
            },
        ),
    )

    first_run = source.run("acquisition-a")
    first = list(first_run.facts)
    second_run = source.run("acquisition-b")
    second = list(second_run.facts)

    assert second == first
    assert len(first) == 1
    assert "run_id" not in first[0].source_revision
    assert first_run.applied_token_scope == "global"
    assert first_run.applied_token_authority == "exact"
    assert first_run.applied_raw_token == source.change_probe.token_for_value(
        identity
    )

    replay = source.run(
        "authoritative-replay",
        cursor={"max_event_id": 17},
        authoritative_replay=True,
    )
    assert list(replay.facts) == first
    assert replay.completed_scope is True
    assert replay.record_set is not None
    assert set(replay.record_set.records) == {"17"}


def test_pact_source_scoped_complete_merges_prior_record_set_and_removes_deleted_change(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.pact import PACTSource

    pact_root = tmp_path / "pact"
    _write_pact(pact_root, "alpha")
    beta = _write_pact(pact_root, "beta")
    source = PACTSource(repo="demo", pact_root=pact_root)
    baseline = source.run("run-1", mode="scope_complete")
    list(baseline.facts)
    assert baseline.record_set is not None

    beta.unlink()
    scoped = source.run(
        "run-2",
        mode="scope_complete",
        cursor={"record_set": baseline.record_set.to_cursor()},
        sync_scope={"scope_keys": ["pact/changes/beta/pact.yaml"]},
    )
    scoped_facts = list(scoped.facts)

    assert scoped.completed_scope is True
    assert scoped.record_set is not None
    assert "alpha" in scoped.record_set.records
    assert "beta" not in scoped.record_set.records
    assert "__cap__:pact" in scoped.record_set.records
    assert not any(
        isinstance(fact, NodeFact) and fact.node_id == "cap:pact"
        for fact in scoped_facts
    )


def test_pact_source_unrecognized_scoped_paths_fall_back_to_full_read(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.pact import PACTSource

    pact_root = tmp_path / "pact"
    _write_pact(pact_root, "alpha")
    _write_pact(pact_root, "beta")
    source = PACTSource(repo="demo", pact_root=pact_root)

    run = source.run(
        "run-1",
        mode="scope_incomplete",
        sync_scope={"scope_keys": ["pact/project.yaml"]},
    )

    facts = list(run.facts)
    assert {
        fact.node_id
        for fact in facts
        if isinstance(fact, NodeFact) and fact.node_id.startswith("pact-change:")
    } == {"pact-change:alpha", "pact-change:beta"}
    assert run.completed_scope is False
    assert run.record_set is None
    assert run.applied_raw_token is None
    assert run.applied_token_authority == "unavailable"
    assert source.last_run_stats["scope_status"] == "full"
    assert source.last_run_stats["scope_keys"] == 1
    assert source.last_run_stats["recognized_changes"] == 0
    assert source.last_run_stats["manifests_loaded"] == 2
    assert source.last_run_stats["facts_emitted"] == len(facts)


def test_pact_source_unchanged_unscoped_run_rederives_facts_deterministically(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.pact import PACTSource

    pact_root = tmp_path / "pact"
    _write_pact(pact_root, "alpha")
    _write_pact(pact_root, "beta")
    source = PACTSource(repo="demo", pact_root=pact_root)

    first = source.run("run-1", mode="cursor")
    first_facts = list(first.facts)
    assert first.next_cursor is not None
    assert first.next_cursor["manifest_hashes"]

    second = source.run("run-2", mode="cursor", cursor=first.next_cursor)
    second_facts = list(second.facts)

    assert second_facts == first_facts
    assert second.completed_scope is False
    assert second.next_cursor == first.next_cursor
    assert second.applied_raw_token is None
    assert source.last_run_stats["scope_status"] == "full"
    assert source.last_run_stats["manifests_loaded"] == 2
    assert source.last_run_stats["facts_emitted"] == len(second_facts)


def test_pact_source_unchanged_complete_run_rebuilds_record_set(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.pact import PACTSource

    pact_root = tmp_path / "pact"
    _write_pact(pact_root, "alpha")
    source = PACTSource(repo="demo", pact_root=pact_root)
    first = source.run("run-1", mode="cursor")
    list(first.facts)
    assert first.next_cursor is not None

    cursor = {
        **first.next_cursor,
        "record_set_ref": {
            "storage": "okg.source_record_set_blobs",
            "sha256": "abc",
        },
        "record_set_summary": {"record_count": 2},
    }
    second = source.run("run-2", mode="scope_complete", cursor=cursor)
    second_facts = list(second.facts)

    assert second_facts
    assert second.completed_scope is True
    assert second.record_set is not None
    assert {"__cap__:pact", "alpha"} <= set(second.record_set.records)
    assert second.next_cursor == first.next_cursor


def test_pact_no_change_rebuilds_record_set_authority(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.pact import PACTSource

    pact_root = tmp_path / "pact"
    _write_pact(pact_root, "alpha")
    _write_pact(pact_root, "beta")
    source = PACTSource(repo="demo", pact_root=pact_root)
    baseline = source.run("run-1", mode="scope_complete")
    list(baseline.facts)
    assert baseline.next_cursor is not None

    cursor = {
        **baseline.next_cursor,
        "record_set_ref": {
            "storage": "okg.source_record_set_blobs",
            "sha256": "abc",
        },
        "record_set_summary": {"record_count": 3},
    }
    unchanged = source.run("run-2", mode="scope_complete", cursor=cursor)
    unchanged_facts = list(unchanged.facts)

    assert unchanged_facts
    assert unchanged.completed_scope is True
    assert unchanged.record_set is not None
    assert {"__cap__:pact", "alpha", "beta"} <= set(
        unchanged.record_set.records
    )
    assert unchanged.next_cursor == baseline.next_cursor
    assert source.last_run_stats["scope_status"] == "full"
    assert source.last_run_stats["record_set_authority"] == "rebuilt"
    assert source.last_run_stats["facts_emitted"] == len(unchanged_facts)


def test_pact_missing_record_set_authority_repairs_with_full_record_set(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.pact import PACTSource

    pact_root = tmp_path / "pact"
    _write_pact(pact_root, "alpha")
    source = PACTSource(repo="demo", pact_root=pact_root)
    baseline = source.run("run-1", mode="cursor")
    list(baseline.facts)
    assert baseline.next_cursor is not None
    assert "record_set" not in baseline.next_cursor
    assert "record_set_ref" not in baseline.next_cursor

    repair = source.run(
        "run-2",
        mode="scope_complete",
        cursor=baseline.next_cursor,
    )
    repair_facts = list(repair.facts)

    assert repair.completed_scope is True
    assert repair.record_set is not None
    assert repair_facts
    assert "alpha" in repair.record_set.records
    assert source.last_run_stats["scope_status"] == (
        "record_set_authority_repair"
    )
    assert source.last_run_stats["record_set_authority"] == "repair_emitted"
    assert source.last_run_stats["facts_emitted"] == len(repair_facts)


def test_pact_source_scoped_run_loads_only_recognized_change(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.pact import PACTSource

    pact_root = tmp_path / "pact"
    _write_pact(pact_root, "alpha")
    _write_pact(pact_root, "beta")
    source = PACTSource(repo="demo", pact_root=pact_root)

    run = source.run(
        "run-1",
        mode="scope_incomplete",
        sync_scope={"scope_keys": ["pact/changes/beta/pact.yaml"]},
    )
    facts = list(run.facts)

    assert any(
        isinstance(fact, NodeFact)
        and fact.node_id == "pact-change:beta"
        for fact in facts
    )
    assert not any(
        isinstance(fact, NodeFact)
        and fact.node_id == "pact-change:alpha"
        for fact in facts
    )
    assert source.last_run_stats["scope_status"] == "scoped"
    assert source.last_run_stats["recognized_changes"] == 1
    assert source.last_run_stats["manifests_loaded"] == 1


def test_pact_source_scoped_run_filters_unchanged_facts_with_hash_cursor(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.pact import PACTSource

    pact_root = tmp_path / "pact"
    _write_pact(pact_root, "alpha")
    source = PACTSource(repo="demo", pact_root=pact_root)

    baseline = source.run("run-1", mode="scope_complete")
    baseline_facts = list(baseline.facts)
    assert baseline.record_set is not None
    assert baseline.next_cursor is not None
    assert any(
        fact.source_record_id.get("change_id") == "alpha"
        for fact in baseline_facts
    )
    cursor = {
        **baseline.next_cursor,
        "record_set": baseline.record_set.to_cursor(),
    }

    scoped = source.run(
        "run-2",
        mode="scope_complete",
        cursor=cursor,
        sync_scope={"scope_keys": ["pact/changes/alpha/pact.yaml"]},
    )

    assert list(scoped.facts) == []
    assert scoped.completed_scope is True
    assert scoped.record_set is not None
    assert scoped.next_cursor is not None
    assert scoped.next_cursor["fact_hashes"] == baseline.next_cursor["fact_hashes"]
    assert {"__cap__:pact", "alpha"} <= set(scoped.record_set.records)
    assert source.last_run_stats["scope_status"] == "scoped"
    assert source.last_run_stats["manifests_loaded"] == 1
    assert source.last_run_stats["facts_evaluated"] > 0
    assert source.last_run_stats["facts_emitted"] == 0
    assert source.last_run_stats["facts_filtered_unchanged"] == (
        source.last_run_stats["facts_evaluated"]
    )


def test_pact_source_scoped_run_emits_only_changed_facts_with_hash_cursor(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.pact import PACTSource

    pact_root = tmp_path / "pact"
    _write_pact(pact_root, "alpha")
    source = PACTSource(repo="demo", pact_root=pact_root)

    baseline = source.run("run-1", mode="scope_complete")
    baseline_facts = list(baseline.facts)
    assert baseline.record_set is not None
    assert baseline.next_cursor is not None
    scoped_fact_count = sum(
        1
        for fact in baseline_facts
        if fact.source_record_id.get("change_id") == "alpha"
    )

    _write_pact(pact_root, "alpha", title="renamed alpha")
    scoped = source.run(
        "run-2",
        mode="scope_complete",
        cursor={
            **baseline.next_cursor,
            "record_set": baseline.record_set.to_cursor(),
        },
        sync_scope={"scope_keys": ["pact/changes/alpha/pact.yaml"]},
    )
    facts = list(scoped.facts)

    assert 0 < len(facts) < scoped_fact_count
    assert any(
        isinstance(fact, NodeFact)
        and fact.node_id == "pact-change:alpha"
        for fact in facts
    )
    assert scoped.completed_scope is True
    assert source.last_run_stats["facts_evaluated"] == scoped_fact_count
    assert source.last_run_stats["facts_emitted"] == len(facts)
    assert source.last_run_stats["facts_filtered_unchanged"] > 0


def test_pact_source_skips_dangling_supersession_targets(tmp_path: Path) -> None:
    from okg.substrate.library.sources.base import EdgeFact
    from okg.substrate.library.sources.pact import PACTSource

    pact_root = tmp_path / "pact"
    _write_pact(pact_root, "alpha")
    _write_pact(pact_root, "beta")
    manifest_path = pact_root / "changes" / "alpha" / "pact.yaml"
    raw = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    # "ghost-change" matches the prose regex but has no manifest; "beta"
    # does. Only the beta edge may project — a dangling edge fails
    # producer-signature admission for the whole run.
    raw["change"]["rationale"] = (
        "This PACT supersedes the ghost-change half of older work. "
        "Superseded by beta."
    )
    manifest_path.write_text(yaml.safe_dump(raw), encoding="utf-8")

    source = PACTSource(repo="demo", pact_root=pact_root)
    run = source.run("run-1", mode="scope_complete")
    edges = [
        fact for fact in run.facts
        if isinstance(fact, EdgeFact) and fact.edge_type == "supersedes"
    ]

    assert {(e.src, e.dst) for e in edges} == {
        ("pact-change:beta", "pact-change:alpha"),
    }
    assert source.last_run_stats["supersession_targets_skipped"] == [
        {"change": "alpha", "target": "ghost-change", "basis": "prose_supersedes"},
    ]
