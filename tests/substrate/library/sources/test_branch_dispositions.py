from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

from okg.substrate.library.sources.branch_dispositions import (
    BranchDispositionError,
    BranchDispositionsSource,
    default_register_path,
    normalize_disposition,
)
from okg.substrate.library.sources.change_probe import tokens_equal
from okg.substrate.library.sources.content_hash_probe import ContentHashProbe


def test_branch_dispositions_content_hash_probe_churn_immune_and_edit(tmp_path):
    reg = tmp_path / "register.yaml"
    reg.write_text("dispositions: []\n")
    assert BranchDispositionsSource.change_probe_kind == "content_hash"
    src = BranchDispositionsSource(repo="okg", register_path=str(reg))
    assert isinstance(src.change_probe, ContentHashProbe)
    t0 = src.change_probe.build_token()
    os.utime(reg, (2_000_000_000, 2_000_000_000))
    assert tokens_equal(t0, src.change_probe.build_token())  # mtime churn-immune
    reg.write_text("dispositions: [{}]\n")
    assert not tokens_equal(t0, src.change_probe.build_token())  # edit detected


def test_archive_with_summary_refuses_missing_summary() -> None:
    with pytest.raises(BranchDispositionError, match="without a non-empty summary"):
        normalize_disposition(
            {
                "branch_name": "research/empty-archive",
                "state": "archive_with_summary",
            },
            repo="okg",
        )


def test_kill_refuses_missing_reason() -> None:
    with pytest.raises(BranchDispositionError, match="cannot be kill without a reason"):
        normalize_disposition(
            {
                "branch_name": "research/delete-me",
                "state": "kill",
            },
            repo="okg",
        )


def test_default_register_path_is_repo_local_okg(tmp_path: Path) -> None:
    assert default_register_path(tmp_path) == (
        tmp_path / ".okg" / "research_branch_dispositions.yaml"
    )


def test_branch_dispositions_source_emits_summary_node_and_edge(tmp_path: Path) -> None:
    register = tmp_path / "research_branch_dispositions.yaml"
    register.write_text(yaml.safe_dump({
        "dispositions": [
            {
                "branch_name": "research/archive-me",
                "state": "archive_with_summary",
                "origin_session_id": "session-123",
                "summary": {
                    "body": "This branch recorded a negative result.",
                    "findings": ["Do not merge the runner layout."],
                },
            }
        ]
    }))
    source = BranchDispositionsSource(repo="okg", register_path=register)

    run = source.run("test-run", mode="scope_complete")
    facts = list(run.facts)

    assert {fact.subtype for fact in facts if hasattr(fact, "subtype")} == {
        "branch_disposition",
        "branch_archive_summary",
    }
    assert any(
        fact.edge_type == "references"
        and fact.src.startswith("branch-disposition:okg:")
        and fact.dst.startswith("branch-archive-summary:okg:")
        for fact in facts
        if hasattr(fact, "edge_type")
    )
    summary = next(
        fact
        for fact in facts
        if getattr(fact, "subtype", None) == "branch_archive_summary"
    )
    assert summary.attrs["branch_name"] == "research/archive-me"
    assert summary.attrs["origin_session_id"] == "session-123"
    assert run.record_set is not None
    assert len(run.record_set.records) == 1


def test_branch_disposition_facts_use_only_register_time_and_content(
    tmp_path: Path,
) -> None:
    register = tmp_path / "research_branch_dispositions.yaml"
    register.write_text(yaml.safe_dump({
        "dispositions": [
            {
                "branch_name": "research/check-me",
                "state": "merge",
            },
            {
                "branch_name": "research/dated",
                "state": "merge",
                "checked_at": "2026-07-25T12:00:00Z",
            },
        ]
    }))
    source = BranchDispositionsSource(repo="okg", register_path=register)

    first_run = source.run("acquisition-a", mode="scope_complete")
    first = list(first_run.facts)
    second_run = source.run("acquisition-b", mode="scope_complete")
    second = list(second_run.facts)

    assert second == first
    assert first_run.applied_token_scope == "global"
    assert first_run.applied_token_authority == "exact"
    assert first_run.applied_raw_token == source.change_probe.build_token()
    assert second_run.applied_raw_token == source.change_probe.build_token()
    by_branch = {
        fact.attrs["branch_name"]: fact
        for fact in first
        if getattr(fact, "subtype", None) == "branch_disposition"
    }
    assert "checked_at" not in by_branch["research/check-me"].attrs
    assert (
        by_branch["research/dated"].attrs["checked_at"]
        == "2026-07-25T12:00:00Z"
    )
    assert all("run_id" not in fact.source_revision for fact in first)
