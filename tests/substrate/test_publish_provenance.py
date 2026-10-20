"""Tier W publish-provenance guard (worktree-access-tiers)."""

from __future__ import annotations

from okg.substrate.deployment_authority import publish_provenance_verdict


def _target(identity: str) -> dict:
    return {"identity": identity, "database": "okg_workspace",
            "host": "127.0.0.1", "port": 55434, "deployment": "okg-workspace"}


def test_foreign_checkout_refused_on_canonical() -> None:
    verdict = publish_provenance_verdict(
        sync_root="/repo/main",
        checkout="/repo/.claude/worktrees/agent-a",
        target=_target("ok"),
    )
    assert verdict["allowed"] is False
    assert verdict["mode"] == "foreign_checkout"
    assert "last-publisher-wins" in verdict["reason"]
    assert len(verdict["legal_moves"]) == 2


def test_sync_root_allowed_on_canonical() -> None:
    verdict = publish_provenance_verdict(
        sync_root="/repo/main", checkout="/repo/main", target=_target("ok"),
    )
    assert verdict == {"allowed": True, "mode": "sync_root"}


def test_isolated_clone_always_legal() -> None:
    verdict = publish_provenance_verdict(
        sync_root="/repo/main",
        checkout="/repo/.claude/worktrees/agent-a",
        target=_target("isolated"),
    )
    assert verdict == {"allowed": True, "mode": "isolated_clone"}


def test_event_plane_append_source_allowed_from_foreign_checkout() -> None:
    verdict = publish_provenance_verdict(
        sync_root="/repo/main",
        checkout="/repo/.claude/worktrees/agent-a",
        target=_target("ok"),
        file_derived=False,
    )
    assert verdict == {"allowed": True, "mode": "event_plane"}


def test_undeclared_sync_root_allows_with_loud_warning() -> None:
    verdict = publish_provenance_verdict(
        sync_root=None, checkout="/anywhere", target=_target("ok"),
    )
    assert verdict["allowed"] is True
    assert "sync_root" in verdict["warning"]


def test_unknown_checkout_refused_on_canonical() -> None:
    verdict = publish_provenance_verdict(
        sync_root="/repo/main", checkout=None, target=_target("ok"),
    )
    assert verdict["allowed"] is False
    assert verdict["mode"] == "checkout_unknown"
