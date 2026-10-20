from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from okg.substrate.library.agent_runtimes.paired_workspace import (
    PairProvisionError,
    PairedWorkspaceLifecycle,
    PairedWorkspaceRequest,
)


def test_ownership_inspection_failures_are_typed(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    worktree = tmp_path / "worktree"
    repo.mkdir()
    worktree.mkdir()

    def _raise_secret(*_args, **_kwargs):
        raise RuntimeError("postgresql://secret@db.invalid/private/path")

    lifecycle = PairedWorkspaceLifecycle(
        SimpleNamespace(inspect=_raise_secret),
        SimpleNamespace(inspect=_raise_secret),
    )
    request = PairedWorkspaceRequest(
        assignment_id="assignment:paired",
        assignment_generation=1,
        authenticated_host="worker01.example.org",
        repo_path=repo,
        worktree_path=worktree,
        git_branch="coord/paired-g1",
        graph_deployment="okg-workspace",
        grant_id="grant:paired:g1",
        mcp_route="coordination@coordination.okg.example",
    )

    with pytest.raises(PairProvisionError, match="Git rollback ownership") as git_error:
        lifecycle._git_attempt_owned(
            request,
            repo_path=str(repo),
            worktree_path=str(worktree),
            base_commit="1" * 40,
        )
    assert "secret" not in str(git_error.value)

    with pytest.raises(
        PairProvisionError, match="graph rollback ownership"
    ) as graph_error:
        lifecycle._graph_attempt_owned(
            request,
            graph_branch=request.resolved_graph_branch,
            base_generation=request.graph_base_generation,
        )
    assert "secret" not in str(graph_error.value)
