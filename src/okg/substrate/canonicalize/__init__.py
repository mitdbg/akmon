"""Substrate ID canonicalization.

A canonicalizer takes a raw `node_id` or edge endpoint and
returns its canonical form. Different adapters writing about
the same physical entity can land on a single canonical ID —
e.g. agent_session running in worktree `~/.claude/worktrees/foo`
emits `file:foo:src/x.py`; the canonical
`RepoNamespaceCanonicalizer({foo: okg})` rewrites that to
`file:okg:src/x.py` so the file edge joins with git_files'
emit.

Per `add-substrate-id-canonicalization` (2026-04-25). The
canonicalization layer is the single chokepoint; adapters
emit raw IDs, the SourceRunner rewrites once before facts
land in node_facts / edge_facts. Downstream readers
(projector, MCP, viz) see only canonical IDs.
"""
from okg.substrate.canonicalize.base import (
    IdCanonicalizer,
    canonicalize_id,
)
from okg.substrate.canonicalize.repo_namespace import (
    RepoNamespaceCanonicalizer,
)


__all__ = [
    "IdCanonicalizer",
    "RepoNamespaceCanonicalizer",
    "canonicalize_id",
]
