"""RepoNamespaceCanonicalizer — rewrite the `<repo>` segment of
`<family>:<repo>:<rest>` ids to a canonical name.

Use case: agent_session running in a git worktree
`~/.claude/worktrees/foo` resolves `repo = cwd_basename`, so
its tool-call edits emit `file:foo:src/x.py`. git_files,
running against canonical `~/projects/mit/okg`, emits
`file:okg:src/x.py`. Without canonicalization the two ID
namespaces never join; the audit reports 1,549 orphan edges.

Constructed with `repo_aliases: dict[str, str]` (alias →
canonical) and an optional `families: tuple[str, ...]`
restriction (defaults to file/commit/branch/tag — every
git-namespaced id family). One instance per id_family makes
the registry simpler; `from_aliases` ships a sensible default
multi-family chain.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable


__all__ = ["RepoNamespaceCanonicalizer"]


@dataclass
class RepoNamespaceCanonicalizer:
    """Rewrite `<id_family>:<alias>:<rest>` →
    `<id_family>:<canonical>:<rest>` per `repo_aliases`."""

    id_family: str
    repo_aliases: dict[str, str] = field(default_factory=dict)

    def canonicalize(self, raw_id: str) -> str:
        if not raw_id:
            return raw_id
        family, sep1, after = raw_id.partition(":")
        if family != self.id_family or sep1 != ":":
            return raw_id
        repo, sep2, rest = after.partition(":")
        if sep2 != ":":
            return raw_id
        canonical = self.repo_aliases.get(repo)
        if canonical is None or canonical == repo:
            return raw_id
        return f"{family}:{canonical}:{rest}"

    @classmethod
    def chain_from_aliases(
        cls,
        repo_aliases: dict[str, str],
        *,
        families: Iterable[str] = (
            "file", "commit", "branch", "tag",
        ),
    ) -> list["RepoNamespaceCanonicalizer"]:
        """Build a list of canonicalizers — one per id-family —
        sharing the same `repo_aliases` map. Used by
        deployment bootstrap to register the full chain in one
        shot."""
        return [
            cls(id_family=fam, repo_aliases=dict(repo_aliases))
            for fam in families
        ]
