"""External-repos multi-repo source factory.

Per `add-substrate-external-repos-module` (Wave A5 of
`evolve-memex-to-ai-scientist-substrate`). The factory reads
an operator-curated YAML config listing N external git
repositories and spawns per-repo instances of the substrate's
`git_files`, `git_history`, and `code_structure` sources,
emitting one `ExternalRepo` node per declared repo plus
`(SourceFile, member_of, ExternalRepo)` edges typing every
emitted source file under its parent repo.
"""
from __future__ import annotations

from okg.substrate.library.sources.external_repos.factory import (
    ExternalRepoConfig,
    ExternalRepoFactory,
    RepoEntry,
)

__all__ = ["ExternalRepoConfig", "ExternalRepoFactory", "RepoEntry"]
