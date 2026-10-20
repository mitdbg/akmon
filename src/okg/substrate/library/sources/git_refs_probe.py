"""GitRefsProbe — change-probe for a git *history* walk (issue #323).

A `GitScopeProbe` fingerprints the HEAD tree (the working-tree snapshot), which
is the right identity for a source that reads files at HEAD. A history source is
different: it walks the commit graph reachable from MANY refs (local branches,
tags, optionally remotes), so its emission is a function of the full set of ref
TIPS, not the HEAD tree. A branch/tag created, deleted, or advanced changes the
reachable history even when the HEAD tree is untouched.

This probe's content component is a membership-sensitive digest of the sorted
``(ref_name, target_sha)`` pairs plus a count, so all three mutation kinds bust
the token:

  - a ref **advances** (new commit)        -> its target sha changes;
  - a branch/tag is **created**            -> a new entry (+count);
  - a branch/tag is **deleted**            -> an entry drops (-count).

The reachable commit set is fully determined by the ref tips, and a commit's
content cannot change without changing its sha (and therefore some ref tip), so
the ref-tips digest is a sound identity for the commits/branches/tags/authors
the history walk emits. Walk-shaping config (include_remotes, branch_filter,
history_since, rename detection, stub globs) is folded into the config component.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Mapping, Optional

from okg.substrate.library.sources.change_probe import (
    CURSOR_TOKEN_KEY,
    ProbeVerdict,
    compose_token,
    emit_version_for,
    tokens_equal,
)


__all__ = ["GitRefsProbe"]


class GitRefsProbe:
    """A ref-tips change-probe for a git history source.

    Parameters
    ----------
    repo_root:
        The git work-tree root to enumerate refs in.
    config:
        The adapter's effective walk-shaping config (hashed into the token).
    emit_targets:
        Emission callables/modules (machine-derived emit-version).
    include_remotes:
        Whether the source emits remote-tracking BRANCH NODES. Recorded for the
        caller's benefit; it deliberately does NOT narrow the content digest,
        which always covers remote tips because the walk always reaches them.
    """

    def __init__(
        self,
        *,
        repo_root: Any,
        config: Mapping[str, Any],
        emit_targets: Any,
        include_remotes: bool = False,
    ) -> None:
        self._repo_root = Path(repo_root)
        self._config_hash = _hash_config(config)
        self._emit_version = emit_version_for(*_as_tuple(emit_targets))
        self._include_remotes = bool(include_remotes)

    @staticmethod
    def _content_component_from_tips(
        tips: Mapping[str, str],
    ) -> dict[str, Any]:
        items = sorted((str(name), str(sha)) for name, sha in tips.items())
        digest = hashlib.sha256()
        digest.update(f"n={len(items)}".encode("utf-8"))
        for name, sha in items:
            digest.update(b"\x00")
            digest.update(name.encode("utf-8"))
            digest.update(b"\x00")
            digest.update(sha.encode("utf-8"))
        return {"kind": "git_tree", "value": digest.hexdigest()}

    def _content_component(self) -> dict[str, Any]:
        from okg.substrate.library.sources._git_helpers import in_proc_ref_tips

        # Always digest remote-tracking tips, regardless of `include_remotes`.
        # The history walk's positive set is EVERY ref (`_list_refs`), because
        # remote-only commits are reachable and emitted; `include_remotes` gates
        # branch-NODE emission alone. A probe narrower than the walk would let a
        # remote-only advance change emission without busting the token -- the
        # unstable-membership defect this PR exists to fix. `include_remotes`
        # still varies the token through the config component.
        tips = in_proc_ref_tips(self._repo_root, include_remotes=True)
        return self._content_component_from_tips(tips)

    def build_token(self) -> dict[str, Any]:
        return compose_token(
            content=self._content_component(),
            config=self._config_hash,
            emit_version=self._emit_version,
        )

    def token_for_ref_tips(
        self,
        tips: Mapping[str, str],
    ) -> dict[str, Any]:
        """Compose a token from the exact raw ref targets the emitter used."""
        return compose_token(
            content=self._content_component_from_tips(tips),
            config=self._config_hash,
            emit_version=self._emit_version,
        )

    def probe(
        self,
        *,
        cursor: Optional[Mapping[str, Any]],
        sync_scope: Optional[Mapping[str, Any]],
    ) -> ProbeVerdict:
        prior = (
            cursor.get(CURSOR_TOKEN_KEY) if isinstance(cursor, Mapping) else None
        )
        token = self.build_token()
        if prior is None:
            return ProbeVerdict(changed=True, token=token, reason="no_prior_token")
        changed = not tokens_equal(token, prior)
        return ProbeVerdict(
            changed=changed,
            token=token,
            reason="refs_changed" if changed else "refs_unchanged",
        )


def _as_tuple(targets: Any) -> tuple[Any, ...]:
    if isinstance(targets, (list, tuple)):
        return tuple(targets)
    return (targets,)


def _hash_config(config: Mapping[str, Any]) -> str:
    import json

    payload = json.dumps(dict(config), sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
