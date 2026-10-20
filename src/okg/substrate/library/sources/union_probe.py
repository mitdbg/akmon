"""UnionProbe — compose change-probes for HYBRID-scope sources (issue #323).

Some sources read more than one kind of scope and a change in ANY of them must
re-emit:

  - pact: git-tracked ``pact/changes/**`` files PLUS a DB decision ledger;
  - agent-sessions / codex-sessions: repo files (git) PLUS external transcripts
    (a non-git, append-growing directory).

A git-only or transcript-only probe would silently miss a change in the other
half — a forbidden false-unchanged. ``UnionProbe`` composes sub-probes so the
verdict is ``changed`` iff ANY sub-probe is changed, and the token folds every
sub-probe's token (which already captures that sub's content + head + config +
emit-version). If any sub-probe raises, ``build_token`` propagates and the
runner boundary (``run_probe``) resolves it to ``changed=true`` (fail toward a
full read).
"""
from __future__ import annotations

import hashlib
import json
from typing import Any, Iterable, Mapping, Optional

from okg.substrate.library.sources.change_probe import (
    CURSOR_TOKEN_KEY,
    ChangeProbe,
    ChangeProbeError,
    ProbeVerdict,
    compose_token,
    cursor_has_outstanding_work,
    emit_version_for,
    tokens_equal,
)


__all__ = ["UnionProbe"]


class UnionProbe:
    """A change-probe that is changed iff any of its sub-probes is changed."""

    def __init__(self, probes: Iterable[ChangeProbe]) -> None:
        self._probes = [p for p in probes if p is not None]
        if not self._probes:
            raise ChangeProbeError("UnionProbe requires at least one sub-probe")

    def build_token(self) -> dict[str, Any]:
        # Each sub-token already embeds that sub's content + head + config +
        # emit-version, so a content-hash over the sorted sub-tokens is a
        # complete, collision-resistant fingerprint of the whole hybrid scope.
        return self.token_from_subtokens(
            probe.build_token() for probe in self._probes
        )

    def token_from_subtokens(
        self,
        subtokens: Iterable[Mapping[str, Any]],
    ) -> dict[str, Any]:
        """Compose the union from exact child snapshots an adapter consumed."""
        sub_bytes = sorted(
            json.dumps(
                dict(token),
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            for token in subtokens
        )
        if len(sub_bytes) != len(self._probes):
            raise ChangeProbeError(
                "UnionProbe exact composition requires one token per child"
            )
        digest = hashlib.sha256()
        digest.update(f"union:n={len(sub_bytes)}".encode("utf-8"))
        for raw in sub_bytes:
            digest.update(b"\x00")
            digest.update(raw)
        content = {"kind": "content_hash", "value": digest.hexdigest()}
        structure = "|".join(sorted(type(p).__name__ for p in self._probes))
        config = hashlib.sha256(("union:" + structure).encode("utf-8")).hexdigest()
        return compose_token(
            content=content,
            config=config,
            emit_version=emit_version_for(UnionProbe),
        )

    def probe(
        self,
        *,
        cursor: Optional[Mapping[str, Any]],
        sync_scope: Optional[Mapping[str, Any]],
    ) -> ProbeVerdict:
        prior = (
            cursor.get(CURSOR_TOKEN_KEY)
            if isinstance(cursor, Mapping)
            else None
        )
        token = self.build_token()
        if prior is None:
            return ProbeVerdict(changed=True, token=token, reason="no_prior_token")
        # A sub-probe's own `probe()` is never consulted here (its token is not
        # what the cursor holds), so the cursor-derived outstanding-work signal
        # has to be applied at this level too, or a hybrid source's chunked
        # backlog would short-circuit exactly where a single-probe source's
        # would not.
        if cursor_has_outstanding_work(cursor):
            return ProbeVerdict(
                changed=True, token=token, reason="outstanding_backlog",
            )
        changed = not tokens_equal(token, prior)
        return ProbeVerdict(
            changed=changed,
            token=token,
            reason="union_changed" if changed else "union_unchanged",
        )
