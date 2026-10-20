"""MutableApiProbe — change-probe for a mutable upstream API (issue #323).

A source backed by a remote mutable API (GitHub issues, a ticketing system, any
REST endpoint) cannot fingerprint local bytes. Its cheap, sound change-identity
is an opaque *version token* the API exposes far more cheaply than the full
payload: an HTTP ETag, a Last-Modified / high-water ``updated_at`` timestamp, a
sync cursor, a ``(max_updated_at, count)`` pair. The source supplies a
``version_fn`` that returns that token as a string; the probe compares it for
equality only.

Soundness is one-directional, as for every probe: ``version_fn`` MUST advance
whenever the emitted facts could change. A high-water ``updated_at`` satisfies
this for create / edit / close (each bumps the timestamp); a hard delete that
does not move the high-water mark is the residual gap every watermark shares and
is caught by the ungated reconcile pass. A ``version_fn`` that raises (network
down, auth failure, rate limit) propagates -> the run_probe boundary maps it to
``changed=True`` (a full read), never a silent no-change skip.
"""
from __future__ import annotations

import hashlib
from typing import Any, Callable, Mapping, Optional

from okg.substrate.library.sources.change_probe import (
    CURSOR_TOKEN_KEY,
    ProbeVerdict,
    compose_token,
    emit_version_for,
    tokens_equal,
)


__all__ = ["MutableApiProbe"]


class MutableApiProbe:
    """An opaque-version-token probe for a mutable upstream API.

    Parameters
    ----------
    version_fn:
        Zero-arg callable returning the current upstream version token as a
        string (ETag / updated_at high-water / sync cursor). MUST advance on any
        change that would alter emitted facts. May raise -> changed=True.
    config:
        The adapter's effective config (endpoint, filters); hashed into the token.
    emit_targets:
        Emission callables/modules (machine-derived emit-version).
    content_kind:
        The token's content-identity kind; one of ``updated_at`` / ``etag`` /
        ``version_token`` (all sound mutable-API identities). Defaults to
        ``version_token``.
    """

    def __init__(
        self,
        *,
        version_fn: Callable[[], str],
        config: Mapping[str, Any],
        emit_targets: Any,
        content_kind: str = "version_token",
    ) -> None:
        self._version_fn = version_fn
        self._config_hash = _hash_config(config)
        self._emit_version = emit_version_for(*_as_tuple(emit_targets))
        self._content_kind = content_kind

    def build_token(self) -> dict[str, Any]:
        return self.token_for_version(self._version_fn())

    def token_for_version(self, value: Any) -> dict[str, Any]:
        """Compose a token for the exact API snapshot a reader consumed."""
        return compose_token(
            content={"kind": self._content_kind, "value": str(value)},
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
            reason="api_version_changed" if changed else "api_version_unchanged",
        )


def _as_tuple(targets: Any) -> tuple[Any, ...]:
    if isinstance(targets, (list, tuple)):
        return tuple(targets)
    return (targets,)


def _hash_config(config: Mapping[str, Any]) -> str:
    import json

    payload = json.dumps(dict(config), sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
