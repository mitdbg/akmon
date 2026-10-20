"""ChangeProbe contract — the cheap, sound change-probe (issue #323).

A ``ChangeProbe`` answers "has my scope changed since the prior cursor?"
cheaply, in the ``SourceRunner``, BEFORE any adapter reader runs. The runner
checks it in exactly one place and short-circuits the whole read on
``changed=False`` (see ``enforce-incremental-source-contract``).

Soundness is **one-directional**:

  - a false ``changed=True`` is PERMITTED — it degrades to a correct full read;
  - a false ``changed=False`` is a FORBIDDEN correctness bug — it silently drops
    a real change.

Therefore any probe error MUST resolve to ``changed=True`` (see ``run_probe``),
and a verdict's token is honored for a short-circuit only if it is *sound*
(see ``validate_token_composition``) — an unsound token can never let the
runner skip the reader.

The **token** is an opaque, collision-resistant SUPERSET of every input that
can change emitted facts:

  - ``content`` — scope content identity (git tree sha / append offset+hash /
    api etag-or-updated-at / a content-hash of the fetched payload). The
    stat-digest tier (mtime / size only) is BANNED: it is exactly the
    churn-fragile signal that defeated the prior per-file fix.
  - ``head`` — head identity (git HEAD commit sha), DISTINCT from the tree sha so
    a HEAD move to an identical tree is still detected. Optional for non-git
    scopes that have no head analogue.
  - ``config`` — the adapter's effective configuration hash (include/exclude
    globs, scope roots, repo identity, extraction params, plus any pipeline
    input that changes emitted facts under an unchanged scope: the
    canonicalizer chain, the catalog volatile_fields union, the Nomos
    policy_fingerprint).
  - ``emit_version`` — a MACHINE-DERIVED hash of the adapter's emission code
    (see ``emit_version_for``), NOT a hand-maintained integer, so a forgotten
    bump cannot silently drop a change.

The runner compares the token for equality only; it never interprets it.

``delta_hint`` is ADVISORY-FOR-COST-ONLY: on ``changed=True`` the reader
re-derives its own delta-proportional scope. The runner MUST NOT narrow a read
based on the hint (a too-narrow hint would be a silent under-read).
"""
from __future__ import annotations

import hashlib
import inspect
import json
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Protocol, runtime_checkable


__all__ = [
    "ProbeVerdict",
    "ChangeProbe",
    "ChangeProbeError",
    "TOKEN_SCHEMA",
    "CURSOR_TOKEN_KEY",
    "CURSOR_OUTSTANDING_KEY",
    "cursor_has_outstanding_work",
    "ALLOWED_CONTENT_KINDS",
    "BANNED_CONTENT_KINDS",
    "compose_token",
    "validate_token_composition",
    "tokens_equal",
    "emit_version_for",
    "run_probe",
    "CHANGE_PROBE_KIND_ATTR",
    "SOUND_PROBE_KINDS",
    "BANNED_PROBE_KINDS",
]


TOKEN_SCHEMA = "okg.source.change_probe.token.v1"

# A migrated adapter declares this CLASS-LEVEL attribute (a literal string) so
# deployment lint can statically confirm — without importing the adapter — that
# the source resolves to a sound probe. The runtime gate reads the INSTANCE
# attribute `change_probe`; the class attribute is the static declaration.
CHANGE_PROBE_KIND_ATTR = "change_probe_kind"

# The probe families that are content-identity-SOUND. `content_hash` is the
# O(content) floor (read+hash, skip parse) for sources that cannot get a cheaper
# probe — never a stat-digest tier and never a read-every-cycle escape.
SOUND_PROBE_KINDS = frozenset({
    "git_scope",
    "git_refs",  # ref-tips digest for a commit-graph (history) walk
    "append_offset",
    "mutable_api",
    "reference_catalog",
    "content_hash",
    "union",  # a UnionProbe composing two or more sound probes (hybrid scope)
})

# Where the runner persists a verdict token in the source cursor, and where a
# probe reads the prior token to compute `changed`. The runner stamps this on
# BOTH the short-circuit path and the normal read path — otherwise a normal read
# leaves no prior token and the next run can never short-circuit.
CURSOR_TOKEN_KEY = "change_probe_token"

# Reserved cursor key by which a source declares "my last run left work
# undone". A probe digests the SCOPE (what the files look like), never how much
# of that scope the source has consumed, so a source that bounds its work per
# run — chunked ingest of a large append feed — would be short-circuited while
# still holding a backlog, and would only resume by luck when the scope
# happened to change for an unrelated reason. A non-empty value here forces
# `changed`, making catch-up depend on the source's own state.
#
# One-directional, like every other probe rule: it can only force a full read,
# never suppress one. Sources that never write the key are unaffected.
CURSOR_OUTSTANDING_KEY = "outstanding_backlog"


def cursor_has_outstanding_work(cursor: Optional[Mapping[str, Any]]) -> bool:
    """True when the cursor records undrained work from a prior run."""
    if not isinstance(cursor, Mapping):
        return False
    outstanding = cursor.get(CURSOR_OUTSTANDING_KEY)
    if isinstance(outstanding, (list, tuple, set, dict)):
        return len(outstanding) > 0
    return bool(outstanding)

# Component keys a sound token MUST carry. `head` is optional (non-git scopes
# have no head analogue); content + config + emit_version are mandatory — a
# token that omits config cannot detect a config edit, and one that omits
# emit_version cannot detect an emission-logic change. Both are silent
# false-unchanged bugs.
_REQUIRED_COMPONENTS = ("content", "config", "emit_version")

# Sound content-identity kinds: each is a function of the scope's CONTENT.
ALLOWED_CONTENT_KINDS = frozenset({
    "git_tree",        # git HEAD^{tree} sha (+ dirty blob shas)
    "content_hash",    # sha of the read bytes (the O(content) floor)
    "append_offset",   # byte size + prefix/suffix hash of an append feed
    "etag",            # HTTP entity tag
    "updated_at",      # mutable-API high-water timestamp + version dedup
    "version_token",   # opaque upstream version/sync token
})

# The stat-digest tier. mtime/size/stat are NOT content identity: a worktree
# checkout moves mtime without changing content (false changed=true, tolerable)
# and a mtime-preserving in-place edit changes content without moving mtime
# (false changed=false, FORBIDDEN). Rejected at validation.
BANNED_CONTENT_KINDS = frozenset({"mtime", "size", "stat", "mtime_size"})

# Probe-kind declarations that name the banned stat-digest tier. A source that
# declares one of these as its `change_probe_kind` is rejected by lint: mtime /
# size are not content identity (a mtime-preserving in-place edit is a silent
# false-unchanged).
BANNED_PROBE_KINDS = frozenset({"mtime", "size", "stat", "mtime_size", "stat_digest"})

# Hash-bearing component values must be full precision (no truncation /
# digest-of-digest collisions). 32 hex chars admits sha1 (40) and sha256 (64);
# git tree/commit shas (40) pass.
_MIN_HASH_HEX = 32


class ChangeProbeError(Exception):
    """Raised when a token is structurally unsound or a probe cannot run.

    At the runner boundary (``run_probe``) this resolves to ``changed=True`` —
    never to a default ``changed=False``.
    """


@dataclass(frozen=True)
class ProbeVerdict:
    """The result of one cheap change-probe.

    ``token`` is the new scope fingerprint to persist; the runner stores it and
    compares it for equality next cycle. ``delta_hint`` is advisory-only — the
    runner MUST NOT use it to narrow the reader's scope.
    """

    changed: bool
    token: dict[str, Any] = field(default_factory=dict)
    delta_hint: Optional[dict[str, Any]] = None
    reason: str = ""


@runtime_checkable
class ChangeProbe(Protocol):
    """A source-class-bound cheap change-probe.

    ``probe`` is O(scope-metadata), never O(content) for the cheap tiers (git /
    append / api); the content-hash floor is O(content) read+hash but still
    skips parse/extraction. It must be conservative: when in doubt, return
    ``changed=True``.
    """

    def probe(
        self,
        *,
        cursor: Optional[Mapping[str, Any]],
        sync_scope: Optional[Mapping[str, Any]],
    ) -> ProbeVerdict:
        raise NotImplementedError


def _component(kind: str, value: Any) -> dict[str, Any]:
    return {"kind": kind, "value": value}


def compose_token(
    *,
    content: dict[str, Any],
    config: str,
    emit_version: str,
    head: Optional[dict[str, Any]] = None,
    extra: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    """Build a canonical, validated token from its fact-determining components.

    ``content`` and (optional) ``head`` are ``{"kind": ..., "value": ...}``
    dicts; ``config`` and ``emit_version`` are full-precision hex digests. Raises
    ``ChangeProbeError`` if the composition is unsound.
    """
    components: dict[str, Any] = {
        "content": dict(content),
        "config": _component("config_hash", config),
        "emit_version": _component("emit_code_hash", emit_version),
    }
    if head is not None:
        components["head"] = dict(head)
    if extra:
        # Extra fact-determining inputs fold into config so the token stays a
        # superset; they never create a parallel un-validated channel.
        merged = hashlib.sha256()
        merged.update(config.encode("utf-8"))
        merged.update(_canonical_bytes(dict(extra)))
        components["config"] = _component("config_hash", merged.hexdigest())
    token = {"schema": TOKEN_SCHEMA, "components": components}
    validate_token_composition(token)
    return _canonicalize(token)


def validate_token_composition(token: Any) -> None:
    """Raise ``ChangeProbeError`` unless ``token`` is a structurally sound
    superset token.

    Enforces, at construction / before any short-circuit:
      - schema tag present;
      - content + config + emit_version components present and non-empty;
      - content is a content-identity kind, NOT a banned stat-digest kind
        (rejects the mtime/size-only tier — the silent false-unchanged bug);
      - hash-bearing values are full precision (no truncation collisions);
      - rejects a content-only token (missing config or emit_version).
    """
    if not isinstance(token, Mapping):
        raise ChangeProbeError("token must be a mapping")
    if token.get("schema") != TOKEN_SCHEMA:
        raise ChangeProbeError(
            f"token schema must be {TOKEN_SCHEMA!r}, got {token.get('schema')!r}"
        )
    components = token.get("components")
    if not isinstance(components, Mapping):
        raise ChangeProbeError("token.components must be a mapping")

    for key in _REQUIRED_COMPONENTS:
        if key not in components:
            raise ChangeProbeError(
                f"token is missing required component {key!r}; a content-only "
                "or under-specified token cannot detect config/emission changes "
                "and would silently drop them (forbidden false-unchanged)"
            )

    content = components.get("content")
    if not isinstance(content, Mapping):
        raise ChangeProbeError("token.components.content must be a mapping")
    content_kind = content.get("kind")
    if content_kind in BANNED_CONTENT_KINDS:
        raise ChangeProbeError(
            f"content kind {content_kind!r} is a banned stat-digest tier; "
            "mtime/size are not content identity. Use a content-identity probe "
            "(git_tree / append_offset / etag / updated_at) or the content_hash "
            "floor (read+hash, skip parse)."
        )
    if content_kind not in ALLOWED_CONTENT_KINDS:
        raise ChangeProbeError(
            f"content kind {content_kind!r} is not an allowed content-identity "
            f"kind {sorted(ALLOWED_CONTENT_KINDS)}"
        )

    for key in ("config", "emit_version"):
        comp = components.get(key)
        if not isinstance(comp, Mapping):
            raise ChangeProbeError(f"token.components.{key} must be a mapping")
        value = comp.get("value")
        if not isinstance(value, str) or not value:
            raise ChangeProbeError(
                f"token.components.{key}.value must be a non-empty hex digest"
            )
        if _looks_like_hex(value) and len(value) < _MIN_HASH_HEX:
            raise ChangeProbeError(
                f"token.components.{key}.value is a truncated digest "
                f"(len {len(value)} < {_MIN_HASH_HEX}); lossy encodings collide "
                "and cause silent false-unchanged"
            )


def tokens_equal(a: Any, b: Any) -> bool:
    """Canonical equality: two tokens are equal iff they encode the same
    fact-determining state, independent of key ordering."""
    if a is None or b is None:
        return False
    return _canonical_bytes(a) == _canonical_bytes(b)


def emit_version_for(*targets: Any) -> str:
    """Derive an emit-version from the source of the given emission targets
    (functions / classes / modules), so a change to emission logic changes the
    token with no manual bump.

    Falls back to bytecode for objects whose source is unavailable. Raises
    ``ChangeProbeError`` if nothing fingerprintable is supplied.
    """
    if not targets:
        raise ChangeProbeError("emit_version_for requires at least one target")
    chunks: list[str] = []
    for target in targets:
        name = getattr(target, "__qualname__", None) or getattr(
            target, "__name__", repr(target)
        )
        body: Optional[str] = None
        try:
            body = inspect.getsource(target)
        except (OSError, TypeError):
            code = getattr(target, "__code__", None)
            if code is not None and getattr(code, "co_code", None) is not None:
                body = "bytecode:" + code.co_code.hex()
        if body is None:
            raise ChangeProbeError(
                f"emit_version_for: cannot fingerprint {name!r} "
                "(no source and no bytecode)"
            )
        chunks.append(f"{name}\n{body}")
    digest = hashlib.sha256("\n\x00\n".join(sorted(chunks)).encode("utf-8"))
    return digest.hexdigest()


def run_probe(
    probe: ChangeProbe,
    *,
    cursor: Optional[Mapping[str, Any]],
    sync_scope: Optional[Mapping[str, Any]],
) -> ProbeVerdict:
    """The runner boundary. Invoke ``probe`` and enforce one-directional
    soundness:

      - any exception resolves to ``changed=True`` (never a default
        ``changed=False``);
      - a ``changed=False`` verdict is honored ONLY if its token is sound;
        an unsound token forces ``changed=True``.

    A ``changed=True`` verdict is returned as-is (its token may be empty;
    a full read follows regardless).
    """
    try:
        verdict = probe.probe(cursor=cursor, sync_scope=sync_scope)
    except Exception as exc:  # noqa: BLE001 — probe error MUST fail toward a full read
        return ProbeVerdict(
            changed=True,
            token={},
            reason=f"probe_error:{type(exc).__name__}:{exc}",
        )
    if not isinstance(verdict, ProbeVerdict):
        return ProbeVerdict(
            changed=True,
            token={},
            reason=f"probe_returned_{type(verdict).__name__}_not_ProbeVerdict",
        )
    if not verdict.changed:
        try:
            validate_token_composition(verdict.token)
        except ChangeProbeError as exc:
            return ProbeVerdict(
                changed=True,
                token=verdict.token,
                reason=f"unsound_token_forces_full_read:{exc}",
            )
    return verdict


# ---- canonicalization helpers ----

def _canonical_bytes(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _canonicalize(obj: Any) -> dict[str, Any]:
    return json.loads(_canonical_bytes(obj))


def _looks_like_hex(value: str) -> bool:
    if len(value) < 8:
        return False
    try:
        int(value, 16)
    except ValueError:
        return False
    return True
