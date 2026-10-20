"""Per-commit memoization of state that is invariant within one ingest run.

A single :func:`okg.substrate.ingest.service.run_ingest` re-reads a few values
that cannot change during a data-ingest commit — the active catalog version and
the active Nomos policy bundle — once per dependency layer and per store view.
Ingest never applies a catalog version or activates a policy mid-run, so each is
resolved at most once per commit and memoized here.

The memo is scoped to one ``run_ingest`` via a :class:`contextvars.ContextVar`
set at the top of the run and reset on exit. It never outlives the commit, so a
cached value can never go stale across commits — the next commit opens a fresh
scope and re-reads once. Outside an active scope (non-ingest callers of the same
reads) :func:`memoized` falls through to a live read, so behavior is unchanged.

Only reads with no writer reachable from within an ingest run are memoized in
the read function's body (the catalog version: ingest never applies one). Reads
whose underlying state *can* be written by other code paths (the Nomos policy
bundle, written by deployment seeding / policy activation) are memoized at their
read-only call sites instead, so the read-before-write on those writer paths is
never served a stale value.
"""

from __future__ import annotations

import contextlib
import contextvars
import itertools
from typing import Any, Callable, Iterator, TypeVar

T = TypeVar("T")

_MISSING = object()
#: Marks a scope opened by :func:`read_pass_scope`. Stored in the memo dict
#: itself (not a second contextvar) so it cannot drift out of sync with the
#: scope it describes. It is a ``str`` and every memo key in the tree is a
#: tuple, so it cannot collide with one.
_READ_PASS_KEY = "__okg_read_pass__"
#: Per-pass token for callers that skip an idempotent side effect within one
#: read pass. It is part of their memo key, so marks left in a SHARED outer
#: dict by a finished pass can never be hit by the next one -- which is what
#: keeps a read pass nested inside an ingest run from suppressing that run's
#: writes after it exits.
_READ_PASS_TOKEN_KEY = "__okg_read_pass_token__"
_READ_PASS_COUNTER = itertools.count(1)
_SCOPE: contextvars.ContextVar[dict[Any, Any] | None] = contextvars.ContextVar(
    "okg_commit_scope", default=None
)


@contextlib.contextmanager
def commit_scope() -> Iterator[None]:
    """Open a per-commit memo scope around exactly one ingest run."""
    token = _SCOPE.set({})
    try:
        yield
    finally:
        _SCOPE.reset(token)


@contextlib.contextmanager
def read_pass_scope(*, dedup_identity_claims: bool = False) -> Iterator[None]:
    """Open a memo scope around exactly one read-side unit of work.

    Same machinery, different unit of work. PACT ``mcp-principal-mapping``
    moves policy evaluation from once-per-process to once-per-request, which
    makes the Nomos identity reads (providers, subject bindings, permission
    grants, active bundle) run once per ROW per SOURCE instead of once per
    server. Those four reads are invariant within the pass -- nothing inside
    it activates a policy or edits a binding -- so they are resolved once.

    Scoped to the pass, deliberately. A cache that outlived the pass would
    need invalidation on every policy-bundle activation and binding edit; one
    that dies with the pass cannot go stale at all, because the next request
    opens a fresh scope and re-reads. That is the whole invalidation story.

    Reentrant: an inner scope joins the outer one rather than shadowing it,
    so an MCP request that opens a scope around the whole tool call still
    shares its memo with the read filter's own scope inside it. A shadowing
    inner scope would silently re-read everything and look like it worked.

    ``dedup_identity_claims`` additionally permits callers to skip an
    idempotent WRITE (the external-claim upsert) after the first occurrence in
    this pass. Off by default and opted into only by the multi-user request
    gate, because the upsert refreshes ``observed_at`` on conflict: skipping
    it changes what the audit trail records, which is a change no
    single-operator caller and no ingest run asked for.
    """
    parent = _SCOPE.get()
    token_value = (
        next(_READ_PASS_COUNTER) if dedup_identity_claims else None
    )
    if parent is not None:
        # Already inside some scope (an outer read pass, or an ingest commit).
        # Mark it for the duration and restore the outer answer on exit, so a
        # read pass nested in an ingest run does not leave the run looking
        # like a read pass.
        previous = (
            parent.get(_READ_PASS_KEY, False),
            parent.get(_READ_PASS_TOKEN_KEY),
        )
        parent[_READ_PASS_KEY] = True
        if token_value is not None:
            parent[_READ_PASS_TOKEN_KEY] = token_value
        try:
            yield
        finally:
            parent[_READ_PASS_KEY], parent[_READ_PASS_TOKEN_KEY] = previous
        return
    scope: dict[Any, Any] = {_READ_PASS_KEY: True}
    if token_value is not None:
        scope[_READ_PASS_TOKEN_KEY] = token_value
    token = _SCOPE.set(scope)
    try:
        yield
    finally:
        _SCOPE.reset(token)


def identity_claim_dedup_token() -> Any:
    """Per-pass token for skipping a repeated idempotent identity write.

    Returns ``None`` when no read pass opted into the dedup -- outside one,
    and inside an ingest ``commit_scope``, every write happens exactly as it
    did before this scope existed. The token is part of the caller's memo key,
    so a mark left in a shared outer dict by a FINISHED pass can never be hit
    by the next one.
    """
    scope = _SCOPE.get()
    if scope is None or not scope.get(_READ_PASS_KEY, False):
        return None
    return scope.get(_READ_PASS_TOKEN_KEY)


def commit_scope_active() -> bool:
    """True iff a commit scope is open on the current context."""
    return _SCOPE.get() is not None


def memo_seen(key: Any) -> bool:
    """True iff ``key`` was already marked in the active scope; marks it if not.

    For idempotent side effects (an upsert whose repeat within one scope
    writes the same row again). With no active scope this always returns
    False, so the side effect runs exactly as it did before any scope
    existed.
    """
    scope = _SCOPE.get()
    if scope is None:
        return False
    if key in scope:
        return True
    scope[key] = True
    return False


def memoized(key: Any, compute: Callable[[], T]) -> T:
    """Return ``compute()``'s result, memoized within the active commit scope.

    With no active scope the value is computed live (uncached), so non-ingest
    callers are unaffected. A ``None`` result is cached like any other value.
    """
    scope = _SCOPE.get()
    if scope is None:
        return compute()
    cached = scope.get(key, _MISSING)
    if cached is _MISSING:
        cached = compute()
        scope[key] = cached
    return cached


def db_scope_key(conn: Any) -> Any:
    """A connection's database identity, for keying a per-commit memo.

    Distinguishes connections to different databases that share one scope (a
    defensive guard; a single ``run_ingest`` targets one deployment database).
    Returns ``None`` when the connection exposes no ``info`` (single bucket).
    """
    info = getattr(conn, "info", None)
    if info is None:
        return None
    return (
        getattr(info, "dbname", None),
        getattr(info, "host", None),
        getattr(info, "port", None),
    )
