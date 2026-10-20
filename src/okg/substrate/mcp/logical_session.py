"""Transport-lifetime generation defaults for the public MCP read surface.

The substrate :class:`Session` remains immutable and connection-local.  This
module owns the smaller piece the transport adapter needs between tool calls:
one default generation id per live SDK ``ServerSession`` object.

The registry is deliberately server-local and weak-keyed.  It never keys on a
deployment, process id, caller metadata, authentication identity, or the raw
``mcp-session-id`` header.

In multi-user HTTP mode the transport adapter supplies the SDK with a qualified
principal on each verified request. The SDK refuses a different principal
before this registry is reached; the registry still stores no authorization.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
import threading
from typing import Any, Iterator
from uuid import uuid4
import weakref


@dataclass
class LogicalSessionState:
    """Mutable default owned by one live SDK transport session.

    ``lock`` is per transport session.  ``valid`` is cleared before a failed
    first-bind value is removed so a waiter can detect that it acquired an
    orphan and restart registry lookup instead of publishing a second pin.
    Values must never retain their weak-map key.
    """

    logical_session_id: str = field(default_factory=lambda: str(uuid4()))
    default_generation_id: object | None = None
    #: What this session's text results last announced (issue #3059, #3076
    #: DESIGN 1.2.3): the stale-source set under ``"stale"``. The notice
    #: prints once per session and again when the set changes. Only a served
    #: result updates it; a byte measurement never does. Issue #3666
    #: (read-passage-rows) adds ``"calls"`` (this session's served text
    #: replies, numbered from 1), ``"once"`` (each repeated notice and the
    #: generation id, as last printed) and ``"served"`` (record id to the
    #: call that printed its passage or whole text, for "shown in call N").
    result_notices: dict[str, Any] = field(default_factory=dict)
    valid: bool = True
    lock: threading.RLock = field(default_factory=threading.RLock, repr=False)


@dataclass(frozen=True)
class LockedLogicalSession:
    """One canonical state held under its per-session lock."""

    state: LogicalSessionState
    created: bool


@dataclass(frozen=True)
class LogicalSessionSnapshot:
    """Public, non-secret state copied while the value lock is held."""

    logical_session_id: str
    default_generation_id: object | None


class LogicalSessionRegistry:
    """Weak, linearizable mapping from SDK session object to OKG default pin.

    Lock order is intentionally asymmetric: lookup takes only ``_registry_lock``;
    a caller then takes the value lock and re-enters ``_registry_lock`` only to
    prove the value is still canonical.  No path waits for a value lock while
    holding the registry lock, so invalidation cannot deadlock with a waiter.
    """

    def __init__(self) -> None:
        self._registry_lock = threading.RLock()
        self._states: weakref.WeakKeyDictionary[Any, LogicalSessionState] = (
            weakref.WeakKeyDictionary()
        )

    @staticmethod
    def supports(identity: object | None) -> bool:
        """Whether ``identity`` is safe as a weak map key."""

        if identity is None:
            return False
        try:
            hash(identity)
            weakref.ref(identity)
        except (TypeError, ValueError):
            return False
        return True

    def _lookup_or_create(
        self,
        identity: object,
    ) -> tuple[LogicalSessionState, bool]:
        with self._registry_lock:
            state = self._states.get(identity)
            if state is not None and state.valid:
                return state, False
            state = LogicalSessionState()
            self._states[identity] = state
            return state, True

    def _is_canonical(
        self,
        identity: object,
        state: LogicalSessionState,
    ) -> bool:
        with self._registry_lock:
            return state.valid and self._states.get(identity) is state

    @contextmanager
    def locked(
        self,
        identity: object,
    ) -> Iterator[LockedLogicalSession]:
        """Yield the canonical value with its lock held.

        A failed first binder may invalidate a value while another caller is
        waiting on it.  The waiter must not use that orphan: after acquiring the
        value lock it revalidates against the weak map and restarts lookup when
        necessary.
        """

        if not self.supports(identity):
            raise TypeError("logical MCP session identity is not a weak map key")
        while True:
            state, created = self._lookup_or_create(identity)
            state.lock.acquire()
            if not self._is_canonical(identity, state):
                state.lock.release()
                continue
            try:
                yield LockedLogicalSession(state=state, created=created)
            finally:
                state.lock.release()
            return

    def invalidate_locked(
        self,
        identity: object,
        state: LogicalSessionState,
    ) -> None:
        """Invalidate and remove ``state`` if it remains canonical.

        The caller must hold ``state.lock``.  Marking invalid first makes every
        waiter restart even if another caller installs a replacement immediately.
        """

        state.valid = False
        with self._registry_lock:
            if self._states.get(identity) is state:
                del self._states[identity]

    def snapshot(self, identity: object) -> LogicalSessionSnapshot:
        """Return the current public state, creating an empty value if needed."""

        with self.locked(identity) as lease:
            return LogicalSessionSnapshot(
                logical_session_id=lease.state.logical_session_id,
                default_generation_id=lease.state.default_generation_id,
            )

    @contextmanager
    def result_notices(self, identity: object) -> Iterator[dict[str, Any]]:
        """Yield this session's result-notice state with its lock held."""

        with self.locked(identity) as lease:
            yield lease.state.result_notices

    def __len__(self) -> int:
        """Testing/diagnostic count; weak-map cleanup happens before return."""

        with self._registry_lock:
            return len(self._states)
