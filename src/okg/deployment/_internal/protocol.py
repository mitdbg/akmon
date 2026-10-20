"""Connector protocol, context and signals (#1181 slice 3).

Slice 1 exported the fact, identity, preflight and redaction *values* a
connector needs. This slice adds the shape it runs in: a `Connector`
protocol, the `ConnectorContext` handed to it, the `ConnectorRun` it
returns, and the five signals that are the only sanctioned early exits.

Unlike slice 1, these are SDK-owned objects rather than substrate
aliases, because no substrate equivalent exists. `ConnectorAdapter` is
the bridge: it satisfies `SourceAdapter` exactly, so `SourceRunner`
drives a `Connector` with no runner change.

Why the shim materializes the fact stream
-----------------------------------------
`SourceRunner` snapshots ``source_run.next_cursor`` into a local
(``ingest/runner.py`` line 3351) *before* it walks ``source_run.facts``
(line 3712). A signal raised mid-stream therefore cannot retract a
checkpoint the runner has already read. To keep "a signal advances no
checkpoint" true rather than aspirational, the shim collects the
connector's facts before it builds the `SourceRun`, so the checkpoint
decision always precedes the runner's snapshot.

The cost is that a connector's facts are held in memory for the length
of one run. A streaming variant would need the runner to read
``next_cursor`` after the walk; ``ingest/runner.py`` is out of scope for
this slice.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping, Optional, Protocol

from okg.substrate.library.sources.base import (
    SOURCE_RUN_PARTIAL_STATUS,
    RecordAuthorityStream,
    RecordSet,
    SourceHealth,
    SourceRun,
)
from okg.substrate.library.sources.change_probe import ProbeVerdict


# --- Signals -----------------------------------------------------------------
#
# These are the ONLY sanctioned early exits. Each one ends the run without
# advancing the checkpoint, so the next run re-reads the same frontier. They
# are plain exceptions rather than `SubstrateError` subclasses: an external
# connector must not have to import the substrate error taxonomy to fail
# correctly, and these carry no operator-facing error code.


class ConnectorError(Exception):
    """Base for every sanctioned connector early exit.

    Raising it ends the run as a bounded, structured partial rather than an
    unhandled crash. The checkpoint does not advance, so no facts are lost:
    the next run resumes from the same frontier.
    """


class Cancelled(ConnectorError):
    """The run was asked to stop; `ConnectorContext.cancelled()` was true."""


class Deadline(ConnectorError):
    """The run exceeded `ConnectorContext.deadline`."""


def _checked_seconds(seconds: Any) -> float:
    """Return `seconds` as a bounded float, or raise `ValueError`."""
    if isinstance(seconds, bool) or not isinstance(seconds, (int, float)):
        raise ValueError(
            "RetryAfter(seconds=...) must be a real number of seconds, "
            f"got {seconds!r}"
        )
    value = float(seconds)
    # Rejects NaN (never equal to itself) as well as the two infinities.
    if not 0.0 <= value <= MAX_RETRY_AFTER_SECONDS:
        raise ValueError(
            "RetryAfter(seconds=...) must be between 0 and "
            f"{MAX_RETRY_AFTER_SECONDS:g} seconds, got {seconds!r}"
        )
    return value


#: Upper bound on a `RetryAfter` hint, in seconds (24 hours).
#:
#: A hint is advice for the *next* run of this source, and the substrate
#: schedules that run itself. Past a day the hint has outlived the schedule
#: it would inform, so an out-of-range value is far more likely to be a unit
#: mistake — milliseconds passed as seconds, a raw epoch timestamp copied out
#: of a `Retry-After` header — than a real request to wait that long.
MAX_RETRY_AFTER_SECONDS = 86_400.0


class RetryAfter(ConnectorError):
    """The upstream asked the caller to come back later.

    `seconds`, when given, must be a real number in
    ``[0, MAX_RETRY_AFTER_SECONDS]``. Anything else raises `ValueError` at
    construction rather than travelling into run health as a nonsense
    retry hint: a negative or NaN wait is unsatisfiable, and an absurd one
    reads to an operator as a deliberate day-long backoff. This is a
    connector defect, so — like `ConnectorContext.secret_ref` — it fails
    visibly instead of becoming a clean partial run.
    """

    def __init__(self, message: str = "", *, seconds: Optional[float] = None):
        super().__init__(message or "retry requested by upstream")
        self.seconds = None if seconds is None else _checked_seconds(seconds)


class RateLimited(RetryAfter):
    """A `RetryAfter` the upstream attributed to a rate limit.

    `scope` names the limited bucket (an endpoint, a token, a tenant) so an
    operator can tell a per-key limit from a global one.
    """

    def __init__(
        self,
        message: str = "",
        *,
        seconds: Optional[float] = None,
        scope: Optional[str] = None,
    ):
        super().__init__(message or "rate limited by upstream", seconds=seconds)
        self.scope = scope


class _NoConnectorRun(TypeError):
    """Raised when a connector's `run` returns `None` instead of a run.

    A `TypeError`, deliberately *not* a `ConnectorError`: reporting it as a
    partial run would dress a broken connector up as a bounded, resumable
    one and make it indistinguishable from a real signal, and the runner
    would re-drive the same empty frontier forever.
    """


#: What the shim converts into a partial run. Every signal above subclasses
#: `ConnectorError`, so this stays one entry on purpose: a new signal that
#: forgets to inherit from it should fail loudly rather than be swallowed here.
EARLY_EXITS = (ConnectorError,)


# --- Non-fatal results -------------------------------------------------------


@dataclass(frozen=True)
class ConnectorWarning:
    """A condition worth reporting that did not end the run.

    `code` is a stable, connector-owned token an operator can filter on;
    `message` is prose. Neither may contain a secret value.
    """

    code: str
    message: str = ""
    detail: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RejectedItem:
    """One upstream item the connector refused to turn into facts.

    A rejection is the connector's own judgement (unparseable payload,
    missing required field). It is not a narrowing or admission refusal,
    which the substrate decides after the facts are emitted.
    """

    record_id: str
    reason: str
    detail: Mapping[str, Any] = field(default_factory=dict)


# --- Context -----------------------------------------------------------------


class ConnectorMetrics:
    """Mutable counter sink for one run.

    Counters are plain numbers so they survive the trip into run health and
    operator surfaces without a serializer.
    """

    def __init__(self) -> None:
        self.counters: dict[str, float] = {}

    def increment(self, name: str, amount: float = 1.0) -> None:
        self.counters[name] = self.counters.get(name, 0.0) + amount

    def observe(self, name: str, value: float) -> None:
        self.counters[name] = value

    def as_dict(self) -> dict[str, float]:
        return dict(self.counters)


@dataclass(frozen=True)
class Checkpoint:
    """Opaque resume state for one connector.

    The substrate persists `next_cursor` and hands it back unread on the
    next run. Its shape is the connector's own contract; nothing outside
    the connector may interpret it.
    """

    next_cursor: Optional[Mapping[str, Any]] = None

    def as_cursor(self) -> Optional[dict[str, Any]]:
        return None if self.next_cursor is None else dict(self.next_cursor)


@dataclass(frozen=True)
class ConnectorContext:
    """Everything one connector run is given.

    `secret_ref` returns the *reference* to a credential — the environment
    variable name — never its value, so a reference can be logged, put in
    health, and handed to `credential_preflight` safely.

    `deadline` is a `time.monotonic()` reading, not a wall clock. Nothing
    preempts a running connector: `cancelled()` goes true and the connector
    decides where stopping is safe, then raises `Cancelled` or `Deadline`.
    """

    run_id: str
    mode: str = "cursor"
    sync_scope: Optional[Mapping[str, Any]] = None
    checkpoint: Checkpoint = field(default_factory=Checkpoint)
    config: Mapping[str, Any] = field(default_factory=dict)
    deadline: Optional[float] = None
    since_progress: Optional[Mapping[str, str]] = None
    change_probe_verdict: Optional[ProbeVerdict] = None
    authoritative_replay: bool = False
    metrics: ConnectorMetrics = field(default_factory=ConnectorMetrics)
    _secrets: Mapping[str, str] = field(default_factory=dict)
    _cancel: Optional[Callable[[], bool]] = None

    def secret_ref(self, name: str) -> str:
        """Return the declared reference for `name`, never a secret value.

        An undeclared secret raises `KeyError`, deliberately *not* a
        `ConnectorError`: it is a configuration defect that must fail
        visibly, not an early exit the shim turns into a partial run.
        """
        try:
            return self._secrets[name]
        except KeyError:
            raise KeyError(
                f"secret {name!r} is not declared for this connector; "
                f"declared: {sorted(self._secrets)}"
            ) from None

    def cancelled(self) -> bool:
        """True when the run should stop: asked to, or past its deadline."""
        if self.deadline is not None and time.monotonic() >= self.deadline:
            return True
        return bool(self._cancel()) if self._cancel is not None else False


# --- Run result --------------------------------------------------------------


@dataclass
class ConnectorRun:
    """What one connector run returns.

    A narrowing of `SourceRun`: the applied-token, bootstrap-identity and
    raw-cursor fields are substrate concerns and are not offered here.
    Every record-set authority field is kept, because dropping one would
    silently cost a connector its deletion authority.

    `warnings` and `rejected` are SDK-owned and have no `SourceRun` field;
    the shim folds their counts into run metrics.

    `image_root` is the directory the run's file image values
    (`ImageRef.file(path)`) resolve against. okg's ingest opens files only
    under it. It is carried to the runner through `SourceRun.image_root` and
    is never written to a fact or to the graph: only the relative path is.
    """

    facts: Iterable[Any]
    completed_scope: bool = False
    next_checkpoint: Optional[Checkpoint] = None
    health: Optional[SourceHealth] = None
    record_set: Optional[RecordSet] = None
    run_mode: str = "cursor"
    record_authority_stream: Optional[RecordAuthorityStream] = None
    record_set_replacement_keys: Optional[Iterable[str]] = None
    warnings: tuple[ConnectorWarning, ...] = ()
    rejected: tuple[RejectedItem, ...] = ()
    image_root: Optional[str] = None

    def __post_init__(self) -> None:
        partial = (
            self.health is not None
            and self.health.status == SOURCE_RUN_PARTIAL_STATUS
        )
        if partial and self.completed_scope:
            raise ValueError(
                "a connector run reporting health status "
                f"{SOURCE_RUN_PARTIAL_STATUS!r} did not observe its whole "
                "scope, so it cannot also set completed_scope=True; "
                "absence in a partial run is not evidence of deletion"
            )


class Connector(Protocol):
    """The contract an external connector implements.

    `profile` is one of the five source profiles. `run` receives the whole
    context as one argument, so adding an input later does not change every
    connector's signature — the lesson of the runner's six-kwarg
    signature-inspection dance.
    """

    name: str
    profile: str

    def run(self, ctx: ConnectorContext) -> ConnectorRun:
        raise NotImplementedError


# --- Adapter shim ------------------------------------------------------------


def _signal_reason(signal: BaseException) -> str:
    reason = f"{type(signal).__name__}: {signal}"
    seconds = getattr(signal, "seconds", None)
    if seconds is not None:
        reason = f"{reason} (retry after {seconds}s)"
    scope = getattr(signal, "scope", None)
    if scope:
        reason = f"{reason} (scope {scope})"
    return reason


class ConnectorAdapter:
    """Drive a `Connector` through the substrate `SourceAdapter` contract.

    `run` names all six kwargs the runner selects by signature inspection
    (`ingest/runner.py` `_adapter_run_kwargs`) so the runner passes every
    one of them, and returns a real `SourceRun`.
    """

    def __init__(
        self,
        connector: Connector,
        *,
        config: Optional[Mapping[str, Any]] = None,
        secrets: Optional[Mapping[str, str]] = None,
        deadline: Optional[float] = None,
        cancel: Optional[Callable[[], bool]] = None,
    ) -> None:
        self._connector = connector
        self._config = dict(config or {})
        self._secrets = dict(secrets or {})
        self._deadline = deadline
        self._cancel = cancel
        self.name = connector.name
        self.profile = connector.profile
        #: Counters from the most recent `run`. `SourceRun` has no metrics
        #: field, so this is where they stay observable — to the caller that
        #: built the adapter, and to tests. Without it the counters the shim
        #: writes below would be dropped on return.
        self.last_run_metrics: dict[str, float] = {}

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[Mapping[str, Any]] = None,
        since_progress: Optional[Mapping[str, str]] = None,
        sync_scope: Optional[Mapping[str, Any]] = None,
        change_probe_verdict: Optional[ProbeVerdict] = None,
        authoritative_replay: bool = False,
    ) -> SourceRun:
        prior_cursor = None if cursor is None else dict(cursor)
        metrics = ConnectorMetrics()
        ctx = ConnectorContext(
            run_id=run_id,
            mode=mode,
            sync_scope=sync_scope,
            checkpoint=Checkpoint(next_cursor=prior_cursor),
            config=self._config,
            deadline=self._deadline,
            since_progress=since_progress,
            change_probe_verdict=change_probe_verdict,
            authoritative_replay=authoritative_replay,
            metrics=metrics,
            _secrets=self._secrets,
            _cancel=self._cancel,
        )

        facts: list[Any] = []
        outcome: Optional[ConnectorRun] = None
        signal: Optional[ConnectorError] = None
        try:
            outcome = self._connector.run(ctx)
            if outcome is None:
                # `Connector.run` is typed to return a `ConnectorRun`; `None`
                # means the connector fell off the end of its own `run`. That
                # is a defect, not an early exit, and it is checked here —
                # before `outcome.facts` — so it reports itself rather than
                # surfacing as an `AttributeError` on the next line.
                raise _NoConnectorRun(
                    f"connector {self.name!r} returned None from run(); a "
                    "connector must return a ConnectorRun, or raise one of "
                    "Cancelled, Deadline, RetryAfter or RateLimited to end "
                    "the run early"
                )
            # Materialized before the SourceRun is built; see module docstring.
            facts.extend(outcome.facts)
        except EARLY_EXITS as exc:
            signal = exc

        if signal is not None:
            # No checkpoint advance: hand the runner back the cursor it gave
            # us. Record-set authority is withheld — a partial walk cannot
            # prove a record disappeared.
            metrics.increment("connector.signal")
            self.last_run_metrics = metrics.as_dict()
            return SourceRun(
                facts=facts,
                completed_scope=False,
                next_cursor=prior_cursor,
                # Both arms are live. `outcome` is None when `run` itself
                # raised, and set when a lazy fact stream raised part-way
                # through the `facts.extend` above — in which case the
                # run already declared its mode; that beats our default.
                run_mode=outcome.run_mode if outcome is not None else mode,
                # A signal raised part-way through a lazy fact stream still
                # leaves the facts collected so far, and their file image
                # values resolve against the root the run declared.
                image_root=outcome.image_root if outcome is not None else None,
                health=SourceHealth(
                    status=SOURCE_RUN_PARTIAL_STATUS,
                    reason=_signal_reason(signal),
                ),
            )

        metrics.increment("connector.warnings", len(tuple(outcome.warnings)))
        metrics.increment("connector.rejected", len(tuple(outcome.rejected)))
        self.last_run_metrics = metrics.as_dict()
        checkpoint = outcome.next_checkpoint
        return SourceRun(
            facts=facts,
            completed_scope=outcome.completed_scope,
            next_cursor=None if checkpoint is None else checkpoint.as_cursor(),
            run_mode=outcome.run_mode,
            record_set=outcome.record_set,
            record_authority_stream=outcome.record_authority_stream,
            record_set_replacement_keys=outcome.record_set_replacement_keys,
            image_root=outcome.image_root,
            health=outcome.health,
        )


__all__ = [
    "Cancelled",
    "Checkpoint",
    "Connector",
    "ConnectorAdapter",
    "ConnectorContext",
    "ConnectorError",
    "ConnectorMetrics",
    "ConnectorRun",
    "ConnectorWarning",
    "Deadline",
    "EARLY_EXITS",
    "RateLimited",
    "RejectedItem",
    "RetryAfter",
]
