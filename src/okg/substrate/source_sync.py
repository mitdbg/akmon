"""Source sync payload contracts.

Events are durable wakeups for source ingestion; they are not the
source-of-truth for whether a source changed. Source adapters still decide
what to emit from their cursor, progress markers, fingerprints, and record
sets.
"""
from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

from okg.substrate.source_run_modes import COMPLETED_SCOPE_RUN_MODES


SourceSyncTriggerKind = Literal[
    "fs_event",
    "git_ref_change",
    "webhook",
    "poll",
    "manual",
    "reconcile",
]
SourceSyncScopeKind = Literal["path", "record_id", "revision_hint"]
SourceSyncDependentFanout = Literal["none", "direct", "closure"]
SourceSyncPollProbe = Literal["always_changed", "change_probe"]

SOURCE_SYNC_COMPLETED_SCOPE_RUN_MODES: frozenset[str] = COMPLETED_SCOPE_RUN_MODES

SOURCE_SYNC_TRIGGER_KINDS: frozenset[str] = frozenset(
    (
        "fs_event",
        "git_ref_change",
        "webhook",
        "poll",
        "manual",
        "reconcile",
    )
)

SOURCE_SYNC_RUN_MODES: frozenset[str] = frozenset(
    (
        "cursor",
        "reconcile",
        "scope_complete",
        "scope_incomplete",
        "release_new",
        "release_unchanged",
        "append",
    )
)

SOURCE_SYNC_SCOPE_KINDS: frozenset[str] = frozenset(
    ("path", "record_id", "revision_hint")
)

SOURCE_SYNC_DEPENDENT_FANOUT: frozenset[str] = frozenset(
    ("none", "direct", "closure")
)

SOURCE_SYNC_POLL_PROBES: frozenset[str] = frozenset(
    ("always_changed", "change_probe")
)


def source_sync_entry_is_explicitly_dormant(entry: Any) -> bool:
    """Whether a registry entry is an inactive setup candidate.

    Only an explicit false is dormant.  The absent flag preserves the
    historical runnable-by-default behavior for hand-authored registries.
    """
    raw = getattr(entry, "raw", None)
    return (
        isinstance(raw, Mapping)
        and raw.get("enabled_by_default") is False
    )


@dataclass(frozen=True)
class SourceSyncWatch:
    """Filesystem watch hints for a registry-backed source."""

    roots: tuple[str, ...]
    include_globs: tuple[str, ...] = ("**/*",)
    exclude_globs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "roots", _coerce_str_tuple(
            self.roots, "sync.watch.roots", require_non_empty=True,
        ))
        object.__setattr__(self, "include_globs", _coerce_str_tuple(
            self.include_globs,
            "sync.watch.include_globs",
            require_non_empty=True,
        ))
        object.__setattr__(self, "exclude_globs", _coerce_str_tuple(
            self.exclude_globs,
            "sync.watch.exclude_globs",
            require_non_empty=False,
        ))

    def to_dict(self) -> dict[str, Any]:
        return {
            "roots": list(self.roots),
            "include_globs": list(self.include_globs),
            "exclude_globs": list(self.exclude_globs),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "SourceSyncWatch":
        if not isinstance(payload, Mapping):
            raise ValueError("sync.watch must be a mapping")
        return cls(
            roots=_coerce_str_tuple(
                payload.get("roots"),
                "sync.watch.roots",
                require_non_empty=True,
            ),
            include_globs=_coerce_str_tuple(
                payload.get("include_globs", ("**/*",)),
                "sync.watch.include_globs",
                require_non_empty=True,
            ),
            exclude_globs=_coerce_str_tuple(
                payload.get("exclude_globs", ()),
                "sync.watch.exclude_globs",
                require_non_empty=False,
            ),
        )


@dataclass(frozen=True)
class SourceSyncScope:
    """Advisory scope kinds a source can use to narrow execution."""

    accepts: tuple[SourceSyncScopeKind, ...] = ()
    unsupported_fallback: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "accepts", _coerce_scope_kinds(self.accepts))
        if self.unsupported_fallback is not None:
            _require_run_mode(
                self.unsupported_fallback,
                "sync.scope.unsupported_fallback",
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "accepts": list(self.accepts),
            "unsupported_fallback": self.unsupported_fallback,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "SourceSyncScope":
        if not isinstance(payload, Mapping):
            raise ValueError("sync.scope must be a mapping")
        return cls(
            accepts=_coerce_scope_kinds(payload.get("accepts", ())),
            unsupported_fallback=(
                _require_run_mode(
                    payload.get("unsupported_fallback"),
                    "sync.scope.unsupported_fallback",
                )
                if payload.get("unsupported_fallback") is not None
                else None
            ),
        )


@dataclass(frozen=True)
class SourceSyncDependents:
    """How a producer should fan out from a changed source."""

    on_event: SourceSyncDependentFanout = "closure"
    on_reconcile: SourceSyncDependentFanout = "closure"

    def __post_init__(self) -> None:
        object.__setattr__(self, "on_event", _require_dependent_fanout(
            self.on_event, "sync.dependents.on_event",
        ))
        object.__setattr__(self, "on_reconcile", _require_dependent_fanout(
            self.on_reconcile, "sync.dependents.on_reconcile",
        ))

    def to_dict(self) -> dict[str, Any]:
        return {
            "on_event": self.on_event,
            "on_reconcile": self.on_reconcile,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "SourceSyncDependents":
        if not isinstance(payload, Mapping):
            raise ValueError("sync.dependents must be a mapping")
        return cls(
            on_event=_require_dependent_fanout(
                payload.get("on_event", "closure"),
                "sync.dependents.on_event",
            ),
            on_reconcile=_require_dependent_fanout(
                payload.get("on_reconcile", "closure"),
                "sync.dependents.on_reconcile",
            ),
        )


@dataclass(frozen=True)
class SourceSyncCompaction:
    """Pending-job compaction policy for compatible source events."""

    window_seconds: int = 0
    max_events: int = 500
    max_scope_keys: int = 500

    def __post_init__(self) -> None:
        _require_non_negative_int(
            self.window_seconds,
            "sync.compaction.window_seconds",
        )
        _require_positive_int(self.max_events, "sync.compaction.max_events")
        _require_positive_int(
            self.max_scope_keys,
            "sync.compaction.max_scope_keys",
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "window_seconds": self.window_seconds,
            "max_events": self.max_events,
            "max_scope_keys": self.max_scope_keys,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "SourceSyncCompaction":
        if not isinstance(payload, Mapping):
            raise ValueError("sync.compaction must be a mapping")
        return cls(
            window_seconds=_optional_int(
                payload.get("window_seconds", 0),
                "sync.compaction.window_seconds",
            ),
            max_events=_optional_int(
                payload.get("max_events", 500),
                "sync.compaction.max_events",
            ),
            max_scope_keys=_optional_int(
                payload.get("max_scope_keys", 500),
                "sync.compaction.max_scope_keys",
            ),
        )


@dataclass(frozen=True)
class SourceSyncWebhook:
    """Generic webhook producer declaration."""

    path: str | None = None
    secret_ref: str | None = None
    signature_header: str = "X-OKG-Signature"

    def __post_init__(self) -> None:
        _validate_optional_str(self.path, "sync.webhook.path")
        _validate_optional_str(self.secret_ref, "sync.webhook.secret_ref")
        _require_non_empty_str(
            self.signature_header,
            "sync.webhook.signature_header",
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "secret_ref": self.secret_ref,
            "signature_header": self.signature_header,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "SourceSyncWebhook":
        if not isinstance(payload, Mapping):
            raise ValueError("sync.webhook must be a mapping")
        return cls(
            path=_optional_str(payload.get("path"), "sync.webhook.path"),
            secret_ref=_optional_str(
                payload.get("secret_ref"),
                "sync.webhook.secret_ref",
            ),
            signature_header=_require_non_empty_str(
                payload.get("signature_header", "X-OKG-Signature"),
                "sync.webhook.signature_header",
            ),
        )


@dataclass(frozen=True)
class SourceSyncPoll:
    """Generic poll producer declaration."""

    probe: SourceSyncPollProbe | None = None
    interval_seconds: int = 300

    def __post_init__(self) -> None:
        if self.probe is not None and self.probe not in SOURCE_SYNC_POLL_PROBES:
            allowed = ", ".join(sorted(SOURCE_SYNC_POLL_PROBES))
            raise ValueError(
                "sync.poll.probe must be one of "
                f"{allowed}; got {self.probe!r}"
            )
        _require_positive_int(
            self.interval_seconds,
            "sync.poll.interval_seconds",
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "probe": self.probe,
            "interval_seconds": self.interval_seconds,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "SourceSyncPoll":
        if not isinstance(payload, Mapping):
            raise ValueError("sync.poll must be a mapping")
        return cls(
            probe=_optional_str(payload.get("probe"), "sync.poll.probe"),
            interval_seconds=_optional_int(
                payload.get("interval_seconds", 300),
                "sync.poll.interval_seconds",
            ),
        )


@dataclass(frozen=True)
class SourceSyncCapabilities:
    """Source-level sync hints consumed by watchers and workers."""

    watch: SourceSyncWatch | None = None
    triggers: tuple[SourceSyncTriggerKind, ...] = ()
    default_event_mode: str = "cursor"
    reconcile_mode: str | None = None
    deletes_require_reconcile: bool = False
    scoped_delete_mode: str | None = None
    scoped_delete_dependent_fanout: SourceSyncDependentFanout | None = None
    cheap_probe: str | None = None
    scope: SourceSyncScope | None = None
    dependents: SourceSyncDependents = field(
        default_factory=SourceSyncDependents,
    )
    compaction: SourceSyncCompaction | None = None
    webhook: SourceSyncWebhook | None = None
    poll: SourceSyncPoll | None = None

    def __post_init__(self) -> None:
        if self.watch is not None and not isinstance(
            self.watch, SourceSyncWatch,
        ):
            raise ValueError("sync.watch must be a SourceSyncWatch")
        if self.scope is not None and not isinstance(
            self.scope, SourceSyncScope,
        ):
            raise ValueError("sync.scope must be a SourceSyncScope")
        if not isinstance(self.dependents, SourceSyncDependents):
            raise ValueError(
                "sync.dependents must be a SourceSyncDependents"
            )
        if self.compaction is not None and not isinstance(
            self.compaction, SourceSyncCompaction,
        ):
            raise ValueError("sync.compaction must be a SourceSyncCompaction")
        if self.webhook is not None and not isinstance(
            self.webhook, SourceSyncWebhook,
        ):
            raise ValueError("sync.webhook must be a SourceSyncWebhook")
        if self.poll is not None and not isinstance(
            self.poll, SourceSyncPoll,
        ):
            raise ValueError("sync.poll must be a SourceSyncPoll")
        object.__setattr__(self, "triggers", _coerce_trigger_kinds(
            self.triggers,
        ))
        _require_run_mode(self.default_event_mode, "sync.default_event_mode")
        if self.reconcile_mode is not None:
            _require_run_mode(self.reconcile_mode, "sync.reconcile_mode")
        if not isinstance(self.deletes_require_reconcile, bool):
            raise ValueError("sync.deletes_require_reconcile must be a boolean")
        if self.scoped_delete_mode is not None:
            _require_run_mode(
                self.scoped_delete_mode,
                "sync.scoped_delete_mode",
            )
        if self.scoped_delete_dependent_fanout is not None:
            _require_dependent_fanout(
                self.scoped_delete_dependent_fanout,
                "sync.scoped_delete_dependent_fanout",
            )
        _validate_optional_str(self.cheap_probe, "sync.cheap_probe")

    def to_dict(self) -> dict[str, Any]:
        return {
            "watch": self.watch.to_dict() if self.watch else None,
            "triggers": list(self.triggers),
            "default_event_mode": self.default_event_mode,
            "reconcile_mode": self.reconcile_mode,
            "deletes_require_reconcile": self.deletes_require_reconcile,
            "scoped_delete_mode": self.scoped_delete_mode,
            "scoped_delete_dependent_fanout": (
                self.scoped_delete_dependent_fanout
            ),
            "cheap_probe": self.cheap_probe,
            "scope": self.scope.to_dict() if self.scope else None,
            "dependents": self.dependents.to_dict(),
            "compaction": (
                self.compaction.to_dict() if self.compaction else None
            ),
            "webhook": self.webhook.to_dict() if self.webhook else None,
            "poll": self.poll.to_dict() if self.poll else None,
        }

    @classmethod
    def from_dict(
        cls,
        payload: Mapping[str, Any] | None,
    ) -> "SourceSyncCapabilities | None":
        if payload is None:
            return None
        if not isinstance(payload, Mapping):
            raise ValueError("sync must be a mapping")
        watch_payload = payload.get("watch")
        scope_payload = payload.get("scope")
        dependents_payload = payload.get("dependents")
        compaction_payload = payload.get("compaction")
        webhook_payload = payload.get("webhook")
        poll_payload = payload.get("poll")
        return cls(
            watch=(
                SourceSyncWatch.from_dict(watch_payload)
                if watch_payload is not None
                else None
            ),
            triggers=_coerce_trigger_kinds(payload.get("triggers", ())),
            default_event_mode=_require_run_mode(
                payload.get("default_event_mode", "cursor"),
                "sync.default_event_mode",
            ),
            reconcile_mode=(
                _require_run_mode(
                    payload.get("reconcile_mode"),
                    "sync.reconcile_mode",
                )
                if payload.get("reconcile_mode") is not None
                else None
            ),
            deletes_require_reconcile=_optional_bool(
                payload.get("deletes_require_reconcile", False),
                "sync.deletes_require_reconcile",
            ),
            scoped_delete_mode=(
                _require_run_mode(
                    payload.get("scoped_delete_mode"),
                    "sync.scoped_delete_mode",
                )
                if payload.get("scoped_delete_mode") is not None
                else None
            ),
            scoped_delete_dependent_fanout=(
                _require_dependent_fanout(
                    payload.get("scoped_delete_dependent_fanout"),
                    "sync.scoped_delete_dependent_fanout",
                )
                if payload.get("scoped_delete_dependent_fanout") is not None
                else None
            ),
            cheap_probe=_optional_str(
                payload.get("cheap_probe"),
                "sync.cheap_probe",
            ),
            scope=(
                SourceSyncScope.from_dict(scope_payload)
                if scope_payload is not None
                else None
            ),
            dependents=(
                SourceSyncDependents.from_dict(dependents_payload)
                if dependents_payload is not None
                else SourceSyncDependents()
            ),
            compaction=(
                SourceSyncCompaction.from_dict(compaction_payload)
                if compaction_payload is not None
                else None
            ),
            webhook=(
                SourceSyncWebhook.from_dict(webhook_payload)
                if webhook_payload is not None
                else None
            ),
            poll=(
                SourceSyncPoll.from_dict(poll_payload)
                if poll_payload is not None
                else None
            ),
        )


def source_sync_capabilities_from_adapter(
    adapter: Any,
) -> SourceSyncCapabilities | None:
    """Read an adapter's optional `sync_capabilities()` hook."""
    hook = getattr(adapter, "sync_capabilities", None)
    if hook is None:
        return None
    if not callable(hook):
        raise ValueError("adapter sync_capabilities must be callable")
    return _coerce_capabilities(hook())


def merge_source_sync_capabilities(
    defaults: SourceSyncCapabilities | Mapping[str, Any] | None,
    overrides: SourceSyncCapabilities | Mapping[str, Any] | None,
) -> SourceSyncCapabilities | None:
    """Return a new capability object with registry overrides applied.

    Mappings are treated as sparse overlays. `SourceSyncCapabilities`
    objects are treated as complete values.
    """
    if defaults is None and overrides is None:
        return None

    base = _capabilities_overlay(defaults, sparse=False)
    overlay = _capabilities_overlay(overrides, sparse=True)
    merged = _deep_merge_capability_dicts(base, overlay)
    return SourceSyncCapabilities.from_dict(merged)


def source_sync_events_by_source(
    events: Sequence["SourceSyncEvent"],
) -> dict[str, tuple["SourceSyncEvent", ...]]:
    """Group event payloads by registry source name."""
    grouped: dict[str, list[SourceSyncEvent]] = {}
    for event in events:
        grouped.setdefault(event.source_name, []).append(event)
    return {
        source_name: tuple(source_events)
        for source_name, source_events in grouped.items()
    }


def source_sync_scope_payload(
    source_name: str,
    events: Sequence["SourceSyncEvent"],
) -> dict[str, Any] | None:
    """Build the adapter-facing advisory scope payload for one source."""
    scoped = [event for event in events if event.source_name == source_name]
    if not scoped:
        return None
    narrowable = [
        event for event in scoped if event.trigger_kind != "reconcile"
    ]
    scope_keys = tuple(
        sorted({
            event.scope_key
            for event in narrowable
            if event.scope_key
        })
    )
    revision_hints = tuple(
        sorted({
            event.revision_hint
            for event in narrowable
            if event.revision_hint
        })
    )
    trigger_kinds = tuple(
        sorted({event.trigger_kind for event in scoped})
    )
    return {
        "source_name": source_name,
        "event_count": len(scoped),
        "scope_keys": list(scope_keys),
        "revision_hints": list(revision_hints),
        "trigger_kinds": list(trigger_kinds),
    }


def source_sync_scope_narrows(sync_scope: Mapping[str, Any] | None) -> bool:
    """Whether a source-sync scope payload narrows the run it is handed to.

    Issue #3314. The runtime worker hands every source run a payload, because
    every run carries the trigger events of its job. A payload narrows the run
    only when it names at least one non-empty scope key; that is the only
    part an adapter reads to limit what it walks (see `path_scope_requested`
    in `library/sources/_path_scope.py`, which this mirrors). A reconcile or
    poll wakeup names no scope key, so the adapter reads its whole scope and
    the run is unscoped, exactly like an event-less `okg ingest --inline`.

    Revision hints do not narrow. No adapter reads ``revision_hints``: a poll
    wakeup always carries one (the probe fingerprint) and a webhook carries
    the pushed revision, and both still mean "read the whole scope again".
    ``trigger_kinds`` and ``event_count`` are wakeup metadata. Keep the
    payload itself intact for those readers; ask this predicate before
    granting or withholding authority that depends on a narrowed run.

    Only the ``scope_keys`` spelling is read: `source_sync_scope_payload`,
    the one producer of a runner scope, always writes it. The contract
    reader also accepts ``keys`` and ``affected_scopes``; a caller that
    passes those must extend this predicate first.
    """
    if not sync_scope:
        return False
    return any(
        raw is not None and str(raw)
        for raw in sync_scope.get("scope_keys", ()) or ()
    )


@dataclass(frozen=True)
class SourceSyncEvent:
    """One source freshness wakeup.

    `scope_key` and `revision_hint` are deliberately advisory. A source may
    use them to narrow work, but it must still consult its persisted cursor
    or progress state before deciding that no facts need to be emitted.
    """

    deployment: str
    source_name: str
    trigger_kind: SourceSyncTriggerKind
    scope_key: str | None = None
    revision_hint: str | None = None
    event_id: str | None = None
    observed_at: str | None = None
    reason: str | None = None

    def __post_init__(self) -> None:
        _require_non_empty_str(self.deployment, "deployment")
        _require_non_empty_str(self.source_name, "source_name")
        _require_trigger_kind(self.trigger_kind)
        for field_name in (
            "scope_key",
            "revision_hint",
            "event_id",
            "observed_at",
            "reason",
        ):
            _validate_optional_str(getattr(self, field_name), field_name)

    def to_dict(self) -> dict[str, Any]:
        return {
            "deployment": self.deployment,
            "source_name": self.source_name,
            "trigger_kind": self.trigger_kind,
            "scope_key": self.scope_key,
            "revision_hint": self.revision_hint,
            "event_id": self.event_id,
            "observed_at": self.observed_at,
            "reason": self.reason,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True)

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "SourceSyncEvent":
        if not isinstance(payload, Mapping):
            raise ValueError("source sync event payload must be a mapping")
        return cls(
            deployment=_require_non_empty_str(
                payload.get("deployment"), "deployment",
            ),
            source_name=_require_non_empty_str(
                payload.get("source_name"), "source_name",
            ),
            trigger_kind=_require_trigger_kind(
                payload.get("trigger_kind"),
            ),
            scope_key=_optional_str(payload.get("scope_key"), "scope_key"),
            revision_hint=_optional_str(
                payload.get("revision_hint"), "revision_hint",
            ),
            event_id=_optional_str(payload.get("event_id"), "event_id"),
            observed_at=_optional_str(
                payload.get("observed_at"), "observed_at",
            ),
            reason=_optional_str(payload.get("reason"), "reason"),
        )

    @classmethod
    def from_json(cls, payload: str) -> "SourceSyncEvent":
        return cls.from_dict(json.loads(payload))


@dataclass(frozen=True)
class SourceSyncJob:
    """Durable source-sync work item carried by an ``okg_source_sync`` workflow.

    The worker may execute the same logical job more than once. Sources and
    SourceRunner must therefore remain idempotent for repeated invocations.
    """

    deployment: str
    sources: tuple[str, ...]
    run_mode: str
    events: tuple[SourceSyncEvent, ...] = ()
    include_predecessors: bool = True
    include_dependents: bool = True
    dependent_fanout: SourceSyncDependentFanout | None = None
    publish: bool = True
    profile_force_deep: bool = False
    profile_collectors: tuple[str, ...] = ()
    supersedes_workflow_id: str | None = None

    def __post_init__(self) -> None:
        _require_non_empty_str(self.deployment, "deployment")
        _require_run_mode(self.run_mode, "run_mode")
        object.__setattr__(self, "sources", _coerce_sources(self.sources))
        object.__setattr__(self, "events", _coerce_events(self.events))
        if not isinstance(self.include_predecessors, bool):
            raise ValueError("include_predecessors must be a boolean")
        if not isinstance(self.include_dependents, bool):
            raise ValueError("include_dependents must be a boolean")
        fanout = self.dependent_fanout
        if fanout is None:
            fanout = "closure" if self.include_dependents else "none"
        fanout = _require_dependent_fanout(fanout, "dependent_fanout")
        object.__setattr__(self, "dependent_fanout", fanout)
        object.__setattr__(
            self,
            "include_dependents",
            fanout != "none",
        )
        if self.supersedes_workflow_id is not None:
            _require_non_empty_str(
                self.supersedes_workflow_id, "supersedes_workflow_id"
            )
        if not isinstance(self.publish, bool):
            raise ValueError("publish must be a boolean")
        if not isinstance(self.profile_force_deep, bool):
            raise ValueError("profile_force_deep must be a boolean")
        object.__setattr__(
            self,
            "profile_collectors",
            _coerce_str_tuple(
                self.profile_collectors,
                "profile_collectors",
                require_non_empty=False,
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "deployment": self.deployment,
            "sources": list(self.sources),
            "run_mode": self.run_mode,
            "events": [event.to_dict() for event in self.events],
            "include_predecessors": self.include_predecessors,
            "include_dependents": self.include_dependents,
            "dependent_fanout": self.dependent_fanout,
            "publish": self.publish,
            "profile_force_deep": self.profile_force_deep,
            "profile_collectors": list(self.profile_collectors),
            "supersedes_workflow_id": self.supersedes_workflow_id,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True)

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "SourceSyncJob":
        if not isinstance(payload, Mapping):
            raise ValueError("source sync job payload must be a mapping")
        return cls(
            deployment=_require_non_empty_str(
                payload.get("deployment"), "deployment",
            ),
            sources=_coerce_sources(payload.get("sources")),
            run_mode=_require_run_mode(payload.get("run_mode"), "run_mode"),
            events=_coerce_events(payload.get("events", ())),
            include_predecessors=_optional_bool(
                payload.get("include_predecessors", True),
                "include_predecessors",
            ),
            include_dependents=_optional_bool(
                payload.get("include_dependents", True),
                "include_dependents",
            ),
            dependent_fanout=(
                _require_dependent_fanout(
                    payload.get("dependent_fanout"),
                    "dependent_fanout",
                )
                if payload.get("dependent_fanout") is not None
                else None
            ),
            publish=_optional_bool(payload.get("publish", True), "publish"),
            profile_force_deep=_optional_bool(
                payload.get("profile_force_deep", False),
                "profile_force_deep",
            ),
            supersedes_workflow_id=_optional_legacy_supersession_id(
                payload.get("supersedes_workflow_id"),
                "supersedes_workflow_id",
            ),
            profile_collectors=_coerce_str_tuple(
                payload.get("profile_collectors") or (),
                "profile_collectors",
                require_non_empty=False,
            ),
        )

    @classmethod
    def from_json(cls, payload: str) -> "SourceSyncJob":
        return cls.from_dict(json.loads(payload))


@dataclass(frozen=True)
class SourceSyncDesiredFrontier:
    """Read-side model for desired frontier state carried by a wakeup.

    A DBOS ``okg_source_sync`` workflow is durable admission/wakeup evidence.
    It is not completion evidence for the desired frontier; completion is
    proved later by source progress/watermark movement, Chronos apply, publish,
    and read-side visibility.
    """

    source_name: str
    run_mode: str
    workflow_id: str | None = None
    workflow_status: str | None = None
    stale: bool = False
    started_age_seconds: float | None = None

    def __post_init__(self) -> None:
        _require_non_empty_str(self.source_name, "source_name")
        _require_run_mode(self.run_mode, "run_mode")
        _validate_optional_str(self.workflow_id, "workflow_id")
        _validate_optional_str(self.workflow_status, "workflow_status")
        if not isinstance(self.stale, bool):
            raise ValueError("stale must be a boolean")
        if self.started_age_seconds is not None and not isinstance(
            self.started_age_seconds,
            (int, float),
        ):
            raise ValueError("started_age_seconds must be a number or None")

    def to_dict(self) -> dict[str, Any]:
        workflow_status = (
            str(self.workflow_status).upper()
            if self.workflow_status is not None else None
        )
        return {
            "schema": "okg.source_sync.desired_frontier.v1",
            "source_name": self.source_name,
            "state": _desired_frontier_state(
                workflow_status=workflow_status,
                stale=self.stale,
                started_age_seconds=self.started_age_seconds,
            ),
            "run_mode": self.run_mode,
            "completed_scope_run_mode": (
                self.run_mode in SOURCE_SYNC_COMPLETED_SCOPE_RUN_MODES
            ),
            "workflow_id": self.workflow_id,
            "workflow_status": workflow_status,
            "workflow_stale": self.stale,
            "started_age_seconds": self.started_age_seconds,
            "completed": False,
            "completion_state": "not_completed",
            "completion_basis": "dbos_wakeup_is_not_completion_evidence",
            "completion_requires": [
                "source_progress_or_source_watermark_completed_scope",
                "chronos_apply",
                "published_generation_visible_to_read_side",
            ],
        }


def source_sync_desired_frontier_state(
    *,
    source_name: str,
    run_mode: str,
    workflow_id: str | None = None,
    workflow_status: str | None = None,
    stale: bool = False,
    started_age_seconds: float | None = None,
) -> dict[str, Any]:
    """Return the durable desired-frontier state for one source wakeup."""
    return SourceSyncDesiredFrontier(
        source_name=source_name,
        run_mode=run_mode,
        workflow_id=workflow_id,
        workflow_status=workflow_status,
        stale=stale,
        started_age_seconds=started_age_seconds,
    ).to_dict()


def _desired_frontier_state(
    *,
    workflow_status: str | None,
    stale: bool,
    started_age_seconds: float | None,
) -> str:
    if stale:
        return "stale"
    if workflow_status == "PENDING":
        return "running" if started_age_seconds is not None else "pending"
    if workflow_status == "ENQUEUED":
        return "waiting"
    if workflow_status == "DELAYED":
        return "delayed"
    if workflow_status == "CANCELLED":
        return "superseded"
    if workflow_status in {"SUCCESS", "ERROR"}:
        return "terminal_wakeup"
    return "desired"


def _require_non_empty_str(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _validate_optional_str(value: Any, field_name: str) -> None:
    if value is None:
        return
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string or None")


def _optional_str(value: Any, field_name: str) -> str | None:
    _validate_optional_str(value, field_name)
    return value


def _optional_legacy_supersession_id(
    value: Any,
    field_name: str,
) -> str | None:
    """Decode legacy empty sentinels for the optional supersession id.

    Older source-sync workflow payloads predate the nullable supersession
    contract and serialized absence as an empty string or boolean ``False``.
    Keep all other optional-string validation strict; only normalize these
    known persisted representations so queue inspection and compaction can
    read old rows.
    """
    if value is False:
        return None
    _validate_optional_str(value, field_name)
    if isinstance(value, str) and not value.strip():
        return None
    return value


def _optional_bool(value: Any, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field_name} must be a boolean")
    return value


def _optional_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer")
    return value


def _require_positive_int(value: Any, field_name: str) -> int:
    value = _optional_int(value, field_name)
    if value <= 0:
        raise ValueError(f"{field_name} must be positive")
    return value


def _require_non_negative_int(value: Any, field_name: str) -> int:
    value = _optional_int(value, field_name)
    if value < 0:
        raise ValueError(f"{field_name} must be non-negative")
    return value


def _require_trigger_kind(value: Any) -> SourceSyncTriggerKind:
    if value not in SOURCE_SYNC_TRIGGER_KINDS:
        allowed = ", ".join(sorted(SOURCE_SYNC_TRIGGER_KINDS))
        raise ValueError(
            f"trigger_kind must be one of {allowed}; got {value!r}"
        )
    return value


def _coerce_trigger_kinds(value: Any) -> tuple[SourceSyncTriggerKind, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (_require_trigger_kind(value),)
    if not isinstance(value, Sequence) or isinstance(value, bytes):
        raise ValueError("sync.triggers must be a string or sequence")
    return tuple(_require_trigger_kind(v) for v in value)


def _require_scope_kind(value: Any) -> SourceSyncScopeKind:
    if value not in SOURCE_SYNC_SCOPE_KINDS:
        allowed = ", ".join(sorted(SOURCE_SYNC_SCOPE_KINDS))
        raise ValueError(
            f"sync.scope.accepts must be one of {allowed}; got {value!r}"
        )
    return value


def _coerce_scope_kinds(value: Any) -> tuple[SourceSyncScopeKind, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (_require_scope_kind(value),)
    if not isinstance(value, Sequence) or isinstance(value, bytes):
        raise ValueError("sync.scope.accepts must be a string or sequence")
    return tuple(_require_scope_kind(v) for v in value)


def _require_dependent_fanout(
    value: Any,
    field_name: str,
) -> SourceSyncDependentFanout:
    if value not in SOURCE_SYNC_DEPENDENT_FANOUT:
        allowed = ", ".join(sorted(SOURCE_SYNC_DEPENDENT_FANOUT))
        raise ValueError(f"{field_name} must be one of {allowed}; got {value!r}")
    return value


def source_row_candidates(
    deployment: str,
    source_name: str,
    ownership_id: str | None,
) -> tuple[str, ...]:
    """Name the rows a source's runner writes: its ``ownership_id`` (#3295).

    ``SourceRunner`` keys ``okg.source_watermarks`` and its ``sync.last_run``
    progress row by the registry ``ownership_id``, which need not be
    ``<deployment>.<source name>``. When the id is known it is the only key:
    a name-shaped fallback could match a different source whose id has that
    shape. The name candidates remain for entries without one.
    """
    if ownership_id:
        return (ownership_id,)
    return (f"{deployment}.{source_name}", source_name)


def _coerce_capabilities(value: Any) -> SourceSyncCapabilities | None:
    if value is None:
        return None
    if isinstance(value, SourceSyncCapabilities):
        return value
    if isinstance(value, Mapping):
        return SourceSyncCapabilities.from_dict(value)
    raise ValueError(
        "sync_capabilities must return SourceSyncCapabilities, mapping, or None"
    )


def _capabilities_overlay(
    value: SourceSyncCapabilities | Mapping[str, Any] | None,
    *,
    sparse: bool,
) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, SourceSyncCapabilities):
        return value.to_dict()
    if not isinstance(value, Mapping):
        raise ValueError("sync capabilities overlay must be a mapping")
    if not sparse:
        cap = SourceSyncCapabilities.from_dict(value)
        return cap.to_dict() if cap is not None else {}
    return dict(value)


def _deep_merge_capability_dicts(
    base: Mapping[str, Any],
    overlay: Mapping[str, Any],
) -> dict[str, Any]:
    merged = dict(base)
    for key, value in overlay.items():
        if (
            key in {
                "watch",
                "scope",
                "dependents",
                "compaction",
                "webhook",
                "poll",
            }
            and isinstance(value, Mapping)
            and isinstance(merged.get(key), Mapping)
        ):
            merged[key] = {**merged[key], **value}
        else:
            merged[key] = value
    return merged


def _require_run_mode(value: Any, field_name: str) -> str:
    if value not in SOURCE_SYNC_RUN_MODES:
        allowed = ", ".join(sorted(SOURCE_SYNC_RUN_MODES))
        raise ValueError(f"{field_name} must be one of {allowed}; got {value!r}")
    return value


def _coerce_str_tuple(
    value: Any,
    field_name: str,
    *,
    require_non_empty: bool,
) -> tuple[str, ...]:
    if isinstance(value, str):
        values = (_require_non_empty_str(value, field_name),)
    elif isinstance(value, Sequence) and not isinstance(value, bytes):
        values = tuple(_require_non_empty_str(v, field_name) for v in value)
    else:
        raise ValueError(f"{field_name} must be a string or sequence")
    if require_non_empty and not values:
        raise ValueError(f"{field_name} must contain at least one value")
    return values


def _coerce_sources(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        return (_require_non_empty_str(value, "sources"),)
    if not isinstance(value, Sequence):
        raise ValueError("sources must be a string or sequence of strings")
    sources = tuple(_require_non_empty_str(v, "sources") for v in value)
    if not sources:
        raise ValueError("source sync job requires at least one source")
    return sources


def _coerce_events(value: Any) -> tuple[SourceSyncEvent, ...]:
    if value is None:
        return ()
    if isinstance(value, SourceSyncEvent):
        return (value,)
    if isinstance(value, Mapping):
        return (SourceSyncEvent.from_dict(value),)
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ValueError("events must be a sequence of event payloads")
    events: list[SourceSyncEvent] = []
    for item in value:
        if isinstance(item, SourceSyncEvent):
            events.append(item)
        elif isinstance(item, Mapping):
            events.append(SourceSyncEvent.from_dict(item))
        else:
            raise ValueError(
                "events must contain SourceSyncEvent or mapping values"
            )
    return tuple(events)


__all__ = [
    "SOURCE_SYNC_COMPLETED_SCOPE_RUN_MODES",
    "SOURCE_SYNC_DEPENDENT_FANOUT",
    "SOURCE_SYNC_RUN_MODES",
    "SOURCE_SYNC_POLL_PROBES",
    "SOURCE_SYNC_SCOPE_KINDS",
    "SOURCE_SYNC_TRIGGER_KINDS",
    "SourceSyncCapabilities",
    "SourceSyncCompaction",
    "SourceSyncDependentFanout",
    "SourceSyncDependents",
    "SourceSyncDesiredFrontier",
    "SourceSyncEvent",
    "SourceSyncJob",
    "SourceSyncPoll",
    "SourceSyncPollProbe",
    "SourceSyncScope",
    "SourceSyncScopeKind",
    "SourceSyncTriggerKind",
    "SourceSyncWatch",
    "SourceSyncWebhook",
    "merge_source_sync_capabilities",
    "source_row_candidates",
    "source_sync_events_by_source",
    "source_sync_scope_narrows",
    "source_sync_scope_payload",
    "source_sync_capabilities_from_adapter",
    "source_sync_desired_frontier_state",
    "source_sync_entry_is_explicitly_dormant",
]
