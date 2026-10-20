"""Watchdog-backed observer that enqueues source-sync workflows.

Events are wakeups, not source truth. A filesystem event records an
affected source/scope and enqueues durable ingest work; SourceRunner and
the source's cursor/progress/record set decide what facts actually change.
"""
from __future__ import annotations

import fnmatch
import json
import logging
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


from okg.substrate.source_sync import (
    SourceSyncCompaction,
    SourceSyncEvent,
    SourceSyncJob,
)
from okg.substrate.source_sync_enqueue import (
    enqueue_compacted_source_sync_via_dbos,
)
from okg.substrate.db.connection import connect as db_connect


log = logging.getLogger(__name__)


@dataclass(frozen=True)
class WatchSource:
    """One source's filesystem sync scope."""

    source_name: str
    roots: tuple[Path, ...]
    include_globs: tuple[str, ...] = ("**/*",)
    exclude_globs: tuple[str, ...] = ()
    run_mode: str = "scope_complete"
    trigger_kind: str = "fs_event"
    deletes_require_reconcile: bool = False
    reconcile_mode: str | None = None
    scoped_delete_mode: str | None = None
    dependent_fanout: str = "closure"
    reconcile_dependent_fanout: str = "closure"
    scoped_delete_dependent_fanout: str | None = None
    compaction_window_seconds: int = 0
    compaction_max_events: int = 500
    compaction_max_scope_keys: int = 500


@dataclass
class WatchStats:
    """Telemetry for one Watcher run."""

    events_seen: int = 0
    events_filtered: int = 0
    #: Events put into a workflow of their own because their source is
    #: failing its record-set coverage check. Counted separately from
    #: `events_filtered` (a glob miss) so a chronically-isolated source is
    #: visible rather than indistinguishable from an uninteresting file.
    events_isolated_failing_coverage: int = 0
    ingest_jobs_enqueued: int = 0
    started_at: float = 0.0
    last_event_at: Optional[float] = None
    last_enqueue_at: Optional[float] = None

    def as_dict(self) -> dict:
        return {
            "events_seen": self.events_seen,
            "events_filtered": self.events_filtered,
            "events_isolated_failing_coverage": (
                self.events_isolated_failing_coverage
            ),
            "ingest_jobs_enqueued": self.ingest_jobs_enqueued,
            "uptime_s": (
                round(time.time() - self.started_at, 2)
                if self.started_at else 0.0
            ),
            "last_event_at": self.last_event_at,
            "last_enqueue_at": self.last_enqueue_at,
        }


# watchdog event types that can change a file's content or presence. On
# Linux, inotify also reports `opened` and `closed_no_write`, so a sync's own
# read of a watched file would queue the next sync (#3295).
_CONTENT_EVENT_TYPES = frozenset(
    {"created", "modified", "deleted", "moved", "closed"}
)


class Watcher:
    """Filesystem watcher that enqueues DBOS source-sync workflows."""

    def __init__(
        self,
        dsn: str,
        deployment_name: str,
        roots: list[Path],
        *,
        include_globs: tuple[str, ...] = ("**/*",),
        exclude_globs: tuple[str, ...] = (),
        source_scopes: tuple[WatchSource, ...] = (),
        debounce_s: float = 5.0,
        heartbeat_s: float = 60.0,
    ) -> None:
        """Filesystem watcher that enqueues DBOS source-sync work."""
        self.dsn = dsn
        self.deployment_name = deployment_name
        self.roots = [Path(r).resolve() for r in roots]
        self.include_globs = tuple(include_globs)
        self.exclude_globs = tuple(exclude_globs)
        self.source_scopes = tuple(source_scopes)
        self.debounce_s = float(debounce_s)
        self.heartbeat_s = max(float(heartbeat_s), 1.0)
        self.stats = WatchStats()
        self._pending_at: Optional[float] = None
        self._pending_events: list[SourceSyncEvent] = []
        self._pending_modes: dict[str, str] = {}
        self._pending_fanout: dict[str, str] = {}
        self._pending_compaction: dict[str, SourceSyncCompaction] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._observer = None
        self._loop_thread: Optional[threading.Thread] = None
        self._head_poller_thread: Optional[threading.Thread] = None
        self._heartbeat_thread: Optional[threading.Thread] = None
        # Per-root last-seen HEAD SHA. Populated lazily by the
        # head-poller thread; absence means "not a git repo".
        self._last_head_sha: dict[str, str] = {}

    @staticmethod
    def _glob_matches(rel: str, pattern: str) -> bool:
        """Approximate gitignore-style `**` semantics with fnmatch."""
        if fnmatch.fnmatch(rel, pattern):
            return True
        if pattern.startswith("**/") and fnmatch.fnmatch(rel, pattern[3:]):
            return True
        if "**" not in pattern:
            return False
        relaxed = (
            pattern.replace("**/", "")
            .replace("/**", "")
            .replace("**", "*")
        )
        if fnmatch.fnmatch(rel, relaxed):
            return True
        parts = rel.split("/")
        for i in range(len(parts)):
            suffix = "/".join(parts[i:])
            if fnmatch.fnmatch(suffix, relaxed):
                return True
        return fnmatch.fnmatch(parts[-1], relaxed)

    def _is_relevant(self, abs_path: str) -> bool:
        return bool(self._matching_sources(abs_path))

    def _matching_sources(
        self,
        abs_path: str,
    ) -> list[tuple[WatchSource | None, str]]:
        p = Path(abs_path)
        if self.source_scopes:
            matches: list[tuple[WatchSource | None, str]] = []
            for scope in self.source_scopes:
                for root in scope.roots:
                    try:
                        rel = p.relative_to(root)
                    except ValueError:
                        continue
                    rel_str = str(rel)
                    if any(
                        self._glob_matches(rel_str, ex)
                        for ex in scope.exclude_globs
                    ):
                        continue
                    if any(
                        self._glob_matches(rel_str, inc)
                        for inc in scope.include_globs
                    ):
                        matches.append((scope, rel_str))
                    break
            return matches

        for root in self.roots:
            try:
                rel = p.relative_to(root)
            except ValueError:
                continue
            rel_str = str(rel)
            if any(self._glob_matches(rel_str, ex) for ex in self.exclude_globs):
                return []
            if any(self._glob_matches(rel_str, inc) for inc in self.include_globs):
                return [(None, rel_str)]
            return []
        return []

    def _on_event(
        self,
        abs_path: str,
        *,
        event_type: str = "modified",
        revision_hint: str | None = None,
    ) -> None:
        self.stats.events_seen += 1
        # Suppress fsevents-side events on `.git/HEAD`. The HEAD-poller
        # is the canonical detector for ref changes (it uses
        # `event_type="head_poll"` to bypass this filter). fsevents on
        # `.git/HEAD` is unreliable on macOS because git writes that
        # file via temp + atomic rename, and even when it DOES fire,
        # the HEAD-poller will independently fire ~125 ms later — the
        # 100 ms debounce can't coalesce two events that arrive that
        # far apart, leading to ~1.4× the workflows per commit and
        # queue backup that breaks per-commit latency measurement.
        if event_type != "head_poll" and abs_path.endswith("/.git/HEAD"):
            self.stats.events_filtered += 1
            return
        matches = self._matching_sources(abs_path)
        if not matches:
            self.stats.events_filtered += 1
            return
        observed_at = datetime.now(timezone.utc).isoformat()
        pending_events: list[SourceSyncEvent] = []
        pending_modes: dict[str, str] = {}
        pending_fanout: dict[str, str] = {}
        pending_compaction: dict[str, SourceSyncCompaction] = {}
        for scope, rel_path in matches:
            if scope is None:
                continue
            run_mode = scope.run_mode
            trigger_kind = scope.trigger_kind
            fanout = scope.dependent_fanout
            if event_type in {"deleted", "moved"} and scope.scoped_delete_mode:
                run_mode = scope.scoped_delete_mode
                fanout = (
                    scope.scoped_delete_dependent_fanout
                    or scope.dependent_fanout
                )
            elif event_type in {"deleted", "moved"} and (
                scope.deletes_require_reconcile
            ):
                trigger_kind = "reconcile"
                run_mode = scope.reconcile_mode or "scope_complete"
                fanout = scope.reconcile_dependent_fanout
            pending_modes[scope.source_name] = _merge_run_mode(
                pending_modes.get(scope.source_name),
                run_mode,
            )
            pending_fanout[scope.source_name] = _merge_fanout(
                pending_fanout.get(scope.source_name),
                fanout,
            )
            if scope.compaction_window_seconds > 0:
                pending_compaction[scope.source_name] = SourceSyncCompaction(
                    window_seconds=scope.compaction_window_seconds,
                    max_events=scope.compaction_max_events,
                    max_scope_keys=scope.compaction_max_scope_keys,
                )
            pending_events.append(
                SourceSyncEvent(
                    deployment=self.deployment_name,
                    source_name=scope.source_name,
                    trigger_kind=trigger_kind,
                    scope_key=rel_path,
                    revision_hint=revision_hint,
                    event_id=f"fs:{uuid.uuid4().hex}",
                    observed_at=observed_at,
                    reason=f"watch:{event_type}",
                )
            )
        with self._lock:
            self._pending_at = time.time()
            self._pending_events.extend(pending_events)
            for source_name, run_mode in pending_modes.items():
                self._pending_modes[source_name] = _merge_run_mode(
                    self._pending_modes.get(source_name),
                    run_mode,
                )
            for source_name, fanout in pending_fanout.items():
                self._pending_fanout[source_name] = _merge_fanout(
                    self._pending_fanout.get(source_name),
                    fanout,
                )
            self._pending_compaction.update(pending_compaction)
            self.stats.last_event_at = self._pending_at

    def _make_handler(self):
        from watchdog.events import FileSystemEventHandler

        watcher = self

        class _H(FileSystemEventHandler):
            def on_any_event(self, event) -> None:
                if event.is_directory:
                    return
                event_type = getattr(event, "event_type", "modified")
                if event_type not in _CONTENT_EVENT_TYPES:
                    return
                watcher._on_event(
                    event.src_path,
                    event_type=event_type,
                )
                dest_path = getattr(event, "dest_path", None)
                if event_type == "moved" and dest_path:
                    watcher._on_event(dest_path, event_type="created")

        return _H()

    def _debounce_loop(self) -> None:
        while not self._stop.is_set():
            self._flush_pending_if_ready(time.time())
            self._stop.wait(timeout=min(self.debounce_s, 0.5))

    def _heartbeat_loop(self) -> None:
        while not self._stop.wait(timeout=self.heartbeat_s):
            self.record_heartbeat()

    def _flush_pending_if_ready(self, now: float) -> None:
        with self._lock:
            pending = self._pending_at
        if pending is None or now - pending < self.debounce_s:
            return
        self._enqueue_ingest()

    def _sources_to_exclude(self) -> frozenset[str]:
        """Sources to keep out of this enqueue.

        A source that is failing its record-set coverage check must not be
        bundled into a multi-source workflow: if that workflow dies partway,
        the incomplete checkpoint records an exact repair contract naming
        every source in the bundle, and every later workflow refuses with a
        contract mismatch until an operator intervenes (observed on a live
        deployment, 2026-09-04).

        A read that FAILS returns the empty set and logs why. That is a
        deliberate choice between two bad options: refusing to enqueue on a
        transient database error would stop ingest entirely for a protection
        that is only advisory, so the watcher proceeds unfiltered and says
        so rather than pretending it checked.
        """
        try:
            from okg.substrate.ingest.record_set_coverage_state import (
                sources_failing_record_set_coverage,
            )

            return frozenset(sources_failing_record_set_coverage(
                dsn=self.dsn,
                deployment=self.deployment_name,
            ))
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "watch: could not read record-set coverage state (%s); "
                "enqueuing WITHOUT the failing-source exclusion",
                exc,
            )
            return frozenset()

    def _plan_enqueues(self, groups):
        """Turn debounced event groups into the workflows to enqueue.

        A source that is failing its record-set coverage check is split out
        into a workflow OF ITS OWN rather than dropped:

        * Out of the bundle, because a multi-source workflow that dies partway
          records an exact repair contract naming every source it was running.
          A source that cannot pass makes that contract unsatisfiable, and
          every later workflow then refuses (observed live, 2026-09-04).
        * Still enqueued, because the coverage flag clears only on a
          successful run of that source, and the watcher is what enqueues it.
          Dropping the events would make the exclusion self-perpetuating: the
          source could never run, so it could never clear its own flag.

        A single-source workflow that fails leaves a repair contract naming
        only itself, which blocks nothing else.
        """
        excluded = self._sources_to_exclude()
        plans = []
        for (run_mode, fanout), group_events in groups.items():
            sources = tuple(sorted({
                event.source_name for event in group_events
            }))
            isolated = tuple(s for s in sources if s in excluded)
            kept = tuple(s for s in sources if s not in excluded)
            if kept:
                plans.append((
                    run_mode,
                    fanout,
                    kept,
                    [e for e in group_events if e.source_name not in excluded],
                ))
            for name in isolated:
                own_events = [
                    e for e in group_events if e.source_name == name
                ]
                self.stats.events_isolated_failing_coverage += len(own_events)
                log.warning(
                    "watch: %s is failing its record-set coverage check; "
                    "enqueuing its %d event(s) alone instead of bundling "
                    "them with %s",
                    name, len(own_events), kept or "(nothing)",
                )
                plans.append((run_mode, fanout, (name,), own_events))
        return plans

    def _enqueue_ingest(self) -> bool:
        with self._lock:
            events = tuple(self._pending_events)
            modes = dict(self._pending_modes)
            fanout_by_source = dict(self._pending_fanout)
            compaction_by_source = dict(self._pending_compaction)
            self._pending_at = None
            self._pending_events = []
            self._pending_modes = {}
            self._pending_fanout = {}
            self._pending_compaction = {}
        if not events:
            return True
        groups = _group_events_by_run_mode_and_fanout(
            events,
            modes,
            fanout_by_source,
        )
        plans = self._plan_enqueues(groups)
        enqueued = 0
        try:
            # Filesystem events are advisory scope, so repeated debounce
            # batches compact into queued DBOS rows instead of being dropped by
            # queue dedupe.
            for run_mode, fanout, sources, group_events in plans:
                compaction = _compaction_for_sources(
                    sources,
                    compaction_by_source,
                )
                dedupe_window = (
                    compaction.window_seconds
                    if compaction else max(int(self.debounce_s), 1)
                )
                bucket = int(time.time() / max(dedupe_window, 1))
                job = SourceSyncJob(
                    deployment=self.deployment_name,
                    sources=sources,
                    run_mode=run_mode,
                    events=tuple(group_events),
                    dependent_fanout=fanout,
                    publish=True,
                )
                source_key = ",".join(sources)
                dedupe = (
                    f"sync:{self.deployment_name}:watch:{run_mode}:"
                    f"{source_key}:{fanout}:{bucket}"
                )
                workflow_id = enqueue_compacted_source_sync_via_dbos(
                    deployment=self.deployment_name,
                    dsn=self.dsn,
                    job=job,
                    deduplication_id=(
                        dedupe if compaction is not None else None
                    ),
                    compaction=compaction,
                )
                log.info(
                    "watch enqueue (DBOS): workflow_id=%s "
                    "sources=%s run_mode=%s fanout=%s "
                    "n_events=%d",
                    workflow_id, sources, run_mode, fanout,
                    len(group_events),
                )
                self.stats.last_enqueue_at = time.time()
                enqueued += 1
                self._record_watch_health(
                    sources,
                    enqueue_count=1,
                    error_count=0,
                    event_count=len(group_events),
                )
        except Exception as e:
            log.warning(
                "watch: enqueue failed (%s); will retry on next "
                "debounce cycle",
                e,
            )
            with self._lock:
                self._pending_events = list(events) + self._pending_events
                for source_name, run_mode in modes.items():
                    self._pending_modes[source_name] = _merge_run_mode(
                        self._pending_modes.get(source_name),
                        run_mode,
                    )
                for source_name, fanout in fanout_by_source.items():
                    self._pending_fanout[source_name] = _merge_fanout(
                        self._pending_fanout.get(source_name),
                        fanout,
                    )
                self._pending_compaction.update(compaction_by_source)
                self._pending_at = time.time()
            self._record_watch_health(
                sorted({event.source_name for event in events}),
                enqueue_count=0,
                error_count=1,
                event_count=len(events),
            )
            return False
        # Count what was actually enqueued. `len(plans)` would report a
        # job for a group the loop never reached after a partial failure.
        self.stats.ingest_jobs_enqueued += enqueued
        return True

    def record_heartbeat(self) -> None:
        """Write a liveness heartbeat for all scoped watch sources."""
        source_names = tuple(sorted({
            scope.source_name for scope in self.source_scopes
        }))
        self._record_watch_health(
            source_names,
            enqueue_count=0,
            error_count=0,
            event_count=0,
        )

    def _record_watch_health(
        self,
        source_names: list[str] | tuple[str, ...],
        *,
        enqueue_count: int,
        error_count: int,
        event_count: int | None = None,
    ) -> None:
        if not source_names:
            return
        now = datetime.now(timezone.utc).isoformat()
        value = json.dumps({
            "last_run_at": now,
            "last_heartbeat_at": now,
            "last_event_at": (
                datetime.fromtimestamp(
                    self.stats.last_event_at,
                    tz=timezone.utc,
                ).isoformat()
                if self.stats.last_event_at else None
            ),
            "last_enqueue_at": (
                datetime.fromtimestamp(
                    self.stats.last_enqueue_at,
                    tz=timezone.utc,
                ).isoformat()
                if self.stats.last_enqueue_at else None
            ),
            "enqueue_count": enqueue_count,
            "error_count": error_count,
            "event_count": event_count,
        }, sort_keys=True)
        try:
            with db_connect(self.dsn, autocommit=True) as conn, \
                 conn.cursor() as cur:
                for source_name in source_names:
                    cur.execute(
                        """
                        INSERT INTO okg.source_progress
                          (deployment, source_name, key, value, updated_at)
                        VALUES (%s, %s, %s, %s, now())
                        ON CONFLICT (deployment, source_name, key)
                        DO UPDATE SET value = EXCLUDED.value,
                                      updated_at = EXCLUDED.updated_at
                        """,
                        (
                            self.deployment_name,
                            source_name,
                            "sync.producer.watch",
                            value,
                        ),
                    )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "watch: failed to record producer health for deployment=%s "
                "sources=%s: %s",
                self.deployment_name,
                ",".join(source_names),
                exc,
            )

    def _git_repo_roots(self) -> list[tuple[WatchSource | None, Path]]:
        """Return (scope, root_path) pairs for every watched root that
        contains a git repository (a ``.git`` dir or worktree pointer
        file). Used by the HEAD poller to know what to watch.
        """
        out: list[tuple[WatchSource | None, Path]] = []
        if self.source_scopes:
            for scope in self.source_scopes:
                for root in scope.roots:
                    if (root / ".git").exists():
                        out.append((scope, root))
        else:
            for root in self.roots:
                if (root / ".git").exists():
                    out.append((None, root))
        return out

    def _head_poller_loop(self) -> None:
        """Poll ``git rev-parse HEAD`` every 250 ms on every watched
        root that's a git repo. On HEAD advance, synthesize a fs-event
        on ``<root>/.git/HEAD`` so the existing event pipeline (filter
        by include_globs, debounce, enqueue) picks it up.

        Background: macOS fsevents on ``.git/HEAD`` is unreliable —
        ``git reset --hard`` atomically writes the file via temp +
        rename, which fsevents may report as a rename of an *inode*
        rather than a content change of the path. For zero-tree-change
        commits (clean merge commits whose tree matches the parent),
        the working tree doesn't change at all, so we lose the only
        non-``.git/HEAD`` signal. Result: the watcher misses the
        commit entirely and the system waits up to 60 s for the
        ``okg_source_poll_*`` cron to notice.

        This poller is the substrate's ground truth for "did the
        repo's HEAD move?". Cheap (~5 ms per ``git rev-parse``),
        reliable, no fsevents quirks. 250 ms cadence puts the
        worst-case latency for a zero-tree commit at ~250 ms
        instead of 60 s.
        """
        roots = self._git_repo_roots()
        if not roots:
            return  # no git repos in watched roots; nothing to do
        # Seed last-seen HEAD so a stable boot doesn't fire a
        # spurious "HEAD changed" event on first poll.
        for _scope, root in roots:
            sha = self._read_head_sha(root)
            if sha is not None:
                self._last_head_sha[str(root)] = sha
        while not self._stop.wait(timeout=0.25):
            for _scope, root in roots:
                sha = self._read_head_sha(root)
                if sha is None:
                    continue
                prev = self._last_head_sha.get(str(root))
                if prev == sha:
                    continue
                self._last_head_sha[str(root)] = sha
                # Synthesize a fs-event on .git/HEAD. The existing
                # `_on_event` path filters by include_globs; only
                # source_scopes that include ".git/HEAD" will see
                # this event. Other deployments are unaffected.
                head_path = str(root / ".git" / "HEAD")
                log.debug(
                    "head-poller: %s advanced %s -> %s; "
                    "synthesizing event at %s",
                    root, (prev or "")[:10], sha[:10], head_path,
                )
                # Use event_type='head_poll' so `_on_event` bypasses
                # the fsevents-on-.git/HEAD suppression filter. This
                # is the only path through which `.git/HEAD` events
                # reach the source-sync queue.
                self._on_event(
                    head_path,
                    event_type="head_poll",
                    revision_hint=sha,
                )

    @staticmethod
    def _read_head_sha(root: Path) -> str | None:
        """Best-effort ``git rev-parse HEAD``. Returns None if the
        directory isn't a git repo or git is missing."""
        try:
            out = subprocess.check_output(
                ["git", "-C", str(root), "rev-parse", "HEAD"],
                stderr=subprocess.DEVNULL,
                text=True,
                timeout=2.0,
            )
            return out.strip() or None
        except (subprocess.CalledProcessError, FileNotFoundError,
                subprocess.TimeoutExpired):
            return None

    def start(self) -> None:
        from watchdog.observers import Observer

        self.stats.started_at = time.time()
        self._observer = Observer()
        handler = self._make_handler()
        for root in self.roots:
            if root.is_dir():
                self._observer.schedule(
                    handler, str(root), recursive=True,
                )
        self._observer.start()
        self._loop_thread = threading.Thread(
            target=self._debounce_loop,
            name="okg-watch-debounce",
            daemon=True,
        )
        self._loop_thread.start()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            name="okg-watch-heartbeat",
            daemon=True,
        )
        self._heartbeat_thread.start()
        # Only spawn the HEAD-poller thread if any watched root is a
        # git repo. Non-git deployments pay zero cost.
        git_roots = self._git_repo_roots()
        if git_roots:
            log.info(
                "watcher: starting head-poller for %d git repo root(s): %s",
                len(git_roots),
                [str(r) for _s, r in git_roots],
            )
            self._head_poller_thread = threading.Thread(
                target=self._head_poller_loop,
                name="okg-watch-head-poller",
                daemon=True,
            )
            self._head_poller_thread.start()
        else:
            log.debug(
                "watcher: no git repo roots; head-poller not started",
            )
        self._record_watch_health(
            sorted({scope.source_name for scope in self.source_scopes}),
            enqueue_count=0,
            error_count=0,
            event_count=0,
        )

    def stop(self, *, timeout_s: float = 10.0) -> None:
        """Stop every watcher thread within one shared monotonic budget.

        The observer can legitimately need more than the old fixed three-second
        join on a busy filesystem.  Giving each thread a separate three-second
        wait both starved the observer and could exceed the supervisor's total
        shutdown window.  Signal all loops first, then spend one caller-owned
        budget across their joins.
        """
        self._stop.set()
        if self._observer is not None:
            self._observer.stop()
        deadline = time.monotonic() + max(0.0, timeout_s)
        threads = (
            self._observer,
            self._loop_thread,
            self._head_poller_thread,
            self._heartbeat_thread,
        )
        for thread in threads:
            if thread is not None:
                thread.join(timeout=max(0.0, deadline - time.monotonic()))

    def run_forever(self) -> None:
        """Blocking convenience wrapper. Signal handling lives in the CLI."""
        self.start()
        try:
            while not self._stop.is_set():
                time.sleep(0.5)
        finally:
            self.stop()


def _group_events_by_run_mode_and_fanout(
    events: tuple[SourceSyncEvent, ...],
    modes: dict[str, str],
    fanout_by_source: dict[str, str],
) -> dict[tuple[str, str], list[SourceSyncEvent]]:
    groups: dict[tuple[str, str], list[SourceSyncEvent]] = {}
    for event in events:
        run_mode = modes.get(event.source_name) or "scope_complete"
        fanout = fanout_by_source.get(event.source_name) or "closure"
        groups.setdefault((run_mode, fanout), []).append(event)
    return groups


_RUN_MODE_PRIORITY = {
    "append": 0,
    "cursor": 1,
    "scope_incomplete": 1,
    "release_unchanged": 1,
    "release_new": 2,
    "reconcile": 3,
    "scope_complete": 4,
}


def _merge_run_mode(existing: str | None, new: str) -> str:
    if not existing:
        return new
    if _RUN_MODE_PRIORITY.get(new, 0) > _RUN_MODE_PRIORITY.get(existing, 0):
        return new
    return existing


_FANOUT_PRIORITY = {
    "none": 0,
    "direct": 1,
    "closure": 2,
}


def _merge_fanout(existing: str | None, new: str) -> str:
    if not existing:
        return new
    if _FANOUT_PRIORITY.get(new, 0) > _FANOUT_PRIORITY.get(existing, 0):
        return new
    return existing


def _compaction_for_sources(
    sources: tuple[str, ...],
    compaction_by_source: dict[str, SourceSyncCompaction],
) -> SourceSyncCompaction | None:
    policies = [
        compaction_by_source[source]
        for source in sources
        if source in compaction_by_source
    ]
    if not policies:
        return None
    return SourceSyncCompaction(
        window_seconds=max(policy.window_seconds for policy in policies),
        max_events=min(policy.max_events for policy in policies),
        max_scope_keys=min(policy.max_scope_keys for policy in policies),
    )


__all__ = ["Watcher", "WatchSource", "WatchStats"]
