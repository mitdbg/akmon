"""Codex session transcript source.

Codex stores session rollouts as JSONL under ``~/.codex/sessions``.
This adapter emits those transcripts through the existing agent session
ontology so okg-workspace graphs can connect Codex work to files, tools,
approvals, and later code symbols.
"""
from __future__ import annotations

import hashlib
import fnmatch
import os
import re
import json
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Optional

from okg.substrate.library.sources.base import (
    APPLIED_TOKEN_BINDING_SEALED_SNAPSHOT,
    APPLIED_TOKEN_BINDING_UNVERIFIED,
    BOOTSTRAP_SNAPSHOT_MATERIALIZATION,
    BootstrapIdentityClaim,
    EdgeFact,
    EdgeKey,
    NodeFact,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceHealth,
    SourceRun,
    cursor_has_record_set_authority,
    preserve_cursor_record_set_authority,
)
from okg.substrate.library.sources._path_scope import (
    path_scope_requested,
    resolve_scoped_paths,
)
from okg.substrate.library.sources._claude_project_scope import (
    is_repo_local_claude_worktree_cwd,
)
from okg.substrate.library.sources._mcp_tool_telemetry import (
    McpToolTelemetry,
    parse_mcp_namespace,
    parse_mcp_tool_call,
)
from okg.substrate.library.sources._transcript_redaction import scrub_transcript_text
from okg.substrate.library.sources import _transcript_resume
from okg.substrate.library.sources.change_probe import (
    ChangeProbeError,
    ProbeVerdict,
    cursor_has_outstanding_work,
)
from okg.substrate.library.sources.content_hash_probe import ContentHashProbe


__all__ = ["CodexSessionsSource"]


_PATH_RE = re.compile(
    r"(?P<path>(?:[./~]?[\w.-]+/)+[\w.@%+=:, -]+(?:\.[A-Za-z0-9_+-]+)?)"
)
_PATCH_FILE_RE = re.compile(
    r"^\*\*\* (?:Add|Update|Delete) File: (?P<path>.+)$",
    flags=re.MULTILINE,
)
_REPO_FILE_SCAN_SKIP_DIRS = {
    ".git",
    ".claude",
    ".mypy_cache",
    ".pytest_cache",
    ".venv",
    "__pycache__",
    "node_modules",
}
_UNSET = object()
# v2 adds chunked-ingest fields (consumed-prefix `complete`, carried
# `ambiguous_mcp_operators` / `pending_tool_events`). A v1 cursor is not
# read: it predates chunking, so it is safely re-parsed from zero rather
# than misinterpreted as a consumed prefix.
_APPEND_CURSOR_SCHEMA = "okg.codex_sessions.append_cursor.v2"
_REPLACEMENT_EPOCH_SCHEMA = "okg.codex_sessions.replacement_epoch.v1"
_REPLACEMENT_EPOCHS_KEY = "replacement_epochs"
_OWNED_FOOTPRINT_KEY = "owned_footprint"
_TRANSCRIPT_CONTENT_MANIFEST_KEY = "transcript_content_manifest"
# Ceiling on tool calls held open across a chunk boundary. Outputs follow
# their calls closely, so this is normally 0-2; the cap keeps a transcript
# whose outputs never arrive from growing the cursor without bound. Overflow
# is counted into run stats rather than dropped silently.
_MAX_PENDING_TOOL_EVENTS = 512
_TRANSCRIPT_READ_CHUNK_BYTES = 1024 * 1024
# Same window as the shared content probe: a file whose ctime is this recent
# may have been rewritten within one timestamp tick, so one pass is not proof.
_TRANSCRIPT_TIMESTAMP_COLLISION_WINDOW_NS = 2_000_000_000


@dataclass(frozen=True)
class _PrefixPass:
    digest: str
    bytes_read: int
    before: os.stat_result
    after: os.stat_result
    current: os.stat_result
    started_ns: int


def _read_transcript_prefix_pass(
    path: Path | str,
    *,
    limit: int | None,
) -> _PrefixPass:
    """Hash ``[0, N)`` from one handle; N is fixed before the first read.

    N is ``limit`` when given, else the size ``fstat`` reports on open. Bytes a
    writer appends after that are never read, so a live session's growth does
    not tear the digest.
    """
    import time

    digest = hashlib.sha256()
    bytes_read = 0
    started_ns = time.time_ns()
    with open(path, "rb") as handle:
        before = os.fstat(handle.fileno())
        target = int(before.st_size) if limit is None else int(limit)
        while bytes_read < target:
            chunk = handle.read(
                min(_TRANSCRIPT_READ_CHUNK_BYTES, target - bytes_read)
            )
            if not chunk:
                break
            digest.update(chunk)
            bytes_read += len(chunk)
        after = os.fstat(handle.fileno())
    current = os.stat(path)
    return _PrefixPass(
        digest=digest.hexdigest(),
        bytes_read=bytes_read,
        before=before,
        after=after,
        current=current,
        started_ns=started_ns,
    )


def _same_file_not_shorter(scan: _PrefixPass, size: int) -> bool:
    files = {
        (int(st.st_dev), int(st.st_ino))
        for st in (scan.before, scan.after, scan.current)
    }
    return (
        len(files) == 1
        and scan.bytes_read == size
        and all(
            int(st.st_size) >= size
            for st in (scan.before, scan.after, scan.current)
        )
    )


def _stat_unchanged(scan: _PrefixPass) -> bool:
    def identity(st: os.stat_result) -> tuple[int, int, int, int, int]:
        return (
            int(st.st_dev),
            int(st.st_ino),
            int(st.st_size),
            int(st.st_mtime_ns),
            int(st.st_ctime_ns),
        )

    return (
        identity(scan.before) == identity(scan.after) == identity(scan.current)
    )


def _append_tolerant_prefix_sha(
    path: Path | str,
    *,
    limit: int | None = None,
) -> tuple[str, int]:
    """Identity of a transcript's ``[0, N)`` while a writer may append.

    The shared content probe refuses a file whose size or mtime moved during
    its read, so a session that appends every few milliseconds never gets a
    token (#2469). Codex rollouts are append-only. This reader fixes N before
    reading and tolerates growth past N, but keeps the shared probe's guard
    against rewrites: when the stat moved during the pass, or the file was
    touched within one timestamp tick, it reads ``[0, N)`` a second time from
    a fresh handle and requires the same digest. A rewrite inside the prefix,
    a truncation below N, or a replaced file raises ``ChangeProbeError``.
    """
    first = _read_transcript_prefix_pass(path, limit=limit)
    size = int(first.before.st_size) if limit is None else int(limit)
    if not _same_file_not_shorter(first, size):
        raise ChangeProbeError(
            "transcript prefix changed while identity was being read"
        )
    recent = (
        int(first.before.st_ctime_ns)
        >= first.started_ns - _TRANSCRIPT_TIMESTAMP_COLLISION_WINDOW_NS
    )
    if _stat_unchanged(first) and not recent:
        return first.digest, size
    second = _read_transcript_prefix_pass(path, limit=size)
    if (
        not _same_file_not_shorter(second, size)
        or (int(second.before.st_dev), int(second.before.st_ino))
        != (int(first.before.st_dev), int(first.before.st_ino))
        or second.digest != first.digest
    ):
        raise ChangeProbeError(
            "transcript prefix changed while identity was being read"
        )
    return first.digest, size


class _AppendTolerantTranscriptProbe(ContentHashProbe):
    """The transcript half of the Codex change probe.

    Identical to ``ContentHashProbe`` (same membership, item digests over
    whole files, token composition) except that each transcript is hashed by
    ``_append_tolerant_prefix_sha``: a transcript growing during the probe
    yields the identity of the bytes present when it was opened instead of a
    probe error. The appended bytes make the next probe differ, so growth is
    still seen as a change.

    It hashes whole files by path only. ``max_bytes_per_item`` and
    ``read_chunk_bytes`` are refused, and so is a bytes or callable item,
    because this reader would otherwise silently ignore them.
    """

    def __init__(self, **kwargs: Any) -> None:
        unsupported = sorted(
            name
            for name in ("max_bytes_per_item", "read_chunk_bytes")
            if name in kwargs
        )
        if unsupported:
            raise TypeError(
                "_AppendTolerantTranscriptProbe does not support "
                + ", ".join(unsupported)
            )
        super().__init__(**kwargs)

    def _content_component(self) -> dict[str, Any]:
        self._last_item_hashes = None
        self._last_item_sizes = None
        items = sorted(
            ((str(key), src) for key, src in self._content_items()),
            key=lambda kv: kv[0],
        )
        digest = hashlib.sha256()
        item_hashes: dict[str, str] = {}
        item_sizes: dict[str, int] = {}
        digest.update(f"n={len(items)}".encode("utf-8"))
        for key, src in items:
            if key in item_hashes:
                raise ChangeProbeError(
                    "content item keys must be unique within the probe scope"
                )
            if not isinstance(src, (str, Path)):
                raise TypeError(
                    "_AppendTolerantTranscriptProbe hashes file paths only; "
                    f"item {key!r} is a {type(src).__name__}"
                )
            try:
                item_hash, item_size = _append_tolerant_prefix_sha(src)
            except ChangeProbeError:
                raise
            except Exception as exc:  # noqa: BLE001 - unknown identity
                raise ChangeProbeError(
                    "content item path could not be read"
                ) from exc
            item_hashes[key] = item_hash
            item_sizes[key] = item_size
            digest.update(b"\x00")
            digest.update(key.encode("utf-8"))
            digest.update(b"\x00")
            digest.update(item_hash.encode("utf-8"))
        self._last_item_hashes = item_hashes
        self._last_item_sizes = item_sizes
        return {"kind": "content_hash", "value": digest.hexdigest()}


@dataclass(frozen=True)
class _ToolEvent:
    event_id: str
    session_id: str
    tool_name: str
    turn_index: int
    block_index: int
    timestamp: str
    arguments: dict[str, Any]
    file_paths: tuple[str, ...]
    command: str = ""
    input_excerpt: str = ""
    message_index: int | None = None
    mcp_server: str = ""
    mcp_deployment: str = ""
    mcp_operator: str = ""
    # Native error signal from the matching function_call_output
    # (reduce-agent-friction req.friction.event-observability). None =
    # no output observed in this scan chunk — an honest unknown.
    is_error: bool | None = None
    exit_code: int | None = None


@dataclass(frozen=True)
class _MessageEvent:
    role: str
    message_index: int
    line_index: int
    timestamp: str
    message_kind: str
    content_hash: str
    content_excerpt: str
    tool_call_count: int = 0
    parent_message_id: str = ""

    def to_cursor(self) -> dict[str, Any]:
        return {
            "role": self.role,
            "message_index": self.message_index,
            "line_index": self.line_index,
            "timestamp": self.timestamp,
            "message_kind": self.message_kind,
            "content_hash": self.content_hash,
            "content_excerpt": self.content_excerpt,
            "tool_call_count": self.tool_call_count,
            "parent_message_id": self.parent_message_id,
        }

    @classmethod
    def from_cursor(cls, raw: Any) -> "_MessageEvent | None":
        if not isinstance(raw, dict):
            return None
        try:
            return cls(
                role=str(raw.get("role") or "assistant"),
                message_index=int(raw.get("message_index")),
                line_index=int(raw.get("line_index") or 0),
                timestamp=str(raw.get("timestamp") or ""),
                message_kind=str(raw.get("message_kind") or "assistant_text"),
                content_hash=str(raw.get("content_hash") or ""),
                content_excerpt=str(raw.get("content_excerpt") or ""),
                tool_call_count=int(raw.get("tool_call_count") or 0),
                parent_message_id=str(raw.get("parent_message_id") or ""),
            )
        except (TypeError, ValueError):
            return None


@dataclass(frozen=True)
class _TranscriptParseState:
    bytes: int
    mtime_ns: int
    line_count: int
    last_line_hash: str
    meta: dict[str, Any]
    started_at: str
    ended_at: str
    message_count: int
    turn_count: int
    tool_event_count: int
    permission_event_count: int
    denial_count: int
    interrupt_count: int
    block_index: int
    last_message: _MessageEvent | None
    append_safe: bool
    usage: dict[str, Any] = field(default_factory=dict)
    mcp_tool_registry: dict[str, McpToolTelemetry] = field(default_factory=dict)
    tool_error_count: int = 0
    # Carried across chunk boundaries so a chunked parse emits the same facts
    # as one full parse (design-review-1 P2-1).
    ambiguous_mcp_operators: tuple[str, ...] = ()
    # call_id -> the tool event awaiting its function_call_output. The EVENT is
    # carried, not merely its id: when the output lands in a later chunk the
    # event must be re-emitted carrying is_error/exit_code, or a chunked parse
    # permanently loses a stamp that a full parse would have applied. Node ids
    # are deterministic, so re-emission upserts the same node.
    pending_tool_events: dict[str, dict[str, Any]] = field(default_factory=dict)
    # True once the whole file has been consumed. False means a budget
    # stopped the parse and the transcript still has outstanding backlog.
    complete: bool = True


@dataclass(frozen=True)
class _ReplacementEpoch:
    """Ownership held while one transcript is rebuilt from offset zero."""

    old: RecordEmission | None = None
    cleanup: RecordEmission | None = None
    current: RecordEmission | None = None
    sequence: int = 1

    def restart(self) -> "_ReplacementEpoch":
        return _ReplacementEpoch(
            old=self.old,
            cleanup=_merge_optional_emissions(self.cleanup, self.current),
            current=None,
            sequence=self.sequence + 1,
        )

    def with_current(self, current: RecordEmission) -> "_ReplacementEpoch":
        return replace(self, current=current)

    def to_cursor(self) -> dict[str, Any]:
        return {
            "schema": _REPLACEMENT_EPOCH_SCHEMA,
            "sequence": self.sequence,
            "old": self.old.to_cursor() if self.old is not None else None,
            "cleanup": (
                self.cleanup.to_cursor() if self.cleanup is not None else None
            ),
            "current": (
                self.current.to_cursor() if self.current is not None else None
            ),
        }

    @classmethod
    def from_cursor(cls, raw: Any) -> "_ReplacementEpoch | None":
        if (
            not isinstance(raw, dict)
            or raw.get("schema") != _REPLACEMENT_EPOCH_SCHEMA
        ):
            return None
        try:
            return cls(
                old=_optional_record_emission_from_cursor(raw.get("old")),
                cleanup=_optional_record_emission_from_cursor(
                    raw.get("cleanup"),
                ),
                current=_optional_record_emission_from_cursor(
                    raw.get("current"),
                ),
                sequence=max(1, int(raw.get("sequence") or 1)),
            )
        except (KeyError, TypeError, ValueError):
            return None


@dataclass
class CodexSessionsSource:
    repo: str
    codex_sessions_root: Path | str = "~/.codex/sessions"
    archived_sessions_root: Path | str | None = "~/.codex/archived_sessions"
    cwd_filter: Optional[list[Path | str]] = None
    cwd_globs: Optional[list[str]] = None
    repo_root: Path | str | None = None
    repo_file_include_untracked: bool = True
    repo_file_exclude_globs: tuple[str, ...] = ()
    since: str = "90d"
    capture_tool_use_events: Optional[list[str]] = None
    # Per-RUN ingest budget, not a per-file exclusion threshold: a transcript
    # larger than this is consumed across successive runs rather than dropped.
    # Operator dial trading catch-up latency against per-run cost.
    max_transcript_bytes: int = 25_000_000
    # Companion ceiling on parsed transcript records per run (design §6);
    # bytes bound the read, records bound the publish, and a run stops at
    # whichever binds first. Counted in RECORDS (messages + tool events), not
    # emitted nodes/edges, so the cap and the charge are the same unit -- one
    # record fans out to a node plus a content-dependent number of edges.
    max_transcript_records: int = 200_000

    name: str = "codex_sessions"
    profile: str = "discovery_crawl"
    probe_short_circuit_safe = True
    # Incremental-source contract (issue #323): HYBRID (repo files + external
    # JSONL transcripts under two roots) — UNION of a git scope probe and a
    # bounded-memory complete-content probe over the transcripts.
    change_probe_kind = "union"
    bootstrap_cursor_semantics = _APPEND_CURSOR_SCHEMA
    last_run_stats: dict[str, Any] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        self.codex_sessions_root = Path(self.codex_sessions_root).expanduser()
        if self.archived_sessions_root is not None:
            self.archived_sessions_root = Path(self.archived_sessions_root).expanduser()
        if self.repo_root is not None:
            self.repo_root = Path(self.repo_root).expanduser().resolve()
        if self.cwd_filter:
            self.cwd_filter = [
                Path(p).expanduser().resolve() if isinstance(p, str) else p.expanduser().resolve()
                for p in self.cwd_filter
            ]
        if self.cwd_globs:
            self.cwd_globs = [
                str(Path(p).expanduser()) for p in self.cwd_globs
            ]
        self.repo_file_exclude_globs = tuple(self.repo_file_exclude_globs or ())
        self.change_probe = self._build_change_probe()

    def _build_change_probe(self):
        import sys

        from okg.substrate.library.sources.content_hash_probe import (
            file_content_items,
        )
        from okg.substrate.library.sources.emission_identity import (
            emission_identity_targets,
        )
        from okg.substrate.library.sources.git_scope_probe import GitScopeProbe
        from okg.substrate.library.sources.union_probe import UnionProbe

        emit_targets = emission_identity_targets(
            self.name,
            sys.modules[__name__],
        )
        roots = [Path(self.codex_sessions_root)]
        if self.archived_sessions_root is not None:
            roots.append(Path(self.archived_sessions_root))

        def _transcript_items():
            out: list[tuple[str, Path]] = []
            for root in roots:
                for rel, path in file_content_items(root, include_globs=("*.jsonl",)):
                    out.append((f"{root}::{rel}", path))
            return out

        transcript_probe = _AppendTolerantTranscriptProbe(
            content_items=_transcript_items,
            config={
                "repo": self.repo,
                "since": self.since,
                "cwd_filter": [str(p) for p in (self.cwd_filter or [])],
                "cwd_globs": list(self.cwd_globs or []),
                "capture_tool_use_events": sorted(
                    self.capture_tool_use_events or []
                ),
                "max_transcript_bytes": self.max_transcript_bytes,
                "max_transcript_records": self.max_transcript_records,
            },
            emit_targets=emit_targets,
        )
        self._transcript_item_supplier = _transcript_items
        self._transcript_probe = transcript_probe
        if self.repo_root is None:
            self._repo_probe = None
            return transcript_probe
        repo_probe = GitScopeProbe(
            repo_root=Path(self.repo_root),
            config={
                "repo": self.repo,
                "repo_file_exclude_globs": sorted(self.repo_file_exclude_globs),
                "repo_file_include_untracked": self.repo_file_include_untracked,
            },
            emit_targets=emit_targets,
        )
        self._repo_probe = repo_probe
        return UnionProbe([repo_probe, transcript_probe])

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[dict[str, Any]] = None,
        sync_scope: Optional[dict[str, Any]] = None,
        changed_transcript_keys: Optional[Iterable[str]] = None,
        change_probe_verdict: Optional[ProbeVerdict] = None,
        authoritative_replay: bool = False,
    ) -> SourceRun:
        """Read Codex transcripts, optionally invalidating exact content changes.

        ``changed_transcript_keys`` is the integration seam for a complete
        content-identity probe. Keys may be absolute transcript paths or the
        ``<root>::<relative-path>`` keys emitted by this source's transcript
        probe. A listed transcript starts a new offset-zero replacement epoch;
        callers must list only identities that changed since the immediately
        prior source cursor. Passing the same key while merely draining backlog
        would intentionally restart that epoch.

        ``None`` means no complete identity was evaluated, so direct callers
        use a conservative stat fallback for same-size rewrites/truncations
        while retaining the legacy append path for growth. An evaluated empty
        iterable is stronger: transcript content is known unchanged (for
        example a repo-only union-probe change or mtime churn), so it does not
        start a replacement epoch.
        """
        prior = cursor or {}
        scoped = path_scope_requested(sync_scope)
        scoped_paths = resolve_scoped_paths(
            sync_scope,
            roots=self._transcript_roots(),
            suffixes=[".jsonl"],
        )
        prior_transcripts = dict(prior.get("transcripts") or {})
        scoped_record_keys = self._scoped_transcript_record_keys(sync_scope)
        completed_scope = mode in {"reconcile", "scope_complete"}
        full_replay = authoritative_replay and completed_scope and not scoped
        can_declare_record_set = (
            self._record_set_authoritative() or full_replay
        )
        bounded_completed_scope = completed_scope and not can_declare_record_set
        next_cursor: dict[str, Any] = {
            "transcripts": (
                dict(prior_transcripts)
                if scoped or bounded_completed_scope else {}
            ),
        }
        if scoped and mode in {"reconcile", "scope_complete"}:
            for key in scoped_record_keys:
                next_cursor["transcripts"].pop(key, None)
        facts: list[NodeFact | EdgeFact] = []
        current_records: dict[str, RecordEmission] = {}
        cutoff = None if full_replay else _since_cutoff(self.since)
        allowlist = set(self.capture_tool_use_events or ())
        paths = tuple(scoped_paths) if scoped else tuple(self._iter_transcripts())
        transcript_membership = (
            self._transcript_membership() if not scoped else None
        )
        repo_snapshot = None
        pinned_repo_paths: set[str] | None = None
        if not scoped:
            if self._repo_probe is not None:
                try:
                    repo_snapshot = self._repo_probe.capture_snapshot()
                    pinned_repo_paths = (
                        self._repo_probe.file_paths_for_snapshot(
                            repo_snapshot
                        )
                    )
                except Exception:  # noqa: BLE001 - direct non-git callers
                    repo_snapshot = None
        prior_manifest = _content_manifest_from_cursor(cursor)
        current_manifest = self._current_transcript_manifest(
            scoped=scoped,
            prior_manifest=prior_manifest,
            change_probe_verdict=change_probe_verdict,
        )
        verified_append_keys: set[str] = set()
        if changed_transcript_keys is None and not scoped:
            (
                changed_transcript_keys,
                verified_append_keys,
            ) = self._content_changed_transcript_keys(
                paths=paths,
                prior_transcripts=prior_transcripts,
                prior_manifest=prior_manifest,
                current_manifest=current_manifest,
            )
        elif changed_transcript_keys is None and scoped:
            # The runner deliberately does not evaluate or stamp a global
            # probe for a partial scope. Without a scoped complete identity,
            # every selected transcript must rebuild from offset zero.
            changed_transcript_keys = {
                *scoped_record_keys,
                *(str(path.resolve()) for path in paths),
            }
        has_record_set_authority = cursor_has_record_set_authority(cursor)
        durable_record_set = (
            self._record_set_from_cursor(cursor)
            if can_declare_record_set else None
        )
        prior_record_set = (
            durable_record_set
            if completed_scope and not scoped else None
        )
        can_carry_completed_scope = prior_record_set is not None
        can_probe_completed_scope = (
            completed_scope
            and not scoped
            and can_declare_record_set
            and has_record_set_authority
        )
        prior_record_keys = (
            set(prior_record_set.records) if prior_record_set is not None else set()
        )
        # Chunk accumulation (design §3) needs its OWN cursor store, not the
        # published record set: a partial run declines completeness and
        # therefore publishes no record set, so there would be nothing to
        # accumulate into during exactly the catch-up runs that need it. Only
        # meaningful where a record set can be declared -- in the bounded
        # `since` regime this would be cursor bloat for no benefit.
        prior_record_emissions: dict[str, RecordEmission] = {}
        if can_declare_record_set:
            # Two stores, and the union is what "footprint we already know"
            # means. The published record set covers transcripts that finished;
            # `record_footprint` covers the ones still in flight, which by
            # definition are absent from any published record set. In-flight
            # accumulation wins on overlap because it is the newer state.
            if durable_record_set is not None:
                prior_record_emissions.update(durable_record_set.records)
            prior_record_emissions.update(_record_footprints_from_cursor(cursor))
        next_record_emissions: dict[str, RecordEmission] = dict(
            prior_record_emissions
        )
        prior_replacement_epochs = _replacement_epochs_from_cursor(cursor)
        next_replacement_epochs = dict(prior_replacement_epochs)
        content_identity_evaluated = changed_transcript_keys is not None
        changed_transcripts = self._normalize_changed_transcript_keys(
            changed_transcript_keys,
        )
        verified_append_transcripts = self._normalize_changed_transcript_keys(
            verified_append_keys,
        )
        completed_replacement_records: dict[str, RecordEmission] = {}
        completed_replacement_keys: set[str] = set()
        finalized_replacement_footprints: dict[str, RecordEmission] = {}
        replacement_epochs_ready = 0
        walked_record_keys: set[str] = set()
        replacement_record_keys: set[str] = set()
        enumeration_failure_count = 0
        consumed_slice_hashes: dict[str, str] = {}
        # Chunked ingest (unblock-source-publish-admission): the size cap is a
        # PER-RUN budget, not a per-file exclusion. A transcript larger than
        # the budget is consumed across successive runs instead of being
        # dropped -- a long session is legitimate data.
        run_budget_bytes_remaining = int(self.max_transcript_bytes)
        # Bytes bound the read; a parsed-record ceiling bounds the publish
        # (design §6). PR #858's pressure was Postgres lock-table and
        # shared_buffers sizing -- an object-count problem -- and dense
        # message lines yield far more records per byte than a few giant tool
        # outputs, so a byte budget alone bounds the wrong quantity.
        run_budget_records_remaining = int(self.max_transcript_records)
        any_transcript_incomplete = False
        outstanding_backlog: list[dict[str, Any]] = []
        known_repo_paths: set[str] | None | object = _UNSET
        stats: dict[str, Any] = {
            "scope_status": (
                "scoped"
                if scoped else "bounded_full" if bounded_completed_scope else "full"
            ),
            "run_mode": mode,
            "scope_keys": len(scoped_record_keys) if scoped else 0,
            "files_seen": 0,
            "files_scanned": 0,
            "files_skipped_unchanged": 0,
            "append_delta_files": 0,
            "full_parse_files": 0,
            "bytes_delta": 0,
            "repo_file_inventory_required": False,
            "emitted_nodes": 0,
            "emitted_edges": 0,
            # Tool calls dropped from the cross-chunk pending store because it
            # hit its cap. Surfaced rather than silently discarded: a dropped
            # entry means that call's is_error/exit_code stamp is lost if its
            # output lands in a later chunk.
            "pending_tool_event_overflow": 0,
            "record_set_authoritative": can_declare_record_set,
            "bounded_completed_scope": bounded_completed_scope,
        }

        candidates, stat_failures = self._ordered_candidates(
            paths,
            prior_transcripts,
            snapshot_paths={},
        )
        enumeration_failure_count += stat_failures
        candidates_remaining = len(candidates)
        # Observed identity per transcript (#2469). Each entry names the
        # bytes this run's decision about that transcript rests on, without
        # copying the corpus: see `_observed_transcript_item_hashes`.
        observed_items: dict[Path, tuple[str, Any, Any] | None] = {}
        prior_manifest_by_path = (
            self._manifest_items_by_path(prior_manifest) if not scoped else {}
        )
        if not scoped:
            candidate_paths = {path for path, _stat in candidates}
            for path in paths:
                if path not in candidate_paths:
                    # Dropped at the size check as empty (stat failures make
                    # the enumeration incomplete, which never seals).
                    observed_items[path] = (
                        "observed",
                        hashlib.sha256(b"").hexdigest(),
                        0,
                    )

        for path, stat in candidates:
            read_path = path
            # Fair share of what is left: with candidates ordered by ascending
            # unconsumed bytes, transcripts that fit finish and hand their
            # unused share to the backlog files behind them, so one 817 MB
            # file cannot starve live sessions (design §7). _parse_transcript
            # still consumes at least one line, so a share of zero cannot
            # deadlock progress (design §8).
            per_file_budget = run_budget_bytes_remaining // max(
                1, candidates_remaining
            )
            candidates_remaining -= 1
            cursor_key = str(path)
            prior_info = prior_transcripts.get(cursor_key)
            if (
                not scoped
                and cutoff
                and cursor_key not in prior_transcripts
                and datetime.fromtimestamp(stat.st_mtime, tz=UTC) < cutoff
            ):
                # Decided from the stat alone: the probe's identity holds only
                # if the probe hashed the size this check observed.
                observed_items[path] = ("probe_size", stat.st_size, None)
                continue

            stats["files_seen"] += 1
            walked_record_keys.add(cursor_key)
            # COMPARISON uses the live stat; PERSISTENCE records the consumed
            # offset and happens after the parse (design §1). These were one
            # dict before, which is why a stop offset had nowhere to live.
            fingerprint = {
                "bytes": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
            }
            # Carry the prior entry forward. The post-parse write replaces it
            # with the true consumed prefix; a candidate that is enumerated
            # but never consumed -- cwd mismatch, parse failure, no meta --
            # must never have full consumption asserted on its behalf, which
            # is what writing `fingerprint` here used to do (design §5).
            if prior_info is not None:
                next_cursor["transcripts"][cursor_key] = prior_info
            unchanged = _same_transcript_fingerprint(prior_info, fingerprint)
            known_footprint = (
                prior_record_emissions.get(cursor_key)
                if can_declare_record_set
                else _owned_footprint_from_cursor_entry(prior_info)
            )
            replacement_epoch = prior_replacement_epochs.get(cursor_key)
            content_changed = (
                self._transcript_key_changed(path, changed_transcripts)
                if content_identity_evaluated
                else _direct_replacement_needed(prior_info, fingerprint)
            )
            verified_append = (
                content_identity_evaluated
                and self._transcript_key_changed(
                    path,
                    verified_append_transcripts,
                )
            )
            if (
                content_identity_evaluated
                and not content_changed
                and not verified_append
                and prior_info is not None
                and not _cursor_entry_incomplete(prior_info)
            ):
                # Complete content identity outranks size/mtime metadata.
                # Timestamp churn and repo-only UnionProbe changes must not
                # invoke transcript parsing when the transcript bytes are
                # proven unchanged. Incomplete entries still run so bounded
                # catch-up cannot be stranded.
                unchanged = True
            replacement_started = False
            if replacement_epoch is not None and content_changed:
                # Facts emitted by the abandoned attempt remain cleanup
                # authority. The original complete footprint stays unchanged;
                # only the candidate epoch restarts.
                replacement_epoch = replacement_epoch.restart()
                replacement_started = True
            elif replacement_epoch is None and (
                content_changed
                or prior_info is None
                or (not can_declare_record_set and known_footprint is None)
            ):
                # Existing bounded-since cursors predate per-transcript
                # ownership. Rebuilding them once establishes an honest
                # footprint instead of treating "unknown" as "owns nothing".
                replacement_epoch = _ReplacementEpoch(old=known_footprint)
                replacement_started = True
            if replacement_epoch is not None:
                next_replacement_epochs[cursor_key] = replacement_epoch
            if (
                (
                    mode in {"cursor", "scope_incomplete"}
                    or scoped
                    or (
                        can_carry_completed_scope
                        and cursor_key in prior_record_keys
                    )
                    or can_probe_completed_scope
                    or bounded_completed_scope
                    # A reconcile re-reads by design, but re-reading a file that
                    # has not changed since we consumed all of it is pure cost,
                    # and under a budget it is worse than pure cost: the
                    # re-parse gets cut short, so a finished transcript becomes
                    # unfinished again every cycle and catch-up never ends.
                    # Skipping is only safe where the record's footprint is
                    # already known, which is exactly this condition.
                    or (completed_scope and known_footprint is not None)
                )
                and unchanged
                and replacement_epoch is None
            ):
                stats["files_skipped_unchanged"] += 1
                # Skipped because the pre-run complete identity (or the
                # cursor fingerprint) showed nothing new; that identity is
                # what the carried footprint stands on.
                observed_items[path] = ("probe", None, None)
                if completed_scope and known_footprint is not None:
                    # Contribute the known footprint rather than nothing: on a
                    # bootstrap reconcile there is no prior record set to carry
                    # the key, so omitting it would declare a record set that
                    # does not mention this record -- a retraction.
                    current_records[cursor_key] = known_footprint
                    replacement_record_keys.add(cursor_key)
                continue

            # A record-set-authoritative reconcile normally forbids resume: the
            # full re-parse is what made the emitted footprint complete. Under
            # a budget that trade is not merely suboptimal, it does not
            # terminate -- re-reading every already-ingested transcript spends
            # the budget on known data, so files bounce between complete and
            # backlog forever and nothing converges.
            #
            # Accumulation (§3) supplies completeness instead, so resume is
            # allowed exactly when a prior footprint for this record is known:
            # then a run that reads nothing new still reports the record's full
            # footprint. With no known footprint the full re-parse is still
            # required, because the alternative is reporting a footprint
            # smaller than the graph holds, which retracts nodes.
            use_append_cursor = (
                not replacement_started
                and (
                    replacement_epoch is not None
                    or not completed_scope
                    or bounded_completed_scope
                    or known_footprint is not None
                )
            )
            initial_state = (
                _parse_state_from_cursor(prior_info)
                if use_append_cursor else None
            )
            append_state = _append_delta_state(
                initial_state,
                path=read_path,
                size=stat.st_size,
            )
            stats["files_scanned"] += 1
            if append_state is not None:
                stats["append_delta_files"] += 1
            else:
                stats["full_parse_files"] += 1
            prior_item = (
                prior_manifest_by_path.get(path)
                if not scoped and append_state is not None
                else None
            )
            parsed = _parse_transcript(
                read_path,
                allowlist=allowlist,
                initial_state=append_state,
                max_bytes=max(0, per_file_budget),
                max_records=max(0, run_budget_records_remaining),
                observe_identity=not scoped,
                verify_prefix_bytes=(
                    prior_item[1]
                    if prior_item is not None
                    and prior_item[1] >= append_state.bytes
                    else None
                ),
            )
            if not scoped:
                observed_items[path] = _observed_parse_identity(
                    parsed,
                    resumed_from=(
                        append_state.bytes if append_state is not None else 0
                    ),
                    prior_item=prior_item,
                )
            start_offset = append_state.bytes if append_state is not None else 0
            consumed_bytes = int(parsed.get("consumed_bytes") or stat.st_size)
            slice_sha256 = parsed.get("consumed_slice_sha256")
            if consumed_bytes > start_offset and isinstance(slice_sha256, str):
                consumed_slice_hashes[
                    f"{cursor_key}::{start_offset}:{consumed_bytes}"
                ] = slice_sha256
            transcript_complete = bool(parsed.get("complete", True))
            consumed_this_run = max(0, consumed_bytes - start_offset)
            # bytes_delta used to be charged pre-parse at full file size,
            # which under chunking would report work that never happened
            # (design implementation note P3-2).
            stats["bytes_delta"] += consumed_this_run
            run_budget_bytes_remaining = max(
                0, run_budget_bytes_remaining - consumed_this_run
            )
            parse_failed = int(parsed.get("parse_error_count") or 0) > 0
            if parse_failed:
                enumeration_failure_count += 1
            meta = parsed.get("meta") or {}
            if not isinstance(meta, dict) or not meta:
                if not parse_failed:
                    enumeration_failure_count += 1
                # Record the prefix we actually read. Leaving no entry made
                # this file look new on every subsequent run, so it was
                # re-read from zero forever.
                cursor_entry = _transcript_cursor_entry(
                    parsed,
                    path=read_path,
                    stat=stat,
                    consumed=consumed_bytes,
                )
                if not can_declare_record_set:
                    cursor_entry = _cursor_entry_with_owned_footprint(
                        cursor_entry,
                        replacement_epoch.old
                        if replacement_epoch is not None
                        else known_footprint,
                    )
                next_cursor["transcripts"][cursor_key] = cursor_entry
                if not transcript_complete:
                    any_transcript_incomplete = True
                    outstanding_backlog.append(_backlog_entry(
                        path, consumed_bytes, stat.st_size
                    ))
                continue
            cwd = str(meta.get("cwd") or "")
            if not self._cwd_matches(cwd):
                # OUT OF SCOPE, not unread: this source will never emit from
                # this transcript, so there is nothing left to ingest and the
                # entry records that -- which is what the pre-chunking code
                # did by writing the fingerprint before parsing.
                #
                # ~/.codex/sessions holds every project's sessions, so this is
                # the common case, not an edge case. Leaving no entry made the
                # adapter re-read every foreign transcript on every run: on
                # this host that was half the per-run budget spent on files
                # that emit nothing, and -- because an unfinished foreign read
                # suppressed completed scope while never appearing in the
                # backlog -- a reconcile that could never complete and an
                # operator-visible backlog of zero.
                cursor_entry = dict(fingerprint)
                empty_footprint = RecordEmission()
                if not can_declare_record_set:
                    cursor_entry = _cursor_entry_with_owned_footprint(
                        cursor_entry,
                        empty_footprint,
                    )
                next_cursor["transcripts"][cursor_key] = cursor_entry
                if replacement_epoch is not None:
                    replacement_epoch = replacement_epoch.with_current(
                        empty_footprint,
                    )
                    replacement_may_retract = (
                        _emission_has_identities(replacement_epoch.old)
                        or _emission_has_identities(
                            replacement_epoch.cleanup,
                        )
                    )
                    if (
                        replacement_may_retract
                        and mode not in {"reconcile", "scope_complete"}
                    ):
                        next_replacement_epochs[cursor_key] = (
                            replacement_epoch
                        )
                        if not can_declare_record_set:
                            next_cursor["transcripts"][cursor_key] = (
                                _cursor_entry_with_owned_footprint(
                                    cursor_entry,
                                    replacement_epoch.old,
                                )
                            )
                        replacement_epochs_ready += 1
                        any_transcript_incomplete = True
                        outstanding_backlog.append(_backlog_entry(
                            path,
                            stat.st_size,
                            stat.st_size,
                        ))
                    else:
                        finalized_replacement_footprints[cursor_key] = (
                            empty_footprint
                        )
                        completed_replacement_keys.add(cursor_key)
                        next_replacement_epochs.pop(cursor_key, None)
                        next_record_emissions.pop(cursor_key, None)
                continue
            if not transcript_complete:
                any_transcript_incomplete = True

            if parsed.get("tool_events"):
                if known_repo_paths is _UNSET:
                    known_repo_paths = (
                        pinned_repo_paths
                        if not scoped and repo_snapshot is not None
                        else self._known_repo_file_paths()
                    )
                    stats["repo_file_inventory_required"] = True
                repo_paths = known_repo_paths
            else:
                repo_paths = None
            transcript_facts = self._facts_for_transcript(
                path,
                meta,
                parsed,
                known_repo_paths=(
                    repo_paths if isinstance(repo_paths, set) else None
                ),
            )
            facts.extend(transcript_facts)
            # Charge the budget in the SAME unit the parse-side cap counts --
            # parsed records. Charging emitted facts (nodes plus edges) while
            # capping on parsed records overshot the ceiling by the emission
            # fan-out, which is content-dependent: a cap of 200 emitted 403.
            run_budget_records_remaining = max(
                0, run_budget_records_remaining - int(
                    parsed.get("records_parsed") or 0
                )
            )
            stats["pending_tool_event_overflow"] += int(
                parsed.get("pending_tool_event_overflow") or 0
            )
            stats["emitted_nodes"] += sum(
                1 for fact in transcript_facts if isinstance(fact, NodeFact)
            )
            stats["emitted_edges"] += sum(
                1 for fact in transcript_facts if isinstance(fact, EdgeFact)
            )
            cursor_entry = _transcript_cursor_entry(
                parsed,
                path=read_path,
                stat=stat,
                consumed=consumed_bytes,
            )
            next_cursor["transcripts"][cursor_key] = cursor_entry
            if not transcript_complete:
                outstanding_backlog.append(_backlog_entry(
                    path, consumed_bytes, stat.st_size
                ))
            emission = self._record_emission_for_transcript(transcript_facts)
            replacement_final_footprint: RecordEmission | None = None
            if replacement_epoch is not None:
                current_epoch_footprint = self._merge_record_emissions(
                    replacement_epoch.current,
                    emission,
                )
                replacement_epoch = replacement_epoch.with_current(
                    current_epoch_footprint,
                )
                replacement_may_retract = (
                    _emission_has_identities(replacement_epoch.old)
                    or _emission_has_identities(replacement_epoch.cleanup)
                )
                replacement_can_finalize = (
                    transcript_complete
                    and (
                        mode in {"reconcile", "scope_complete"}
                        or not replacement_may_retract
                    )
                )
                if replacement_can_finalize:
                    replacement_final_footprint = current_epoch_footprint
                    finalized_replacement_footprints[cursor_key] = (
                        current_epoch_footprint
                    )
                    completed_replacement_records[cursor_key] = (
                        current_epoch_footprint
                    )
                    completed_replacement_keys.add(cursor_key)
                    next_replacement_epochs.pop(cursor_key, None)
                    if not can_declare_record_set:
                        next_cursor["transcripts"][cursor_key] = (
                            _cursor_entry_with_owned_footprint(
                                cursor_entry,
                                current_epoch_footprint,
                            )
                        )
                else:
                    next_replacement_epochs[cursor_key] = replacement_epoch
                    if not can_declare_record_set:
                        next_cursor["transcripts"][cursor_key] = (
                            _cursor_entry_with_owned_footprint(
                                cursor_entry,
                                replacement_epoch.old,
                            )
                        )
                    if transcript_complete:
                        # Discovery-crawl policy forbids retractions from
                        # scope-incomplete/cursor runs. Keep both ownership
                        # stores intact and force another read until the
                        # coordinator supplies scope_complete/reconcile.
                        replacement_epochs_ready += 1
                        any_transcript_incomplete = True
                        outstanding_backlog.append(_backlog_entry(
                            path,
                            consumed_bytes,
                            stat.st_size,
                        ))
            elif not can_declare_record_set:
                # A verified append delta can only add ownership. In the
                # correctness-first path all complete-identity changes start a
                # replacement epoch; this branch is retained for unchanged
                # backlog continuation and legacy direct callers.
                owned = self._merge_record_emissions(
                    known_footprint,
                    emission,
                )
                next_cursor["transcripts"][cursor_key] = (
                    _cursor_entry_with_owned_footprint(cursor_entry, owned)
                )
            if can_declare_record_set:
                # Replacing a record's footprint asserts "these facts ARE the
                # file's complete footprint". That only holds when this run
                # parsed the whole file: from offset zero, to the end. Any
                # other run saw a fragment, so its footprint must be the union
                # with what earlier chunks emitted (design §3). Without this,
                # the run that FINISHES a chunked transcript retracts every
                # node from chunks 1..N-1.
                covers_whole_file = append_state is None and transcript_complete
                accumulated = (
                    replacement_final_footprint
                    if replacement_final_footprint is not None
                    else emission
                    if covers_whole_file
                    else self._merge_record_emissions(
                        prior_record_emissions.get(cursor_key), emission
                    )
                )
                next_record_emissions[cursor_key] = accumulated
                if completed_scope:
                    current_records[cursor_key] = accumulated
                    # Marked replaced only where a footprint was actually
                    # produced. Marking it at the top of the loop -- before the
                    # parse could fail, be abandoned on a cwd mismatch, or emit
                    # nothing -- retracted the record's prior nodes while
                    # substituting nothing for them.
                    replacement_record_keys.add(cursor_key)

        missing_prior_keys = set(prior_transcripts).difference(
            walked_record_keys,
        )
        missing_changed_keys = {
            key
            for key in missing_prior_keys
            if self._transcript_key_changed(
                Path(key),
                changed_transcripts,
            )
        }
        if mode not in {"reconcile", "scope_complete"}:
            for key in missing_changed_keys:
                prior_info = prior_transcripts.get(key)
                known_footprint = (
                    prior_record_emissions.get(key)
                    if can_declare_record_set
                    else _owned_footprint_from_cursor_entry(prior_info)
                )
                epoch = prior_replacement_epochs.get(key)
                if epoch is None:
                    epoch = _ReplacementEpoch(old=known_footprint)
                else:
                    epoch = epoch.restart()
                epoch = epoch.with_current(RecordEmission())
                next_replacement_epochs[key] = epoch
                if prior_info is not None:
                    next_cursor["transcripts"][key] = prior_info
                replacement_epochs_ready += 1
                any_transcript_incomplete = True
                outstanding_backlog.append({
                    "path": key,
                    "consumed_bytes": 0,
                    "size_bytes": 0,
                })
        deleted_record_keys = (
            {
                key
                for key in missing_prior_keys
                if (
                    (completed_scope and not scoped)
                    or (
                        scoped
                        and mode in {"reconcile", "scope_complete"}
                        and key in scoped_record_keys
                    )
                    or (
                        mode in {"reconcile", "scope_complete"}
                        and key in missing_changed_keys
                    )
                )
            }
            if enumeration_failure_count == 0 else set()
        )
        for key in deleted_record_keys:
            next_cursor["transcripts"].pop(key, None)
            next_replacement_epochs.pop(key, None)
            next_record_emissions.pop(key, None)
            completed_replacement_keys.add(key)
            finalized_replacement_footprints[key] = RecordEmission()

        if (
            completed_scope
            and not scoped
            and can_declare_record_set
            and has_record_set_authority
            and not can_carry_completed_scope
            and (
                facts
                or set(prior_transcripts).difference(walked_record_keys)
            )
            # The rebuild re-runs with NO cursor, so every candidate parses
            # from zero on a fresh budget -- doubling this run's cost and
            # restarting the very transcript we just made progress on. While a
            # backlog exists the rebuild is deferred; it runs once catch-up
            # completes, which is also the first moment its result could be
            # a genuinely complete record set (design implementation note P3-3).
            and not any_transcript_incomplete
        ):
            return self.run(
                run_id,
                mode=mode,
                authoritative_replay=authoritative_replay,
            )

        enumeration_complete = enumeration_failure_count == 0
        if not enumeration_complete:
            # Health and cursor advance are decoupled (design §4). Reverting
            # the cursor wholesale here discarded consumed chunks belonging to
            # OTHER candidates, so one malformed line anywhere in the backlog
            # meant permanent zero progress -- strictly worse than the old
            # behaviour, because waiting no longer fixed it. Every candidate
            # already carries its own prior entry forward, so nothing is lost
            # by letting the surviving candidates keep their progress.
            completed_scope = False
        if any_transcript_incomplete:
            # A run that stopped mid-transcript has not walked the whole
            # scope, so it must not declare one. Accumulation (§3) keeps the
            # footprint correct; this keeps the *scope* claim honest.
            completed_scope = False
        # Outstanding backlog is persisted so the change probe can treat it as
        # a change signal (design §0). Without it a dormant half-consumed
        # transcript makes zero progress on a quiet host: the probe digests
        # (key, size, head_sha, tail_sha) with no notion of consumed offset,
        # so the runner short-circuits and never calls run() at all, leaving
        # convergence to depend on unrelated Codex churn.
        next_cursor["outstanding_backlog"] = sorted(
            outstanding_backlog, key=lambda item: item["path"]
        )
        stats["backlog_transcripts"] = len(outstanding_backlog)
        stats["backlog_bytes"] = sum(
            max(0, int(item["size_bytes"]) - int(item["consumed_bytes"]))
            for item in outstanding_backlog
        )
        next_cursor[_REPLACEMENT_EPOCHS_KEY] = {
            key: epoch.to_cursor()
            for key, epoch in sorted(next_replacement_epochs.items())
        }
        sealed_items = (
            self._observed_transcript_item_hashes(
                membership=transcript_membership,
                observed_items=observed_items,
                paths=paths,
                current_manifest=current_manifest,
            )
            if not scoped
            and enumeration_complete
            and not outstanding_backlog
            and (self._repo_probe is None or repo_snapshot is not None)
            # Only the runner's pre-run probe names what the skip decisions
            # stood on. A direct call (no verdict) builds its manifest after
            # the caller's own decision, so it never seals.
            and change_probe_verdict is not None
            and bool(change_probe_verdict.token)
            else None
        )
        manifest_to_persist = (
            prior_manifest
            if scoped
            else self._transcript_probe.manifest_from_item_hashes(
                {key: item[0] for key, item in sealed_items.items()},
                item_sizes={
                    key: item[1] for key, item in sealed_items.items()
                },
            )
            if sealed_items is not None
            else current_manifest
        )
        if manifest_to_persist is not None:
            next_cursor[_TRANSCRIPT_CONTENT_MANIFEST_KEY] = manifest_to_persist
        stats["replacement_epochs_active"] = len(next_replacement_epochs)
        stats["replacement_epochs_ready_for_completed_scope"] = (
            replacement_epochs_ready
        )
        stats["replacement_epochs_completed"] = len(
            completed_replacement_keys,
        )

        if not can_declare_record_set:
            prior_authority = _bounded_authority_from_cursor(prior)
            current_authority = _bounded_authority_from_parts(
                next_cursor["transcripts"],
                next_replacement_epochs,
            )
            bounded_retractions = _record_emission_retractions(
                prior_authority,
                current_authority,
                repo=self.repo,
                reason="bounded_transcript_replacement",
            )
            facts.extend(bounded_retractions)
            stats["bounded_owned_node_retractions"] = sum(
                isinstance(fact, NodeFact) for fact in bounded_retractions
            )
            stats["bounded_owned_edge_retractions"] = sum(
                isinstance(fact, EdgeFact) for fact in bounded_retractions
            )
        else:
            # Replacement chunks are published while the runner's old
            # record-set slice remains unchanged. They therefore need their
            # own cleanup authority. An epoch restart moves its abandoned
            # current slice into `cleanup`; completion removes both stores and
            # explicitly retracts their old-only identities. The runner's
            # record-set diff independently retracts the original complete
            # footprint.
            prior_transient = _replacement_transient_authority(
                prior_replacement_epochs,
            )
            current_transient = _replacement_transient_authority(
                next_replacement_epochs,
            )
            for footprint in finalized_replacement_footprints.values():
                current_transient = self._merge_record_emissions(
                    current_transient,
                    footprint,
                )
            transient_retractions = _record_emission_retractions(
                prior_transient,
                current_transient,
                repo=self.repo,
                reason="replacement_epoch_cleanup",
            )
            facts.extend(transient_retractions)
            stats["replacement_cleanup_node_retractions"] = sum(
                isinstance(fact, NodeFact) for fact in transient_retractions
            )
            stats["replacement_cleanup_edge_retractions"] = sum(
                isinstance(fact, EdgeFact) for fact in transient_retractions
            )
        can_build_complete_record_set = (
            completed_scope
            and can_declare_record_set
            and (
                not has_record_set_authority
                or can_carry_completed_scope
                or bool(facts)
                or bool(set(prior_transcripts).difference(walked_record_keys))
            )
        )
        record_set = (
            RecordSet(records=current_records)
            if can_build_complete_record_set else None
        )
        if (
            completed_scope
            and can_carry_completed_scope
            and prior_record_set is not None
            and record_set is not None
        ):
            replacement_record_keys.update(
                prior_record_keys.difference(walked_record_keys)
            )
            record_set = self._record_set_with_prior(
                prior_record_set,
                current=record_set,
                replacement_keys=replacement_record_keys,
            )
        if completed_scope and scoped and can_declare_record_set:
            record_set = self._scoped_record_set(
                cursor,
                current=record_set or RecordSet(),
                replacement_keys=scoped_record_keys,
            )
            completed_scope = record_set is not None
        record_set_replacement_keys: set[str] | None = None
        if (
            can_declare_record_set
            and record_set is None
            and completed_replacement_keys
        ):
            record_set = RecordSet(
                records={
                    key: emission
                    for key, emission in completed_replacement_records.items()
                    if key in completed_replacement_keys
                },
            )
            record_set_replacement_keys = set(completed_replacement_keys)

        if can_declare_record_set:
            # The accumulation store exists to answer "what does this record
            # already own" for a transcript no published record set covers yet.
            # A key is therefore retired the moment the record set being
            # returned covers it -- and NOT merely because the transcript
            # finished. Retiring on completion instead deadlocks: a finished
            # transcript loses its footprint, the next reconcile has to
            # re-parse it from zero, the budget stops that re-parse partway, so
            # it is unfinished again and no run ever declares a record set.
            covered = set(record_set.records) if record_set is not None else set()
            # During catch-up no record set is published, so the rule above
            # retires nothing and the store would grow with the CORPUS rather
            # than the backlog -- thousands of per-transcript footprints on
            # this host, spilled to a blob table and rewritten every run. A
            # transcript that is finished AND already covered by the PRIOR
            # record set needs no entry either: its footprint is re-derivable
            # from there (that union is what `prior_record_emissions` reads).
            # Only a first-ever catch-up, with no published record set at all,
            # is corpus-sized, and nothing can shrink that one.
            covered |= {
                key
                for key in prior_record_keys
                if not _cursor_entry_incomplete(
                    next_cursor["transcripts"].get(key)
                )
            }
            next_cursor["record_footprint"] = {
                key: value.to_cursor()
                for key, value in next_record_emissions.items()
                if key not in covered
            }
            stats["record_footprint_carried"] = len(
                next_cursor["record_footprint"]
            )

        self.last_run_stats = stats
        self.last_run_stats["enumeration_failure_count"] = (
            enumeration_failure_count
        )
        consumed_token = None
        if enumeration_complete:
            transcript_token = self._transcript_probe.token_from_item_hashes(
                consumed_slice_hashes
            )
            consumed_token = (
                self.change_probe.token_from_subtokens([
                    self._repo_probe.token_for_snapshot(**repo_snapshot),
                    transcript_token,
                ])
                if self._repo_probe is not None
                and repo_snapshot is not None
                else transcript_token
                if self._repo_probe is None
                else None
            )
        sealed_token = None
        if sealed_items is not None:
            sealed_transcript_token = (
                self._transcript_probe.token_from_item_hashes(
                    {key: item[0] for key, item in sealed_items.items()}
                )
            )
            sealed_token = (
                self.change_probe.token_from_subtokens([
                    self._repo_probe.token_for_snapshot(**repo_snapshot),
                    sealed_transcript_token,
                ])
                if self._repo_probe is not None
                else sealed_transcript_token
            )
        applied_token = (
            sealed_token
            if sealed_token is not None
            else dict(change_probe_verdict.token)
            if not outstanding_backlog
            and change_probe_verdict is not None
            and change_probe_verdict.token is not None
            else None
        )
        bootstrap_identity = (
            BootstrapIdentityClaim(
                raw_token=consumed_token,
                scope="consumed_slices",
                cursor_semantics=self.bootstrap_cursor_semantics,
                snapshot_materialization=(
                    BOOTSTRAP_SNAPSHOT_MATERIALIZATION
                ),
                input_cursor=dict(prior),
            )
            # A scoped run makes no claim: bootstrap authority is minted only
            # by an unscoped run, and the runner refuses a claim from a
            # path-scoped one (issue #1318). Before, a single-transcript sync
            # with backlog outstanding was refused (issue #2321).
            if outstanding_backlog and consumed_token is not None and not scoped
            else None
        )

        bootstrap_health_required = (
            outstanding_backlog or cursor_has_outstanding_work(cursor)
        )
        completed_scope = completed_scope and not outstanding_backlog
        if outstanding_backlog:
            record_set = None
            record_set_replacement_keys = None

        return SourceRun(
            facts=tuple(facts),
            completed_scope=completed_scope,
            next_cursor=(
                preserve_cursor_record_set_authority(next_cursor, cursor)
                if completed_scope
                and can_declare_record_set
                and has_record_set_authority
                and record_set is None
                else next_cursor
            ),
            applied_raw_token=applied_token,
            applied_token_scope=(
                "global" if applied_token is not None else "unavailable"
            ),
            applied_token_authority=(
                "exact" if applied_token is not None else "unavailable"
            ),
            # Sealed only when every transcript's identity names the bytes
            # this run read or decided on (hashed from the parse handle, or
            # the pre-run probe identity a skip stood on), so the token stays
            # true for the emitted facts while a live session keeps writing.
            applied_token_binding=(
                APPLIED_TOKEN_BINDING_SEALED_SNAPSHOT
                if sealed_token is not None
                else APPLIED_TOKEN_BINDING_UNVERIFIED
            ),
            bootstrap_identity=bootstrap_identity,
            run_mode=mode,
            record_set=record_set,
            record_set_replacement_keys=record_set_replacement_keys,
            health=(
                SourceHealth(
                    status="ok",
                    mode="local",
                    reason=(
                        "Codex transcripts read in place; consumed slices "
                        "hashed from the bytes parsed"
                    ),
                )
                if enumeration_complete and bootstrap_health_required
                else None
                if enumeration_complete
                else SourceHealth(
                    status="acquire_failed",
                    mode="local",
                    reason=(
                        "Codex transcript enumeration was incomplete: "
                        f"{enumeration_failure_count} candidate(s) unreadable, "
                        "empty, oversized, or malformed"
                    ),
                )
            ),
        )

    def _transcript_roots(self) -> tuple[Path, ...]:
        roots = [Path(self.codex_sessions_root)]
        if self.archived_sessions_root is not None:
            roots.append(Path(self.archived_sessions_root))
        return tuple(roots)

    def _transcript_membership(self) -> dict[str, Path] | None:
        """Probe item keys for this run, from the probe's own supplier."""
        try:
            return {
                str(key): Path(item).resolve()
                for key, item in self._transcript_item_supplier()
            }
        except Exception:  # noqa: BLE001 - unknown membership never seals
            return None

    def _manifest_items_by_path(
        self,
        manifest: dict[str, Any] | None,
    ) -> dict[Path, tuple[str, int]]:
        if manifest is None:
            return {}
        sizes = manifest.get("sizes") or {}
        out: dict[Path, tuple[str, int]] = {}
        for key, item_hash in (manifest.get("items") or {}).items():
            size = sizes.get(key)
            path = self._path_for_transcript_item_key(key)
            if path is None or not isinstance(size, int):
                continue
            out.setdefault(path, (item_hash, size))
        return out

    @staticmethod
    def _observed_transcript_item_hashes(
        *,
        membership: dict[str, Path] | None,
        observed_items: dict[Path, tuple[str, Any, Any] | None],
        paths: tuple[Path, ...],
        current_manifest: dict[str, Any] | None,
    ) -> dict[str, tuple[str, int]] | None:
        """Item identities composed like the probe's, from what was read.

        Returns ``{probe_key: (sha256, size)}`` over the probe supplier's
        membership, or None when any transcript lacks an honest identity:
        then the run must not claim a sealed token (#2469).

        - A parsed transcript contributes the digest of ``[0, N)`` hashed
          from the parse's own handle, where N is every byte the parse looked
          at; a resumed parse counts only when its prefix matched the
          identity recorded for it before.
        - A transcript skipped as unchanged contributes the pre-run probe
          identity the skip stood on; one filtered by age at the size check
          does so only if the probe hashed the size that check observed.
        - A transcript emptied at the size check contributes the empty digest.
        """
        if membership is None or current_manifest is None:
            return None
        if set(current_manifest.get("items") or {}) != set(membership):
            # A transcript was deleted or added after the pre-run probe. Do
            # not seal: the runner then requires the post-run probe to match,
            # which is how it refused such a run before.
            return None
        if set(paths) != set(membership.values()):
            # A transcript appeared or vanished between the two enumerations.
            return None
        probe_items = (current_manifest or {}).get("items") or {}
        probe_sizes = (current_manifest or {}).get("sizes") or {}
        out: dict[str, tuple[str, int]] = {}
        for key, path in membership.items():
            observed = observed_items.get(path)
            if observed is None:
                return None
            kind = observed[0]
            if kind == "observed":
                out[key] = (str(observed[1]), int(observed[2]))
                continue
            item_hash = probe_items.get(key)
            item_size = probe_sizes.get(key)
            if not isinstance(item_hash, str) or not isinstance(item_size, int):
                return None
            if kind == "probe_size" and item_size != int(observed[1]):
                return None
            out[key] = (item_hash, item_size)
        return out

    def _current_transcript_manifest(
        self,
        *,
        scoped: bool,
        prior_manifest: dict[str, Any] | None,
        change_probe_verdict: ProbeVerdict | None,
    ) -> dict[str, Any] | None:
        if scoped:
            return prior_manifest
        if change_probe_verdict is not None:
            if not change_probe_verdict.token:
                # A composite sibling may have failed before this child ran.
                # Never reuse the child's cache from an earlier probe cycle.
                return None
            return self._transcript_probe.last_item_manifest()
        # Direct adapter invocations (mostly focused tests) still exercise the
        # production complete-identity contract.
        try:
            self._transcript_probe.build_token()
        except Exception:  # noqa: BLE001 — unknown identity invalidates all
            return None
        return self._transcript_probe.last_item_manifest()

    def _content_changed_transcript_keys(
        self,
        *,
        paths: tuple[Path, ...],
        prior_transcripts: dict[str, Any],
        prior_manifest: dict[str, Any] | None,
        current_manifest: dict[str, Any] | None,
    ) -> tuple[set[str], set[str]]:
        if (
            prior_manifest is None
            or current_manifest is None
            or prior_manifest.get("config_sha256")
            != current_manifest.get("config_sha256")
            or prior_manifest.get("emit_version_sha256")
            != current_manifest.get("emit_version_sha256")
        ):
            return (
                {
                    *prior_transcripts,
                    *(str(path.resolve()) for path in paths),
                },
                set(),
            )

        prior_items = prior_manifest["items"]
        current_items = current_manifest["items"]
        prior_sizes = prior_manifest.get("sizes") or {}
        changed = {
            key
            for key in set(prior_items) | set(current_items)
            if prior_items.get(key) != current_items.get(key)
        }
        verified_appends = {
            key
            for key in changed
            if self._content_change_is_verified_append(
                key,
                prior_item_hash=prior_items.get(key),
                prior_item_size=prior_sizes.get(key),
                prior_transcripts=prior_transcripts,
            )
        }
        return changed.difference(verified_appends), verified_appends

    def _content_change_is_verified_append(
        self,
        item_key: str,
        *,
        prior_item_hash: Any,
        prior_item_size: Any,
        prior_transcripts: dict[str, Any],
    ) -> bool:
        if not isinstance(prior_item_hash, str) or len(prior_item_hash) != 64:
            return False
        if not isinstance(prior_item_size, int) or prior_item_size <= 0:
            return False
        path = self._path_for_transcript_item_key(item_key)
        if path is None:
            return False
        prior = prior_transcripts.get(str(path.resolve()))
        if not isinstance(prior, dict):
            return False
        try:
            current_size = path.stat().st_size
        except OSError:
            return False
        if current_size <= prior_item_size:
            return False
        # The append-tolerant reader, so a session still being written is
        # recognised as an append rather than rebuilt from offset zero.
        try:
            prefix_sha, _size = _append_tolerant_prefix_sha(
                path,
                limit=prior_item_size,
            )
        except (ChangeProbeError, OSError):
            return False
        return prefix_sha == prior_item_hash

    def _path_for_transcript_item_key(self, item_key: str) -> Path | None:
        for root in self._transcript_roots():
            prefix = f"{root}::"
            if item_key.startswith(prefix):
                return (root / item_key[len(prefix):]).resolve()
        return None

    def _normalize_changed_transcript_keys(
        self,
        keys: Optional[Iterable[str]],
    ) -> set[str]:
        if keys is None:
            return set()
        normalized: set[str] = set()
        roots = self._transcript_roots()
        for raw_key in keys:
            key = str(raw_key)
            normalized.add(key)
            for root in roots:
                prefix = f"{root}::"
                if key.startswith(prefix):
                    normalized.add(str((root / key[len(prefix):]).resolve()))
        return normalized

    def _transcript_key_changed(
        self,
        path: Path,
        changed_keys: set[str],
    ) -> bool:
        if not changed_keys:
            return False
        aliases = {str(path), str(path.resolve())}
        for root in self._transcript_roots():
            try:
                rel = path.resolve().relative_to(root.resolve())
            except ValueError:
                continue
            aliases.add(f"{root}::{rel.as_posix()}")
        return not aliases.isdisjoint(changed_keys)

    def _iter_transcripts(self) -> Iterable[Path]:
        seen: set[Path] = set()
        for root in self._transcript_roots():
            if not root.exists():
                continue
            for path in sorted(root.rglob("*.jsonl")):
                resolved = path.resolve()
                if resolved not in seen:
                    seen.add(resolved)
                    yield resolved

    def _scoped_transcript_record_keys(
        self,
        sync_scope: Optional[dict[str, Any]],
    ) -> set[str]:
        return {
            str(path)
            for path in resolve_scoped_paths(
                sync_scope,
                roots=self._transcript_roots(),
                suffixes=[".jsonl"],
                existing_only=False,
            )
        }

    @staticmethod
    def _ordered_candidates(
        paths: tuple[Path, ...],
        prior_transcripts: dict[str, Any],
        *,
        snapshot_paths: Optional[dict[Path, Path]] = None,
    ) -> tuple[list[tuple[Path, os.stat_result]], int]:
        """Order candidates by ascending unconsumed bytes.

        Returns the survivors plus the count of candidates that could not be
        stat'd. A zero-byte transcript is *not* counted as a failure: a
        just-created `.jsonl` is a normal transient state on a live host, and
        counting it as unreadable degraded source health to `acquire_failed`,
        which admission rejects, blocking every publish cycle (design §4).

        Ascending order is what makes the per-file budget share fair: a
        transcript that fits inside its share finishes and leaves the rest to
        the backlog files behind it.
        """
        stat_failures = 0
        sized: list[tuple[int, Path, os.stat_result]] = []
        for path in paths:
            stat_path = (snapshot_paths or {}).get(path.resolve(), path)
            try:
                stat = stat_path.stat()
            except OSError:
                stat_failures += 1
                continue
            if stat.st_size <= 0:
                continue
            prior_info = prior_transcripts.get(str(path))
            consumed = 0
            if isinstance(prior_info, dict):
                try:
                    consumed = max(0, int(prior_info.get("bytes") or 0))
                except (TypeError, ValueError):
                    consumed = 0
            sized.append((max(0, int(stat.st_size) - consumed), path, stat))
        sized.sort(key=lambda item: (item[0], str(item[1])))
        return [(path, stat) for _, path, stat in sized], stat_failures

    @staticmethod
    def _merge_record_emissions(
        prior: Optional[RecordEmission],
        current: RecordEmission,
    ) -> RecordEmission:
        """Union two chunks' footprints for one logical record.

        A resumed parse sees only the newly consumed suffix, so its emission
        is a strict subset of the file's records. Letting it *replace* the
        footprint would retract every node emitted by earlier chunks under
        `missing_from_completed_scope` -- silent data loss on precisely the
        run that finishes a chunked transcript (design §3).
        """
        if prior is None:
            return current
        primary = current.primary or prior.primary
        nodes = dict(prior.nodes_by_id())
        nodes.update(current.nodes_by_id())
        edges = dict(prior.edges_by_identity())
        edges.update(current.edges_by_identity())
        return RecordEmission(
            primary=primary,
            descendants=[
                node
                for node_id, node in nodes.items()
                if primary is None or node_id != primary.node_id
            ],
            edges=list(edges.values()),
        )

    @staticmethod
    def _record_emission_for_transcript(
        facts: list[NodeFact | EdgeFact],
    ) -> RecordEmission:
        primary: RecordNode | None = None
        descendants: list[RecordNode] = []
        edges: list[EdgeKey] = []
        for fact in facts:
            if isinstance(fact, NodeFact):
                node = RecordNode(
                    node_id=fact.node_id,
                    subtype=fact.subtype,
                    source_record_id=dict(fact.source_record_id),
                    source_revision=dict(fact.source_revision),
                )
                if fact.subtype == "agent_session" and primary is None:
                    primary = node
                else:
                    descendants.append(node)
            else:
                edges.append(EdgeKey(
                    src=fact.src,
                    dst=fact.dst,
                    edge_type=fact.edge_type,
                    attrs=dict(fact.attrs),
                    provenance=fact.provenance,
                    confidence=fact.confidence,
                    source_record_id=dict(fact.source_record_id),
                    source_revision=dict(fact.source_revision),
                ))
        return RecordEmission(
            primary=primary,
            descendants=descendants,
            edges=edges,
        )

    @staticmethod
    def _record_set_from_cursor(
        cursor: Optional[dict[str, Any]],
    ) -> Optional[RecordSet]:
        if not isinstance(cursor, dict):
            return None
        prior_raw = cursor.get("record_set")
        if prior_raw is None:
            return None
        return RecordSet.from_cursor(prior_raw)

    @staticmethod
    def _record_set_with_prior(
        prior: RecordSet,
        *,
        current: RecordSet,
        replacement_keys: set[str],
    ) -> RecordSet:
        records = {
            key: value
            for key, value in prior.records.items()
            if key not in replacement_keys
        }
        records.update(current.records)
        return RecordSet(records=records)

    @staticmethod
    def _scoped_record_set(
        cursor: Optional[dict[str, Any]],
        *,
        current: RecordSet,
        replacement_keys: set[str],
    ) -> Optional[RecordSet]:
        if not isinstance(cursor, dict):
            return None
        prior_raw = cursor.get("record_set")
        if prior_raw is None:
            return None
        prior = RecordSet.from_cursor(prior_raw)
        records = {
            key: value
            for key, value in prior.records.items()
            if key not in replacement_keys
        }
        records.update(current.records)
        return RecordSet(records=records)

    def _cwd_matches(self, cwd: str) -> bool:
        if is_repo_local_claude_worktree_cwd(cwd):
            return False
        if not self.cwd_filter and not self.cwd_globs:
            return True
        try:
            cwd_path = Path(cwd).expanduser().resolve()
        except OSError:
            return False
        if any(
            cwd_path == p or p in cwd_path.parents
            for p in self.cwd_filter or []
        ):
            return True
        cwd_text = str(cwd_path)
        for pattern in self.cwd_globs or []:
            ps = str(pattern).rstrip("/")
            if (
                fnmatch.fnmatch(cwd_text, ps)
                or fnmatch.fnmatch(cwd_text, ps + "/*")
            ):
                return True
        return False

    def _facts_for_transcript(
        self,
        path: Path,
        meta: dict[str, Any],
        parsed: dict[str, Any],
        *,
        known_repo_paths: set[str] | None = None,
    ) -> list[NodeFact | EdgeFact]:
        sid = str(meta.get("id") or path.stem)
        session_node_id = f"session:codex:{sid}"
        model = str(meta.get("model") or meta.get("model_provider") or "unknown")
        agent_id = f"agent:{model}:codex"
        cwd = str(meta.get("cwd") or "")
        git = meta.get("git") if isinstance(meta.get("git"), dict) else {}
        source = meta.get("source")
        source_kind = _source_kind(source)
        revision = {
            "transcript_bytes": int(parsed.get("consumed_bytes") or 0),
            "last_line_sha256": str(parsed.get("last_line_hash") or ""),
        }

        session_attrs: dict[str, Any] = {
            "session_id": f"codex:{sid}",
            "transcript_path": str(path),
            "cwd": cwd,
            "cc_version": str(meta.get("cli_version") or ""),
            "os": _os_from_cwd(cwd),
            "git_branch": str(git.get("branch") or ""),
            "started_at": str(meta.get("timestamp") or parsed.get("started_at") or ""),
            "ended_at": str(parsed.get("ended_at") or meta.get("timestamp") or ""),
            "model": model,
            "turn_count": int(parsed.get("turn_count") or 0),
            "tool_call_count": int(parsed.get("tool_event_count") or 0),
            "auto_allow_count": 0,
            "denial_count": int(parsed.get("denial_count") or 0),
            "hook_block_count": 0,
            "tool_error_count": int(parsed.get("tool_error_count") or 0),
            "interrupt_count": int(parsed.get("interrupt_count") or 0),
            "permission_mode": "",
            "originator": str(meta.get("originator") or ""),
            "harness": "codex",
            "source_kind": source_kind,
            "agent_role": str(meta.get("agent_role") or ""),
            "agent_nickname": str(meta.get("agent_nickname") or ""),
            "git_commit": str(git.get("commit_hash") or ""),
            "repository_url": str(git.get("repository_url") or ""),
            "text": f"codex {model} {cwd} {source_kind}".strip(),
        }
        if not bool(parsed.get("complete", True)):
            # Cumulative attrs above (turn_count, ended_at, ...) converge only
            # once the transcript is fully consumed, so mid-ingest the session
            # would otherwise read as a genuinely SHORT session rather than a
            # partially read one (design §9). Booleans and ints survive the
            # attrs filter that drops ""; the marker is omitted on the
            # completing run, which clears it because re-emission replaces
            # raw_attrs wholesale.
            session_attrs["ingest_in_progress"] = True
            session_attrs["ingest_consumed_bytes"] = int(
                parsed.get("consumed_bytes") or 0
            )
        usage_attrs = parsed.get("usage") if isinstance(parsed, dict) else {}
        if isinstance(usage_attrs, dict) and usage_attrs:
            for key in (
                "input_tokens",
                "output_tokens",
                "cache_read_tokens",
                "reasoning_output_tokens",
                "total_tokens",
                "model_context_window",
                "cost_usd_estimate",
                "cost_confidence",
                "cost_basis",
                "cost_measurement_source",
                "usage_recorded_at",
            ):
                if key in usage_attrs:
                    session_attrs[key] = usage_attrs[key]
        else:
            session_attrs["cost_confidence"] = "modeled"
            session_attrs["cost_basis"] = "missing_codex_token_count"
        session_attrs = {k: v for k, v in session_attrs.items() if v not in {"", None}}

        facts: list[NodeFact | EdgeFact] = [
            NodeFact(
                node_id=agent_id,
                subtype="agent",
                attrs={
                    "agent_id": agent_id,
                    "model_name": model,
                    "harness": "codex",
                },
                source_record_id={"repo": self.repo, "node_id": agent_id},
                source_revision=revision,
            ),
            NodeFact(
                node_id=session_node_id,
                subtype="agent_session",
                attrs=session_attrs,
                source_record_id={"repo": self.repo, "node_id": session_node_id},
                source_revision=revision,
            ),
            EdgeFact(
                src=agent_id,
                dst=session_node_id,
                edge_type="assigned_to",
                source_record_id={"repo": self.repo, "node_id": session_node_id, "edge": "agent"},
                source_revision=revision,
            ),
        ]

        session_file_edges: set[str] = set()
        for message in parsed.get("messages") or []:
            message_id = f"message:codex:{sid}:{message.message_index}"
            message_attrs = {
                "message_id": message_id,
                "session_id": f"codex:{sid}",
                "harness": "codex",
                "role": message.role,
                "message_index": message.message_index,
                "line_index": message.line_index,
                "timestamp": message.timestamp,
                "message_kind": message.message_kind,
                "content_hash": message.content_hash,
                "content_excerpt": message.content_excerpt,
                "tool_call_count": message.tool_call_count,
                "redaction_policy": "bounded_excerpt_sha256",
            }
            if message.parent_message_id:
                message_attrs["parent_message_id"] = message.parent_message_id
            facts.append(NodeFact(
                node_id=message_id,
                subtype="agent_message",
                attrs=message_attrs,
                source_record_id={"repo": self.repo, "node_id": message_id},
                source_revision=revision,
            ))
            facts.append(EdgeFact(
                src=session_node_id,
                dst=message_id,
                edge_type="contains",
                source_record_id={
                    "repo": self.repo,
                    "node_id": message_id,
                    "edge": "session",
                },
                source_revision=revision,
            ))

        for event in parsed.get("tool_events") or []:
            event_file_paths = tuple(
                rel_path
                for rel_path in event.file_paths
                if known_repo_paths is None or rel_path in known_repo_paths
            )
            attrs = {
                "event_id": event.event_id,
                "session_id": f"codex:{sid}",
                "tool_name": event.tool_name,
                "turn_index": event.turn_index,
                "block_index": event.block_index,
                "timestamp": event.timestamp,
                "file_path": event_file_paths[0] if event_file_paths else "",
                "command": _truncate(event.command, 500) if event.command else "",
                "input_excerpt": _truncate(event.input_excerpt, 500),
            }
            if event.mcp_server:
                attrs["mcp_server"] = event.mcp_server
            if event.mcp_deployment:
                attrs["mcp_deployment"] = event.mcp_deployment
            if event.mcp_operator:
                attrs["mcp_operator"] = event.mcp_operator
            # Native error signal (reduce-agent-friction
            # req.friction.event-observability); absent = no output
            # observed in the scanned chunk.
            if event.is_error is not None:
                attrs["is_error"] = event.is_error
            if event.exit_code is not None:
                attrs["exit_code"] = event.exit_code
            facts.append(NodeFact(
                node_id=event.event_id,
                subtype="tool_use_event",
                attrs=attrs,
                source_record_id={"repo": self.repo, "node_id": event.event_id},
                source_revision=revision,
            ))
            facts.append(EdgeFact(
                src=session_node_id,
                dst=event.event_id,
                edge_type="contains",
                source_record_id={"repo": self.repo, "node_id": event.event_id, "edge": "session"},
                source_revision=revision,
            ))
            if event.message_index is not None:
                facts.append(EdgeFact(
                    src=f"message:codex:{sid}:{event.message_index}",
                    dst=event.event_id,
                    edge_type="contains",
                    source_record_id={
                        "repo": self.repo,
                        "node_id": event.event_id,
                        "edge": "message",
                    },
                    source_revision=revision,
                ))
            for rel_path in event_file_paths:
                file_id = f"file:{self.repo}:{rel_path}"
                facts.append(EdgeFact(
                    src=event.event_id,
                    dst=file_id,
                    edge_type="affects",
                    attrs={"file_path": rel_path},
                    source_record_id={
                        "repo": self.repo,
                        "event_id": event.event_id,
                        "path": rel_path,
                    },
                    source_revision=revision,
                ))
                if rel_path not in session_file_edges:
                    session_file_edges.add(rel_path)
                    facts.append(EdgeFact(
                        src=session_node_id,
                        dst=file_id,
                        edge_type="affects",
                        attrs={"file_path": rel_path},
                        source_record_id={
                            "repo": self.repo,
                            "session_id": session_node_id,
                            "path": rel_path,
                        },
                        source_revision=revision,
                    ))

        permission_event_start_index = int(
            parsed.get("permission_event_start_index") or 0,
        )
        for idx, approval in enumerate(
            parsed.get("permission_events") or [],
            start=permission_event_start_index,
        ):
            event_id = f"pevent:codex:{sid}:{idx}"
            facts.append(NodeFact(
                node_id=event_id,
                subtype="permission_event",
                attrs={
                    "event_id": event_id,
                    "session_id": f"codex:{sid}",
                    "kind": approval.get("kind") or "approval_request",
                    "timestamp": approval.get("timestamp") or session_attrs.get("started_at"),
                    "tool_name": approval.get("tool_name") or "",
                    "excerpt": approval.get("excerpt") or "",
                    "tool_input_summary": approval.get("tool_input_summary") or "",
                },
                source_record_id={"repo": self.repo, "node_id": event_id},
                source_revision=revision,
            ))
            facts.append(EdgeFact(
                src=session_node_id,
                dst=event_id,
                edge_type="contains",
                source_record_id={"repo": self.repo, "node_id": event_id, "edge": "session"},
                source_revision=revision,
            ))

        return facts

    def _known_repo_file_paths(self) -> set[str] | None:
        if self.repo_root is None:
            return None
        root = Path(self.repo_root)
        if not root.is_dir():
            return set()
        git_paths = self._known_repo_file_paths_from_git(root)
        if git_paths is not None:
            if not self.repo_file_include_untracked:
                return git_paths
            return git_paths | self._known_repo_file_paths_from_walk(root)
        return self._known_repo_file_paths_from_walk(root)

    def _known_repo_file_paths_from_walk(self, root: Path) -> set[str]:
        out: set[str] = set()
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [
                name for name in dirnames
                if name not in _REPO_FILE_SCAN_SKIP_DIRS
                and not self._repo_file_excluded(
                    ((Path(dirpath) / name).relative_to(root)).as_posix(),
                )
            ]
            base = Path(dirpath)
            for filename in filenames:
                path = base / filename
                try:
                    rel = path.relative_to(root).as_posix()
                except ValueError:
                    continue
                if not self._repo_file_excluded(rel):
                    out.add(rel)
        return out

    def _known_repo_file_paths_from_git(self, root: Path) -> set[str] | None:
        from okg.substrate.library.sources._git_helpers import (
            GitInvocationError,
            run_git,
        )

        try:
            output = run_git(root, ["ls-files", "--cached"])
        except (FileNotFoundError, GitInvocationError, OSError):
            return None
        return {
            line
            for line in output.splitlines()
            if line and not self._repo_file_excluded(line)
        }

    def _repo_file_excluded(self, rel_path: str) -> bool:
        return any(
            fnmatch.fnmatch(rel_path, pattern)
            for pattern in self.repo_file_exclude_globs
        )

    @staticmethod
    def _since_is_unbounded(since: str | None) -> bool:
        if since is None:
            return True
        return str(since).strip().lower() in {"", "all"}

    def _record_set_authoritative(self) -> bool:
        return self._since_is_unbounded(self.since)


def _same_transcript_fingerprint(
    prior: Any,
    fingerprint: dict[str, int],
) -> bool:
    if not isinstance(prior, dict):
        return False
    try:
        return (
            int(prior.get("bytes")) == int(fingerprint["bytes"])
            and int(prior.get("mtime_ns")) == int(fingerprint["mtime_ns"])
        )
    except (TypeError, ValueError):
        return False


def _direct_replacement_needed(
    prior: Any,
    fingerprint: dict[str, int],
) -> bool:
    """Conservative fallback when no complete item manifest was evaluated."""
    if not isinstance(prior, dict):
        return False
    try:
        prior_bytes = int(prior.get("bytes"))
        current_bytes = int(fingerprint["bytes"])
    except (TypeError, ValueError):
        return True
    return (
        not _same_transcript_fingerprint(prior, fingerprint)
        and current_bytes <= prior_bytes
    )


def _parse_state_from_cursor(raw: Any) -> _TranscriptParseState | None:
    if not isinstance(raw, dict):
        return None
    state = raw.get("append_cursor")
    if not isinstance(state, dict):
        return None
    if state.get("schema") != _APPEND_CURSOR_SCHEMA:
        return None
    last_message = _MessageEvent.from_cursor(state.get("last_message"))
    try:
        return _TranscriptParseState(
            bytes=int(state["bytes"]),
            mtime_ns=int(state.get("mtime_ns") or 0),
            line_count=int(state["line_count"]),
            last_line_hash=str(state.get("last_line_hash") or ""),
            meta=dict(state.get("meta") or {}),
            started_at=str(state.get("started_at") or ""),
            ended_at=str(state.get("ended_at") or ""),
            message_count=int(state.get("message_count") or 0),
            turn_count=int(state.get("turn_count") or 0),
            tool_event_count=int(state.get("tool_event_count") or 0),
            permission_event_count=int(
                state.get("permission_event_count") or 0,
            ),
            denial_count=int(state.get("denial_count") or 0),
            interrupt_count=int(state.get("interrupt_count") or 0),
            block_index=int(state.get("block_index") or 0),
            last_message=last_message,
            append_safe=bool(state.get("append_safe")),
            usage=dict(state.get("usage") or {}),
            mcp_tool_registry=_mcp_tool_registry_from_cursor(
                state.get("mcp_tool_registry"),
            ),
            tool_error_count=int(state.get("tool_error_count") or 0),
            ambiguous_mcp_operators=tuple(
                str(x) for x in (state.get("ambiguous_mcp_operators") or ())
            ),
            pending_tool_events={
                str(call_id): dict(payload)
                for call_id, payload in (
                    state.get("pending_tool_events") or {}
                ).items()
                if isinstance(payload, dict)
            },
            # Absent on cursors written before chunked ingest: those were
            # always full consumptions, so default True.
            complete=bool(state.get("complete", True)),
        )
    except (KeyError, TypeError, ValueError):
        return None


def _parse_state_to_cursor(
    parsed: dict[str, Any],
    *,
    bytes_size: int,
    mtime_ns: int,
    append_safe: bool,
) -> dict[str, Any]:
    last_message = parsed.get("last_message")
    last_message_payload = (
        last_message.to_cursor()
        if isinstance(last_message, _MessageEvent) else None
    )
    return {
        "append_cursor": {
            "schema": _APPEND_CURSOR_SCHEMA,
            "bytes": bytes_size,
            "mtime_ns": mtime_ns,
            "line_count": int(parsed.get("line_count") or 0),
            "last_line_hash": str(parsed.get("last_line_hash") or ""),
            "meta": dict(parsed.get("meta") or {}),
            "started_at": str(parsed.get("started_at") or ""),
            "ended_at": str(parsed.get("ended_at") or ""),
            "message_count": int(parsed.get("message_count") or 0),
            "turn_count": int(parsed.get("turn_count") or 0),
            "tool_event_count": int(parsed.get("tool_event_count") or 0),
            "permission_event_count": int(
                parsed.get("permission_event_count") or 0,
            ),
            "denial_count": int(parsed.get("denial_count") or 0),
            "interrupt_count": int(parsed.get("interrupt_count") or 0),
            "block_index": int(parsed.get("block_index") or 0),
            "last_message": last_message_payload,
            "append_safe": bool(append_safe),
            "usage": dict(parsed.get("usage") or {}),
            "tool_error_count": int(parsed.get("tool_error_count") or 0),
            "mcp_tool_registry": _mcp_tool_registry_to_cursor(
                parsed.get("mcp_tool_registry"),
            ),
            "ambiguous_mcp_operators": list(
                parsed.get("ambiguous_mcp_operators") or ()
            ),
            "pending_tool_events": dict(
                parsed.get("pending_tool_events") or {}
            ),
            "complete": bool(parsed.get("complete", True)),
        }
    }


def _transcript_cursor_entry(
    parsed: dict[str, Any],
    *,
    path: Path,
    stat: os.stat_result,
    consumed: int,
) -> dict[str, Any]:
    """The cursor entry describing the prefix this run actually consumed.

    Change comparison uses the live stat; persistence records the CONSUMED
    OFFSET (design §1). Recording `st_size` for a partially consumed file
    would make it compare `unchanged` forever and silently lose the
    remainder. `bytes`, `last_line_hash` and `append_safe` all describe that
    same prefix (design §2) -- evaluating `append_safe` at end-of-file made a
    live-growing transcript re-parse from zero every run.
    """
    return {
        "bytes": int(consumed),
        "mtime_ns": stat.st_mtime_ns,
        **_parse_state_to_cursor(
            parsed,
            bytes_size=int(consumed),
            mtime_ns=stat.st_mtime_ns,
            append_safe=_path_ends_with_newline(path, int(consumed)),
        ),
    }


def _backlog_entry(path: Path, consumed: int, size: int) -> dict[str, Any]:
    return {
        "path": str(path),
        "consumed_bytes": int(consumed),
        "size_bytes": int(size),
    }


def _record_footprints_from_cursor(
    cursor: Optional[dict[str, Any]],
) -> dict[str, RecordEmission]:
    """Read the per-record footprint accumulated by earlier chunks."""
    if not isinstance(cursor, dict):
        return {}
    raw = cursor.get("record_footprint")
    if not isinstance(raw, dict):
        return {}
    out: dict[str, RecordEmission] = {}
    for key, payload in raw.items():
        try:
            out[str(key)] = RecordEmission.from_cursor(payload)
        except (ValueError, TypeError, KeyError):
            # An unreadable entry means "we do not know this record's
            # footprint", which must NOT silently become "this record owns
            # nothing" -- that would retract its nodes. Dropping the key
            # leaves the prior published record set in charge of it.
            continue
    return out


def _content_manifest_from_cursor(
    cursor: Optional[dict[str, Any]],
) -> dict[str, Any] | None:
    if not isinstance(cursor, dict):
        return None
    raw = cursor.get(_TRANSCRIPT_CONTENT_MANIFEST_KEY)
    if not isinstance(raw, dict):
        return None
    if raw.get("schema") != "okg.source.content_hash.item_manifest.v1":
        return None
    config_hash = raw.get("config_sha256")
    emit_hash = raw.get("emit_version_sha256")
    items = raw.get("items")
    if not (
        isinstance(config_hash, str)
        and len(config_hash) == 64
        and isinstance(emit_hash, str)
        and len(emit_hash) == 64
        and isinstance(items, dict)
    ):
        return None
    normalized_items: dict[str, str] = {}
    for key, value in items.items():
        if not (
            isinstance(key, str)
            and key
            and isinstance(value, str)
            and len(value) == 64
        ):
            return None
        normalized_items[key] = value
    raw_sizes = raw.get("sizes")
    normalized_sizes: dict[str, int] = {}
    if raw_sizes is not None:
        if not isinstance(raw_sizes, dict):
            return None
        for key, value in raw_sizes.items():
            if (
                not isinstance(key, str)
                or key not in normalized_items
                or not isinstance(value, int)
                or value < 0
            ):
                return None
            normalized_sizes[key] = value
    return {
        "schema": "okg.source.content_hash.item_manifest.v1",
        "config_sha256": config_hash,
        "emit_version_sha256": emit_hash,
        "items": normalized_items,
        "sizes": normalized_sizes,
    }


def _optional_record_emission_from_cursor(raw: Any) -> RecordEmission | None:
    if raw is None:
        return None
    return RecordEmission.from_cursor(raw)


def _merge_optional_emissions(
    prior: RecordEmission | None,
    current: RecordEmission | None,
) -> RecordEmission | None:
    if prior is None:
        return current
    if current is None:
        return prior
    primary = current.primary or prior.primary
    nodes = dict(prior.nodes_by_id())
    nodes.update(current.nodes_by_id())
    edges = dict(prior.edges_by_identity())
    edges.update(current.edges_by_identity())
    return RecordEmission(
        primary=primary,
        descendants=[
            node
            for node_id, node in nodes.items()
            if primary is None or node_id != primary.node_id
        ],
        edges=list(edges.values()),
    )


def _emission_has_identities(emission: RecordEmission | None) -> bool:
    return bool(
        emission is not None
        and (emission.nodes_by_id() or emission.edges_by_identity())
    )


def _owned_footprint_from_cursor_entry(raw: Any) -> RecordEmission | None:
    if not isinstance(raw, dict):
        return None
    payload = raw.get(_OWNED_FOOTPRINT_KEY)
    if payload is None:
        return None
    try:
        return RecordEmission.from_cursor(payload)
    except (KeyError, TypeError, ValueError):
        return None


def _cursor_entry_with_owned_footprint(
    entry: dict[str, Any],
    footprint: RecordEmission | None,
) -> dict[str, Any]:
    out = dict(entry)
    if footprint is None:
        out.pop(_OWNED_FOOTPRINT_KEY, None)
    else:
        out[_OWNED_FOOTPRINT_KEY] = footprint.to_cursor()
    return out


def _replacement_epochs_from_cursor(
    cursor: Optional[dict[str, Any]],
) -> dict[str, _ReplacementEpoch]:
    if not isinstance(cursor, dict):
        return {}
    raw = cursor.get(_REPLACEMENT_EPOCHS_KEY)
    if not isinstance(raw, dict):
        return {}
    out: dict[str, _ReplacementEpoch] = {}
    for key, payload in raw.items():
        epoch = _ReplacementEpoch.from_cursor(payload)
        if epoch is not None:
            out[str(key)] = epoch
    return out


def _bounded_authority_from_cursor(
    cursor: Optional[dict[str, Any]],
) -> RecordEmission:
    if not isinstance(cursor, dict):
        return RecordEmission()
    return _bounded_authority_from_parts(
        dict(cursor.get("transcripts") or {}),
        _replacement_epochs_from_cursor(cursor),
    )


def _bounded_authority_from_parts(
    transcripts: dict[str, Any],
    epochs: dict[str, _ReplacementEpoch],
) -> RecordEmission:
    authority: RecordEmission | None = None
    for entry in transcripts.values():
        authority = _merge_optional_emissions(
            authority,
            _owned_footprint_from_cursor_entry(entry),
        )
    for epoch in epochs.values():
        authority = _merge_optional_emissions(authority, epoch.old)
        authority = _merge_optional_emissions(authority, epoch.cleanup)
        authority = _merge_optional_emissions(authority, epoch.current)
    return authority or RecordEmission()


def _replacement_transient_authority(
    epochs: dict[str, _ReplacementEpoch],
) -> RecordEmission:
    authority: RecordEmission | None = None
    for epoch in epochs.values():
        authority = _merge_optional_emissions(authority, epoch.cleanup)
        authority = _merge_optional_emissions(authority, epoch.current)
    return authority or RecordEmission()


def _record_emission_retractions(
    prior: RecordEmission,
    current: RecordEmission,
    *,
    repo: str,
    reason: str,
) -> list[NodeFact | EdgeFact]:
    current_nodes = current.nodes_by_id()
    current_edges = current.edges_by_identity()
    edge_retractions = [
        EdgeFact(
            src=edge.src,
            dst=edge.dst,
            edge_type=edge.edge_type,
            attrs=dict(edge.attrs),
            provenance=edge.provenance,
            confidence=edge.confidence,
            source_record_id=dict(edge.source_record_id),
            source_revision={reason: True},
            op="R",
        )
        for identity, edge in sorted(prior.edges_by_identity().items())
        if identity not in current_edges
    ]
    node_retractions = [
        NodeFact(
            node_id=node.node_id,
            subtype=node.subtype,
            attrs={},
            source_record_id=(
                dict(node.source_record_id)
                or {"repo": repo, "node_id": node.node_id}
            ),
            source_revision={reason: True},
            op="R",
        )
        for node_id, node in sorted(prior.nodes_by_id().items())
        if node_id not in current_nodes
    ]
    return [*edge_retractions, *node_retractions]


def _cursor_entry_incomplete(raw: Any) -> bool:
    """True when a cursor entry records a partially consumed transcript."""
    if not isinstance(raw, dict):
        return False
    state = raw.get("append_cursor")
    if not isinstance(state, dict):
        return False
    if state.get("schema") != _APPEND_CURSOR_SCHEMA:
        return False
    return not bool(state.get("complete", True))


def _tool_event_to_cursor(event: _ToolEvent) -> dict[str, Any]:
    """Serialize a tool event awaiting its output across a chunk boundary."""
    return {
        "event_id": event.event_id,
        "session_id": event.session_id,
        "tool_name": event.tool_name,
        "turn_index": int(event.turn_index),
        "block_index": int(event.block_index),
        "timestamp": event.timestamp,
        # Already JSON-decoded upstream by _decode_arguments.
        "arguments": dict(event.arguments),
        "file_paths": list(event.file_paths),
        "command": event.command,
        "input_excerpt": event.input_excerpt,
        "message_index": event.message_index,
        "mcp_server": event.mcp_server,
        "mcp_deployment": event.mcp_deployment,
        "mcp_operator": event.mcp_operator,
    }


def _tool_event_from_cursor(raw: Any) -> _ToolEvent | None:
    if not isinstance(raw, dict):
        return None
    try:
        message_index = raw.get("message_index")
        return _ToolEvent(
            event_id=str(raw["event_id"]),
            session_id=str(raw["session_id"]),
            tool_name=str(raw.get("tool_name") or ""),
            turn_index=int(raw.get("turn_index") or 0),
            block_index=int(raw.get("block_index") or 0),
            timestamp=str(raw.get("timestamp") or ""),
            arguments=dict(raw.get("arguments") or {}),
            file_paths=tuple(str(p) for p in (raw.get("file_paths") or ())),
            command=str(raw.get("command") or ""),
            input_excerpt=str(raw.get("input_excerpt") or ""),
            message_index=(
                int(message_index) if message_index is not None else None
            ),
            mcp_server=str(raw.get("mcp_server") or ""),
            mcp_deployment=str(raw.get("mcp_deployment") or ""),
            mcp_operator=str(raw.get("mcp_operator") or ""),
        )
    except (KeyError, TypeError, ValueError):
        return None


def _mcp_tool_registry_from_cursor(raw: Any) -> dict[str, McpToolTelemetry]:
    if not isinstance(raw, dict):
        return {}
    registry: dict[str, McpToolTelemetry] = {}
    for operator, payload in raw.items():
        if not isinstance(payload, dict):
            continue
        op = str(payload.get("operator") or operator).strip()
        server = str(payload.get("server") or "").strip()
        if not op or not server:
            continue
        registry[str(operator)] = McpToolTelemetry(
            server=server,
            deployment=str(payload.get("deployment") or ""),
            operator=op,
        )
    return registry


def _mcp_tool_registry_to_cursor(raw: Any) -> dict[str, dict[str, str]]:
    if not isinstance(raw, dict):
        return {}
    out: dict[str, dict[str, str]] = {}
    for operator, telemetry in raw.items():
        if not isinstance(telemetry, McpToolTelemetry):
            continue
        out[str(operator)] = {
            "server": telemetry.server,
            "deployment": telemetry.deployment,
            "operator": telemetry.operator,
        }
    return out


# The append-resume prefix check lives in `_transcript_resume`, shared with
# the Claude Code `agent_sessions` extractor. These are aliases, not
# wrappers: a fix to the shared module reaches both adapters, which is what
# the 64 KiB-window bug (fixed here, silently missed there) cost us before.
_append_delta_state = _transcript_resume.append_resume_state
_path_ends_with_newline = _transcript_resume.path_ends_with_newline
_line_hash = _transcript_resume.line_hash
# No call sites left in this module -- `_append_delta_state` is now the only
# caller and it lives in the shared module. Kept deliberately as a divergence
# tripwire: test_transcript_resume_shared asserts it is the shared function, so
# re-growing a local copy here fails loudly.
_prior_last_line_hash_matches = _transcript_resume.prior_last_line_hash_matches


def _update_mcp_tool_registry(
    registry: dict[str, McpToolTelemetry],
    ambiguous: set[str],
    payload: dict[str, Any],
) -> None:
    if payload.get("type") != "tool_search_output":
        return
    for namespace_entry in payload.get("tools") or []:
        if not isinstance(namespace_entry, dict):
            continue
        namespace = str(namespace_entry.get("name") or "")
        if parse_mcp_namespace(namespace) is None:
            continue
        for tool in namespace_entry.get("tools") or []:
            if not isinstance(tool, dict):
                continue
            operator = str(tool.get("name") or "").strip()
            if not operator:
                continue
            telemetry = parse_mcp_tool_call(operator, namespace=namespace)
            if telemetry is None:
                continue
            existing = registry.get(operator)
            if existing is not None and existing.server != telemetry.server:
                registry.pop(operator, None)
                ambiguous.add(operator)
            elif operator not in ambiguous:
                registry[operator] = telemetry


def _parse_transcript(
    path: Path,
    *,
    allowlist: set[str],
    initial_state: _TranscriptParseState | None = None,
    max_bytes: int | None = None,
    max_records: int | None = None,
    observe_identity: bool = False,
    verify_prefix_bytes: int | None = None,
) -> dict[str, Any]:
    """Parse a transcript, optionally stopping early at a byte/record budget.

    ``max_bytes``/``max_records`` bound the work a single run spends on one
    transcript (unblock-source-publish-admission). When either budget is
    exhausted the parse stops **on a line boundary** and reports
    ``consumed_bytes`` / ``complete``; the caller persists that offset and the
    next run resumes from it. At least one line is always consumed so a line
    larger than the whole budget cannot deadlock catch-up.

    ``records_parsed`` reports the records THIS call produced, so the caller
    can charge its budget in the unit the cap is expressed in.

    ``observe_identity`` also hashes, from the SAME open handle as the parse,
    every byte this call looked at: the already-consumed prefix ``[0, S)``,
    each consumed line, and a trailing partial line the parse saw and left
    for a later run. ``observed_sha256``/``observed_bytes`` then name exactly
    the content the emitted facts were decided on; the file is never read
    again to a later end of file (#2469). ``verify_prefix_bytes`` asks for
    the digest of ``[0, M)`` from that same handle so the caller can check
    the resumed prefix against the identity it recorded earlier.
    """
    meta: dict[str, Any] = (
        dict(initial_state.meta) if initial_state is not None else {}
    )
    messages: list[_MessageEvent] = []
    tool_events: list[_ToolEvent] = []
    permission_events: list[dict[str, str]] = []
    started_at = initial_state.started_at if initial_state is not None else ""
    ended_at = initial_state.ended_at if initial_state is not None else ""
    turn_count = initial_state.turn_count if initial_state is not None else 0
    block_index = initial_state.block_index if initial_state is not None else 0
    line_count = initial_state.line_count if initial_state is not None else 0
    message_count = (
        initial_state.message_count if initial_state is not None else 0
    )
    tool_event_count = (
        initial_state.tool_event_count if initial_state is not None else 0
    )
    permission_event_count = (
        initial_state.permission_event_count
        if initial_state is not None else 0
    )
    denial_count = initial_state.denial_count if initial_state is not None else 0
    interrupt_count = (
        initial_state.interrupt_count if initial_state is not None else 0
    )
    carried_last_message = (
        initial_state.last_message if initial_state is not None else None
    )
    last_message_index: int | None = (
        carried_last_message.message_index
        if carried_last_message is not None else None
    )
    last_line_hash = initial_state.last_line_hash if initial_state else ""
    usage: dict[str, Any] = (
        dict(initial_state.usage) if initial_state is not None else {}
    )
    mcp_tool_registry: dict[str, McpToolTelemetry] = (
        dict(initial_state.mcp_tool_registry)
        if initial_state is not None else {}
    )
    # Carried across chunk boundaries so a chunked parse emits the same facts
    # as one full parse (design-review-1 P2-1). Without this an operator that
    # is ambiguous across a boundary gets attributed where a full parse would
    # refuse to attribute it.
    ambiguous_mcp_operators: set[str] = set(
        initial_state.ambiguous_mcp_operators if initial_state is not None else ()
    )
    # call_id -> index into tool_events, for matching function_call_output back
    # to its call within THIS chunk.
    tool_event_index_by_call_id: dict[str, int] = {}
    # call_id -> serialized tool event whose function_call_output has not been
    # seen yet, INCLUDING events emitted by earlier chunks. A full parse stamps
    # is_error/exit_code on the event when its output arrives; a chunked parse
    # reaches the same fact set by re-emitting the carried event (node ids are
    # deterministic, so this upserts rather than duplicates).
    pending_tool_events: dict[str, dict[str, Any]] = dict(
        initial_state.pending_tool_events if initial_state is not None else {}
    )
    pending_tool_event_overflow = 0
    tool_error_count = (
        initial_state.tool_error_count if initial_state is not None else 0
    )
    parse_error_count = 0

    def _append_or_replace_message(message: _MessageEvent) -> None:
        for idx, existing in enumerate(messages):
            if existing.message_index == message.message_index:
                messages[idx] = message
                return
        messages.append(message)

    # Byte accounting is accumulated, never fh.tell(): buffered iteration
    # makes tell() report the readahead position, not the consumed prefix
    # (design-review-1 P3-3).
    consumed_bytes = initial_state.bytes if initial_state is not None else 0
    consumed_slice_hasher = hashlib.sha256()
    # Bytes this call looked at from `start`: consumed lines plus a trailing
    # partial line. The prefix before `start` is hashed only after a parse
    # that reached end of file, so a budget-stopped catch-up chunk pays
    # nothing extra.
    start = initial_state.bytes if initial_state is not None else 0
    observed_tail_hasher = hashlib.sha256() if observe_identity else None
    observed_end = start
    observed_sha256: str | None = None
    verify_prefix_sha256: str | None = None
    budget_stopped = False
    with path.open("rb") as fh:
        if initial_state is not None:
            fh.seek(initial_state.bytes)
        for raw_line in fh:
            # A trailing line with no newline is what a writer mid-append
            # leaves behind: not a record yet. Consuming it would put the stop
            # offset mid-line, so `append_safe` is false, so the next run
            # cannot resume and re-parses from zero -- and on a LIVE session
            # (which every oversized transcript is) that happens on every
            # append, restarting catch-up indefinitely. Leave it for the run
            # that sees it completed.
            if not raw_line.endswith(b"\n"):
                # The run saw these bytes and decided they are not a record
                # yet, so they belong to the observed identity.
                if observed_tail_hasher is not None:
                    observed_tail_hasher.update(raw_line)
                    observed_end += len(raw_line)
                break
            # Stop on a LINE BOUNDARY once a budget is exhausted, but only
            # after at least one line so an over-budget line still advances
            # the offset (design §8).
            consumed_this_run = consumed_bytes - (
                initial_state.bytes if initial_state is not None else 0
            )
            if consumed_this_run > 0 and (
                (max_bytes is not None and consumed_this_run >= max_bytes)
                or (
                    max_records is not None
                    and (len(messages) + len(tool_events)) >= max_records
                )
            ):
                budget_stopped = True
                break
            consumed_bytes += len(raw_line)
            consumed_slice_hasher.update(raw_line)
            if observed_tail_hasher is not None:
                observed_tail_hasher.update(raw_line)
                observed_end += len(raw_line)
            line_idx = line_count
            line_count += 1
            line = raw_line.decode("utf-8", errors="replace")
            last_line_hash = _line_hash(raw_line.rstrip(b"\n"))
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                parse_error_count += 1
                continue
            timestamp = str(row.get("timestamp") or "")
            started_at = started_at or timestamp
            ended_at = timestamp or ended_at
            typ = row.get("type")
            payload = row.get("payload")
            if typ == "session_meta" and isinstance(payload, dict):
                meta = payload
                continue
            if typ == "response_item" and isinstance(payload, dict):
                _update_mcp_tool_registry(
                    mcp_tool_registry,
                    ambiguous_mcp_operators,
                    payload,
                )
                if payload.get("type") == "function_call_output":
                    # Native error signal (exit wrapper text / metadata /
                    # success flag). The session rollup counts EVERY error
                    # output (matching the Claude source's semantics);
                    # per-event stamping applies to captured events only,
                    # same-chunk matching.
                    exit_code, is_error = _parse_call_output(
                        payload.get("output")
                    )
                    if is_error:
                        tool_error_count += 1
                    call_id = str(payload.get("call_id") or "")
                    event_idx = tool_event_index_by_call_id.pop(call_id, None)
                    carried = pending_tool_events.pop(call_id, None)
                    if event_idx is not None:
                        tool_events[event_idx] = replace(
                            tool_events[event_idx],
                            exit_code=exit_code,
                            is_error=is_error,
                        )
                    elif carried is not None:
                        # The call was emitted by an earlier chunk. Re-emit it
                        # stamped so the chunked fact set matches a full parse;
                        # without this the stamp is lost for good.
                        restored = _tool_event_from_cursor(carried)
                        if restored is not None:
                            tool_events.append(replace(
                                restored,
                                exit_code=exit_code,
                                is_error=is_error,
                            ))
                if payload.get("type") == "function_call":
                    tool_name = str(payload.get("name") or "")
                    namespace = payload.get("namespace")
                    mcp_tool = parse_mcp_tool_call(
                        tool_name,
                        namespace=str(namespace) if namespace else None,
                    )
                    if mcp_tool is None:
                        mcp_tool = mcp_tool_registry.get(tool_name)
                    if allowlist and tool_name not in allowlist and mcp_tool is None:
                        continue
                    args = _decode_arguments(payload.get("arguments"))
                    sid = str(meta.get("id") or path.stem)
                    event_id = f"tool_use:codex:{sid}:{line_idx}:0"
                    file_paths = tuple(_extract_file_paths(args, cwd=str(meta.get("cwd") or "")))
                    fc_call_id = str(payload.get("call_id") or "")
                    if fc_call_id:
                        tool_event_index_by_call_id[fc_call_id] = len(
                            tool_events
                        )
                    tool_events.append(_ToolEvent(
                        event_id=event_id,
                        session_id=f"codex:{sid}",
                        tool_name=tool_name,
                        turn_index=line_idx,
                        block_index=block_index,
                        timestamp=timestamp or str(meta.get("timestamp") or ""),
                        arguments=args,
                        file_paths=file_paths,
                        command=scrub_transcript_text(
                            _sanitize_graph_text(
                                str(args.get("cmd") or args.get("command") or "")
                            )
                        ),
                        input_excerpt=_summarize_args(args),
                        message_index=last_message_index,
                        mcp_server=mcp_tool.server if mcp_tool else "",
                        mcp_deployment=mcp_tool.deployment if mcp_tool else "",
                        mcp_operator=mcp_tool.operator if mcp_tool else "",
                    ))
                    if fc_call_id:
                        # Held so the stamp survives a chunk boundary landing
                        # between this call and its output.
                        pending_tool_events[fc_call_id] = _tool_event_to_cursor(
                            tool_events[-1]
                        )
                        if len(pending_tool_events) > _MAX_PENDING_TOOL_EVENTS:
                            pending_tool_events.pop(
                                next(iter(pending_tool_events))
                            )
                            pending_tool_event_overflow += 1
                    if last_message_index is not None:
                        current_message = next(
                            (
                                message for message in messages
                                if message.message_index == last_message_index
                            ),
                            carried_last_message,
                        )
                        if current_message is not None:
                            updated_message = replace(
                                current_message,
                                tool_call_count=(
                                    current_message.tool_call_count + 1
                                ),
                            )
                            _append_or_replace_message(updated_message)
                            if (
                                carried_last_message is not None
                                and updated_message.message_index
                                == carried_last_message.message_index
                            ):
                                carried_last_message = updated_message
                    block_index += 1
                    tool_event_count += 1
                elif payload.get("type") == "message":
                    message_index = message_count
                    message = _message_event_from_payload(
                        payload,
                        message_index=message_index,
                        line_index=line_idx,
                        timestamp=timestamp,
                    )
                    messages.append(message)
                    last_message_index = message_index
                    carried_last_message = message
                    message_count += 1
                    turn_count += 1
            if typ in {"mcp_approval_request", "approval_request"}:
                approval = payload if isinstance(payload, dict) else {}
                permission_events.append({
                    "kind": "approval_request",
                    "timestamp": timestamp,
                    "tool_name": str(approval.get("tool_name") or approval.get("name") or ""),
                    "excerpt": _truncate(
                        scrub_transcript_text(json.dumps(approval, sort_keys=True)),
                        220,
                    ),
                    "tool_input_summary": _truncate(
                        scrub_transcript_text(
                            json.dumps(approval.get("arguments") or {}, sort_keys=True)
                        ),
                        120,
                    ),
                })
                permission_event_count += 1
            if typ in {"event_msg", "turn_context"} and isinstance(payload, dict):
                if typ == "event_msg" and payload.get("type") == "token_count":
                    measured = _usage_from_token_count_event(
                        payload,
                        timestamp=timestamp,
                    )
                    if measured:
                        usage = measured
                if "interrupted" in json.dumps(payload, sort_keys=True).lower():
                    permission_events.append({
                        "kind": "interrupted",
                        "timestamp": timestamp,
                        "tool_name": "",
                        "excerpt": _truncate(
                            scrub_transcript_text(json.dumps(payload, sort_keys=True)),
                            220,
                        ),
                        "tool_input_summary": "",
                    })
                    permission_event_count += 1
                    interrupt_count += 1
        if observed_tail_hasher is not None and not budget_stopped:
            observed_sha256, verify_prefix_sha256 = _hash_observed_identity(
                fh,
                start=start,
                end=observed_end,
                tail_hasher=observed_tail_hasher,
                verify_bytes=verify_prefix_bytes,
            )

    return {
        "meta": meta,
        "messages": messages,
        "tool_events": tool_events,
        "permission_events": permission_events,
        "started_at": started_at,
        "ended_at": ended_at,
        "turn_count": turn_count,
        "message_count": message_count,
        "tool_event_count": tool_event_count,
        "permission_event_count": permission_event_count,
        "permission_event_start_index": (
            initial_state.permission_event_count
            if initial_state is not None else 0
        ),
        "denial_count": denial_count + sum(
            1 for e in permission_events if e.get("kind") == "denied"
        ),
        "tool_error_count": tool_error_count,
        "interrupt_count": interrupt_count,
        "block_index": block_index,
        "last_message": carried_last_message,
        "line_count": line_count,
        "last_line_hash": last_line_hash,
        "usage": usage,
        "mcp_tool_registry": mcp_tool_registry,
        "parse_error_count": parse_error_count,
        "delta_from_byte": (
            initial_state.bytes if initial_state is not None else None
        ),
        # Chunked-ingest accounting. `consumed_bytes` is the offset of the
        # consumed PREFIX (line-aligned); `complete` is False only when a
        # budget stopped the parse early, which is what makes the run decline
        # completeness and resume next cycle.
        "consumed_bytes": consumed_bytes,
        "consumed_slice_sha256": consumed_slice_hasher.hexdigest(),
        "complete": not budget_stopped,
        # Identity of every byte this call looked at, or None. A budget stop
        # leaves bytes unseen, so it has no whole-file observed identity.
        "observed_sha256": observed_sha256,
        "observed_bytes": observed_end,
        "verify_prefix_sha256": verify_prefix_sha256,
        "ambiguous_mcp_operators": sorted(ambiguous_mcp_operators),
        "pending_tool_events": pending_tool_events,
        "pending_tool_event_overflow": pending_tool_event_overflow,
        # Records THIS call produced, in the same unit as `max_records`.
        "records_parsed": len(messages) + len(tool_events),
    }


def _observed_parse_identity(
    parsed: dict[str, Any],
    *,
    resumed_from: int,
    prior_item: tuple[str, int] | None,
) -> tuple[str, Any, Any] | None:
    """The parse's observed ``[0, N)`` identity, or None when not honest.

    A resumed parse emitted facts only for ``[S, N)``; the facts for
    ``[0, S)`` came from earlier runs. Its identity is honest only when the
    prefix it hashed now matches the identity recorded for those earlier
    runs, so a rewrite under the cursor is never sealed over.
    """
    observed_sha = parsed.get("observed_sha256")
    if not isinstance(observed_sha, str):
        return None
    if resumed_from > 0 and (
        prior_item is None
        or parsed.get("verify_prefix_sha256") != prior_item[0]
    ):
        return None
    return ("observed", observed_sha, int(parsed.get("observed_bytes") or 0))


def _hash_observed_identity(
    fh: Any,
    *,
    start: int,
    end: int,
    tail_hasher: Any,
    verify_bytes: int | None,
) -> tuple[str | None, str | None]:
    """Digest ``[0, end)`` from the parse's handle, plus ``[0, verify_bytes)``.

    Returns ``(observed_sha256, verify_sha256)``. A parse from offset zero
    already streamed every byte it looked at, so its digest needs no further
    read. A resumed parse hashed only ``[start, end)``: the prefix is read
    from the same handle, and ``[start, end)`` is read once more and must
    match the digest taken while parsing, so the identity can never cover
    bytes other than the ones parsed. Nothing past ``end`` is read.
    ``verify_sha256`` is None when ``verify_bytes`` is unset or lies past
    ``end``; ``observed_sha256`` is None when the bytes did not hold still.
    """
    if start == 0 and verify_bytes is None:
        return tail_hasher.hexdigest(), None
    whole = hashlib.sha256()
    tail = hashlib.sha256()
    verify_sha: str | None = (
        whole.hexdigest() if verify_bytes == 0 else None
    )
    fh.seek(0)
    position = 0
    while position < end:
        stops = [end, start]
        if verify_bytes is not None:
            stops.append(verify_bytes)
        next_stop = min(stop for stop in stops if stop > position)
        chunk = fh.read(min(1024 * 1024, next_stop - position))
        if not chunk:
            return None, verify_sha
        whole.update(chunk)
        if position >= start:
            tail.update(chunk)
        position += len(chunk)
        if verify_bytes is not None and position == verify_bytes:
            verify_sha = whole.hexdigest()
    if tail.hexdigest() != tail_hasher.hexdigest():
        return None, verify_sha
    return whole.hexdigest(), verify_sha


_EXIT_WRAPPER_RE = re.compile(r"Process exited with code (\d+)")


def _parse_call_output(raw: Any) -> tuple[int | None, bool | None]:
    """Native error signal from a Codex function_call_output payload.

    The real corpus embeds the exit status as wrapper TEXT — "Process
    exited with code N" (29.9k nonzero occurrences across 644 of 990
    transcripts on the 2026-07-02 audit) — not as JSON metadata, though
    the metadata.exit_code / success shapes are also handled. Anything
    unrecognized returns (None, None): an honest unknown, never a guessed
    default (the first version defaulted unknown text to success, which
    stamped is_error=False on every real Codex failure — the exact
    silent-default P0 this PACT exists to kill)."""
    text = raw if isinstance(raw, str) else None
    if isinstance(raw, dict):
        parsed: Any = raw
    elif text is not None:
        try:
            parsed = json.loads(text)
        except (json.JSONDecodeError, ValueError):
            parsed = None
    else:
        parsed = None
    if isinstance(parsed, dict):
        metadata = parsed.get("metadata")
        exit_code = _int_or_none(
            (metadata or {}).get("exit_code")
            if isinstance(metadata, dict) else parsed.get("exit_code")
        )
        if exit_code is not None:
            return exit_code, exit_code != 0
        success = parsed.get("success")
        if isinstance(success, bool):
            return None, not success
        inner = parsed.get("output")
        text = inner if isinstance(inner, str) else text
    if text is not None:
        matches = _EXIT_WRAPPER_RE.findall(text)
        if matches:
            exit_code = int(matches[-1])
            return exit_code, exit_code != 0
        lowered = text.lstrip()[:80].lower()
        if lowered.startswith(("error:", "failed:", "exception:")):
            return None, True
    return None, None


def _usage_from_token_count_event(
    payload: dict[str, Any],
    *,
    timestamp: str,
) -> dict[str, Any]:
    info = payload.get("info")
    if not isinstance(info, dict):
        return {}
    total_usage = info.get("total_token_usage")
    if not isinstance(total_usage, dict):
        return {}

    input_tokens = _int_or_none(total_usage.get("input_tokens"))
    output_tokens = _int_or_none(total_usage.get("output_tokens"))
    cache_read_tokens = _int_or_none(total_usage.get("cached_input_tokens"))
    reasoning_output_tokens = _int_or_none(
        total_usage.get("reasoning_output_tokens"),
    )
    total_tokens = _int_or_none(total_usage.get("total_tokens"))
    if total_tokens is None and input_tokens is not None and output_tokens is not None:
        total_tokens = input_tokens + output_tokens
    if input_tokens is None and output_tokens is None and total_tokens is None:
        return {}

    measured: dict[str, Any] = {
        "cost_confidence": "measured",
        "cost_basis": "codex_token_count_event",
        "cost_measurement_source": "codex.event_msg.token_count",
        "usage_recorded_at": timestamp,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cache_read_tokens": cache_read_tokens,
        "reasoning_output_tokens": reasoning_output_tokens,
        "total_tokens": total_tokens,
        "model_context_window": _int_or_none(info.get("model_context_window")),
    }
    cost_usd = _float_or_none(
        _first_present(
            total_usage.get("cost_usd"),
            info.get("cost_usd"),
            payload.get("cost_usd"),
        ),
    )
    if cost_usd is not None:
        measured["cost_usd_estimate"] = cost_usd
    return {k: v for k, v in measured.items() if v not in {None, ""}}


def _first_present(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _message_event_from_payload(
    payload: dict[str, Any],
    *,
    message_index: int,
    line_index: int,
    timestamp: str,
) -> _MessageEvent:
    role = str(payload.get("role") or "assistant")
    content = payload.get("content", payload)
    parent = (
        payload.get("parent_id")
        or payload.get("parent_message_id")
        or payload.get("previous_item_id")
        or ""
    )
    return _MessageEvent(
        role=role,
        message_index=message_index,
        line_index=line_index,
        timestamp=timestamp,
        message_kind=_codex_message_kind(role, content),
        content_hash=_stable_content_hash(content),
        content_excerpt=_message_excerpt(content),
        tool_call_count=0,
        parent_message_id=str(parent) if parent else "",
    )


def _codex_message_kind(role: str, content: Any) -> str:
    if _payload_has_type(content, "tool_result"):
        return "tool_result"
    if role == "user":
        return "user_prompt"
    if role == "system":
        return "system"
    return "assistant_text"


def _payload_has_type(content: Any, block_type: str) -> bool:
    if isinstance(content, list):
        return any(
            isinstance(block, dict) and block.get("type") == block_type
            for block in content
        )
    return False


def _stable_content_hash(content: Any) -> str:
    try:
        text = json.dumps(content, sort_keys=True, separators=(",", ":"), default=str)
    except TypeError:
        text = str(content)
    text = scrub_transcript_text(text)
    return hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()


def _message_excerpt(content: Any) -> str:
    text = _content_to_text(content)
    text = re.sub(r"\s+", " ", text).strip()
    text = scrub_transcript_text(text)
    return _sanitize_graph_text(text)[:500]


def _content_to_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict):
                if "text" in block:
                    parts.append(str(block.get("text") or ""))
                elif "content" in block:
                    parts.append(_content_to_text(block.get("content")))
                elif block.get("type"):
                    parts.append(str(block.get("type")))
        return "\n".join(p for p in parts if p)
    if isinstance(content, dict):
        if "text" in content:
            return str(content.get("text") or "")
        if "content" in content:
            return _content_to_text(content.get("content"))
        return json.dumps(content, sort_keys=True, default=str)
    return ""


def _decode_arguments(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str) or not raw:
        return {}
    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError:
        return {"raw": raw}
    return decoded if isinstance(decoded, dict) else {"value": decoded}


def _extract_file_paths(args: dict[str, Any], *, cwd: str) -> list[str]:
    out: set[str] = set()
    for key, value in _walk_json(args):
        if isinstance(value, str):
            if key in {"path", "file", "filename", "workdir"} or key.endswith("_path"):
                rel = _normalize_repo_path(value, cwd)
                if rel:
                    out.add(rel)
            if key in {"cmd", "command", "raw", "input", "patch"}:
                for match in _PATH_RE.finditer(value):
                    rel = _normalize_repo_path(match.group("path").strip("'\""), cwd)
                    if rel:
                        out.add(rel)
                for match in _PATCH_FILE_RE.finditer(value):
                    rel = _normalize_repo_path(match.group("path").strip(), cwd)
                    if rel:
                        out.add(rel)
    return sorted(out)


def _walk_json(value: Any, prefix: str = "") -> Iterable[tuple[str, Any]]:
    if isinstance(value, dict):
        for key, child in value.items():
            key_s = str(key)
            yield key_s, child
            yield from _walk_json(child, key_s)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_json(child, prefix)


def _normalize_repo_path(path_text: str, cwd: str) -> str:
    if not path_text or "://" in path_text:
        return ""
    path_text = path_text.strip()
    if path_text.startswith("-") or path_text in {".", ".."}:
        return ""
    candidate = Path(path_text).expanduser()
    if candidate.is_absolute():
        try:
            cwd_path = Path(cwd).expanduser().resolve()
            resolved = candidate.resolve()
            if resolved == cwd_path:
                return ""
            if cwd_path in resolved.parents:
                return resolved.relative_to(cwd_path).as_posix()
        except OSError:
            return ""
        return ""
    if path_text.startswith("../"):
        return ""
    rel = path_text.removeprefix("./")
    if rel.startswith((".git/", ".venv/", "__pycache__/")):
        return ""
    if not re.search(r"\.[A-Za-z0-9_+-]+$", rel):
        return ""
    return rel


def _summarize_args(args: dict[str, Any]) -> str:
    if "cmd" in args:
        return _truncate(scrub_transcript_text(str(args["cmd"])), 500)
    if "command" in args:
        return _truncate(scrub_transcript_text(str(args["command"])), 500)
    return _truncate(scrub_transcript_text(json.dumps(args, sort_keys=True)), 500)


def _truncate(text: str, limit: int) -> str:
    text = _sanitize_graph_text(text)
    return text if len(text) <= limit else text[:limit]


def _sanitize_graph_text(text: str) -> str:
    """Remove characters Postgres cannot store inside JSON/text values."""
    return "".join(
        ch
        for ch in text
        if ch == "\n" or ch == "\t" or ord(ch) >= 32
    )


def _source_kind(source: Any) -> str:
    if isinstance(source, str):
        return source
    if isinstance(source, dict):
        if "subagent" in source:
            return "subagent"
        return ",".join(sorted(str(k) for k in source.keys()))
    return ""


def _since_cutoff(since: str) -> datetime | None:
    if since in {"", "all"}:
        return None
    m = re.fullmatch(r"(\d+)([dhw])", since.strip())
    if not m:
        return None
    value = int(m.group(1))
    unit = m.group(2)
    if unit == "d":
        delta = timedelta(days=value)
    elif unit == "h":
        delta = timedelta(hours=value)
    else:
        delta = timedelta(weeks=value)
    return datetime.now(UTC) - delta


def _os_from_cwd(cwd: str) -> str:
    if cwd.startswith("/Users/"):
        return "darwin"
    if cwd.startswith("/home/") or cwd.startswith("/workspace/"):
        return "linux"
    if re.match(r"^[A-Za-z]:\\", cwd):
        return "windows"
    return ""
