"""AgentSessionSource — emit graph nodes from Claude Code session transcripts.

Coarse mode: one AgentSession per JSONL, plus children (chapters,
subagents, chips, permission events) and references (skills, files,
proposals). Per-turn / per-tool-call subtypes are out of scope.

State persisted between runs (via ``set_run_state`` / ``get_run_state``)::

    {
        "<transcript_path_relative_to_claude_projects_root>": {
            "session_id": str,
            "transcript_bytes": int,
            "last_ingested_at": "<iso8601>",
            "append_cursor": {...},
            "subagent_fingerprint": "<sha256>",
            "owned_nodes": [{"node_id": str, "subtype": str}],
            "owned_edges": [{"src": str, "edge_type": str, "dst": str}],
            "owned_footprint_complete": bool,
        },
        ...
    }

Incremental short-circuit: a session whose transcript size is unchanged
AND whose last_ingested_at is more than ``idle_threshold_hours`` in the
past is skipped entirely. Active sessions (size grew) are re-scanned.
"""

from __future__ import annotations

import fnmatch
import hashlib
import json
import logging
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from okg.substrate.library.sources.source_specs import EdgeSpec, NodeSpec
from okg.substrate.library.sources._agent_sessions_extract import (
    SessionScan,
    SessionScanState,
    append_state_for_path,
    extract_proposal_slugs,
    os_from_cwd,
    parse_since,
    path_ends_with_newline,
    prior_last_line_hash_matches,
    resolve_repo,
    scan_transcript,
    scan_state_from_cursor,
    scan_state_to_cursor,
)
from okg.substrate.library.sources._claude_project_scope import (
    is_allowed_claude_worktree_cwd,
    is_claude_project_dir_candidate,
    is_repo_local_claude_worktree_cwd,
    normalize_claude_path,
)
from okg.substrate.transcript_trust import untrusted_web_tool_result_attrs

logger = logging.getLogger(__name__)

_SESSION_OWNED_NODE_SUBTYPES = frozenset({
    "agent_session",
    "agent_chapter",
    "agent_message",
    "spawned_task_chip",
    "permission_event",
    "tool_use_event",
    "sub_agent_session",
})


def subagent_tree_fingerprint(
    parent_jsonl: Path,
    parent_session_id: str | None = None,
) -> str:
    """Return a bounded fingerprint for one parent's nested subagent files."""
    session_id = parent_session_id or parent_jsonl.stem
    session_dir = parent_jsonl.parent / session_id
    sub_dir = session_dir / "subagents"
    digest = hashlib.sha256(b"okg.agent_sessions.subagent_tree.v1")
    if (
        session_dir.is_symlink()
        or sub_dir.is_symlink()
        or not sub_dir.is_dir()
    ):
        return digest.hexdigest()
    paths = sorted(
        (*sub_dir.glob("agent-*.jsonl"), *sub_dir.glob("agent-*.meta.json")),
        key=lambda path: path.name,
    )
    for path in paths:
        if path.is_symlink() or not path.is_file():
            continue
        try:
            stat = path.stat()
        except OSError:
            continue
        digest.update(b"\x00")
        digest.update(path.name.encode("utf-8"))
        digest.update(b"\x00")
        digest.update(str(stat.st_size).encode("ascii"))
        digest.update(b"\x00")
        digest.update(str(stat.st_mtime_ns).encode("ascii"))
        if path.name.endswith(".meta.json"):
            try:
                content_hash = hashlib.sha256(path.read_bytes()).digest()
            except OSError:
                continue
            digest.update(b"\x00")
            digest.update(content_hash)
    return digest.hexdigest()


@dataclass
class AgentSessionSource:
    """Ingest source for Claude Code session JSONL transcripts.

    Parameters
    ----------
    claude_projects_root : Path
        Where Claude Code stores per-cwd session directories.
        Defaults to ``~/.claude/projects``.
    cwd_filter : list[Path] | None
        If set, only sessions whose `cwd` exactly matches one of these
        paths are ingested. Useful for restricting to one deployment.
    cwd_to_repo : dict[str, str] | None
        Override the cwd → repo short-name mapping. Without this, the
        repo defaults to the cwd's basename.
    since : str
        Cold-start filter: only ingest sessions with mtime within this
        window. Accepts ``"30d"``, ``"7d"``, ``"24h"``, an ISO date, or
        ``"all"`` for unbounded. Default ``"30d"``. Subsequent
        incremental runs ignore this — per-session state controls
        re-ingest.
    idle_threshold_hours : int
        Sessions whose size is unchanged AND whose ``last_ingested_at``
        is more than this many hours in the past are skipped entirely.
        Default 24.
    """

    claude_projects_root: Path = field(
        default_factory=lambda: Path("~/.claude/projects").expanduser()
    )
    display_projects_root: Path | None = None
    cwd_filter: list[Path] | None = None
    cwd_globs: list[str] | None = None
    claude_worktree_roots: tuple[Path | str, ...] = ()
    cwd_to_repo: dict[str, str] | None = None
    since: str = "30d"
    idle_threshold_hours: int = 24

    # Cost tracking (see okg.modules.agent_sessions.cost). When True,
    # session and subagent nodes carry cost_usd_estimate +
    # cost_table_version. Token counters populate regardless.
    cost_estimation: bool = True
    cost_table_path: str | None = None

    # Per `add-tool-use-event-capture` (2026-04-27): opt-in
    # allowlist of tool names that emit `tool_use_event` nodes.
    # Default None = zero events. Threaded into `scan_transcript`.
    capture_tool_use_events: list[str] | None = None
    emit_proposal_slug_refs: bool = True
    emit_ssh_host_refs: bool = True
    _cost_table: Any = None

    state: dict[str, Any] = field(default_factory=dict)
    _emit_nodes: list[NodeSpec] | None = None
    _emit_edges: list[EdgeSpec] | None = None
    _emit_aliases: dict[str, str] = field(default_factory=dict)
    last_run_stats: dict[str, Any] = field(default_factory=dict, init=False)
    _stale: list[str] = field(default_factory=list)

    ownership_id: str = "agent_sessions"
    supports_incremental: bool = True
    run_state_kind: str = (
        "metadata"  # bind_run_state passes us state.metadata as a dict
    )

    def __post_init__(self) -> None:
        # Coerce string config values (from YAML) to proper types.
        # The deployment pipeline passes raw dict values from
        # modules_config[name]; users typing paths in YAML get strings.
        from okg.substrate.library.sources._agent_sessions_cost import load_cost_table

        self._cost_table = load_cost_table(self.cost_table_path)
        if isinstance(self.claude_projects_root, str):
            self.claude_projects_root = Path(self.claude_projects_root).expanduser()
        if isinstance(self.display_projects_root, str):
            self.display_projects_root = Path(
                self.display_projects_root
            ).expanduser()
        if self.cwd_filter:
            self.cwd_filter = [
                Path(p).expanduser() if isinstance(p, str) else p
                for p in self.cwd_filter
            ]
        if self.cwd_globs:
            self.cwd_globs = [str(Path(p).expanduser()) for p in self.cwd_globs]
        self.claude_worktree_roots = tuple(
            Path(p).expanduser() if isinstance(p, str) else p
            for p in (self.claude_worktree_roots or ())
        )

    # --- Source protocol ----------------------------------------------------

    def nodes(self) -> Iterable[NodeSpec]:
        if self._emit_nodes is None:
            self._compute_emit()
        yield from self._emit_nodes or []

    def edges(self) -> Iterable[EdgeSpec]:
        if self._emit_edges is None:
            self._compute_emit()
        yield from self._emit_edges or []

    def aliases(self) -> dict[str, str]:
        if self._emit_nodes is None:
            self._compute_emit()
        return dict(self._emit_aliases)

    # --- State hooks --------------------------------------------------------

    def set_run_state(self, state: Any) -> None:
        self.state = dict(state) if state else {}

    def get_run_state(self) -> dict[str, Any]:
        return dict(self.state)

    @property
    def stale_node_ids(self) -> list[str]:
        return list(self._stale)

    # --- Implementation ------------------------------------------------------

    def _compute_emit(self) -> None:
        nodes: list[NodeSpec] = []
        edges: list[EdgeSpec] = []
        aliases: dict[str, str] = {}
        new_state: dict[str, Any] = dict(self.state)
        stats: dict[str, Any] = {
            "files_seen": 0,
            "files_skipped_unchanged": 0,
            "files_skipped_cwd_filter": 0,
            "files_rescanned_filter_changed": 0,
            "files_rescanned_subagent_changed": 0,
            "files_scanned": 0,
            "files_scanned_append_delta": 0,
            "files_scanned_full": 0,
            "bytes_seen": 0,
            "bytes_delta": 0,
            "lines_delta": 0,
            "append_reject_missing_cursor": 0,
            "append_reject_prior_not_append_safe": 0,
            "append_reject_not_grown": 0,
            "append_reject_missing_last_line_hash": 0,
            "append_reject_last_line_hash_mismatch": 0,
            "append_reject_unknown": 0,
        }

        # Track Agent and Skill dedup across sessions in this run
        agent_emitted: set[str] = set()
        skill_emitted: set[str] = set()
        source_file_emitted: set[str] = set()
        filter_signature = self._filter_signature()

        for jsonl_path in self._iter_session_files():
            try:
                stat = jsonl_path.stat()
                cur_size = stat.st_size
            except OSError:
                continue

            stats["files_seen"] += 1
            stats["bytes_seen"] += cur_size
            state_key = self._state_key_for_path(jsonl_path)
            stem = jsonl_path.stem
            prior_key = state_key if state_key in self.state else None
            prior = self.state.get(state_key)
            if prior is None and stem in self.state:
                prior_key = stem
                prior = self.state.get(stem)

            def store_state(payload: dict[str, Any]) -> None:
                new_state[state_key] = payload
                for legacy_key in {stem, prior_key or ""}:
                    if legacy_key and legacy_key != state_key:
                        new_state.pop(legacy_key, None)

            if prior:
                prior_size = int(prior.get("transcript_bytes") or 0)
                last_ts = prior.get("last_ingested_at") or ""
                nested_fingerprint = subagent_tree_fingerprint(
                    jsonl_path,
                    str(prior.get("session_id") or stem),
                )
                if cur_size == prior_size:
                    if prior.get("filter_signature") != filter_signature:
                        stats["files_rescanned_filter_changed"] += 1
                    elif prior.get("subagent_fingerprint") != nested_fingerprint:
                        stats["files_rescanned_subagent_changed"] += 1
                    else:
                        # Skip if idle, otherwise just refresh the timestamp
                        stats["files_skipped_unchanged"] += 1
                        if self._is_idle(last_ts):
                            store_state(dict(prior))
                            continue
                        # Active but not yet idle, no growth → skip and refresh ts
                        store_state({
                            **prior,
                            "transcript_bytes": prior_size,
                            "last_ingested_at": _now_iso(),
                        })
                        continue
                # Else: fall through to re-scan

            scan_cursor = scan_state_from_cursor(prior)
            initial_state = append_state_for_path(
                scan_cursor,
                path=jsonl_path,
                size=cur_size,
            )
            if prior and initial_state is None:
                reject_reason = _append_reject_reason(
                    scan_cursor,
                    path=jsonl_path,
                    size=cur_size,
                )
                stats[f"append_reject_{reject_reason}"] += 1
            stats["files_scanned"] += 1
            if initial_state is not None:
                stats["files_scanned_append_delta"] += 1
                stats["bytes_delta"] += max(0, cur_size - initial_state.bytes)
            else:
                stats["files_scanned_full"] += 1
                stats["bytes_delta"] += cur_size
            scan = scan_transcript(
                jsonl_path,
                capture_tool_use_events=self.capture_tool_use_events,
                initial_state=initial_state,
            )
            if initial_state is not None:
                stats["lines_delta"] += max(
                    0,
                    scan.line_count - initial_state.line_count,
                )
            else:
                stats["lines_delta"] += scan.line_count
            if scan.session_id is None:
                # Couldn't extract sessionId — log + skip
                logger.warning("Skipping %s: no sessionId found", jsonl_path)
                continue

            # Apply cwd filter (post-scan because cwd is in the JSONL).
            # Prefix match so a filter of `/path/to/repo` also captures
            # sessions whose cwd is a worktree under that repo.
            if (
                self.cwd_filter
                or self.cwd_globs
                or is_repo_local_claude_worktree_cwd(scan.cwd)
            ) and not _cwd_matches(
                scan.cwd,
                self.cwd_filter or [],
                self.cwd_globs or [],
                self.claude_worktree_roots,
            ):
                stats["files_skipped_cwd_filter"] += 1
                store_state({
                    "session_id": scan.session_id,
                    "transcript_bytes": cur_size,
                    "last_ingested_at": _now_iso(),
                    "filtered_out": True,
                    "filter_signature": filter_signature,
                    "owned_nodes": [],
                    "owned_footprint_complete": True,
                    "subagent_fingerprint": subagent_tree_fingerprint(
                        jsonl_path,
                        scan.session_id,
                    ),
                    **scan_state_to_cursor(
                        scan,
                        bytes_size=cur_size,
                        mtime_ns=stat.st_mtime_ns,
                        append_safe=path_ends_with_newline(jsonl_path, cur_size),
                    ),
                })
                continue

            node_baseline = len(nodes)
            edge_baseline = len(edges)
            self._materialize_session(
                scan,
                jsonl_path=jsonl_path,
                nodes=nodes,
                edges=edges,
                aliases=aliases,
                agent_emitted=agent_emitted,
                skill_emitted=skill_emitted,
                source_file_emitted=source_file_emitted,
            )
            # Stamp every edge added by this session with the session's
            # started_at as observed_at — overrides the orchestrator's
            # ingest-time default with a per-domain timestamp.
            if scan.started_at:
                for e in edges[edge_baseline:]:
                    e.attrs.setdefault("observed_at", scan.started_at)
            store_state({
                "session_id": scan.session_id,
                "transcript_bytes": cur_size,
                "last_ingested_at": _now_iso(),
                "filtered_out": False,
                "filter_signature": filter_signature,
                "owned_nodes": [
                    {"node_id": node.node_id, "subtype": node.subtype}
                    for node in nodes[node_baseline:]
                    if node.subtype in _SESSION_OWNED_NODE_SUBTYPES
                ],
                "owned_footprint_complete": initial_state is None,
                "subagent_fingerprint": subagent_tree_fingerprint(
                    jsonl_path,
                    scan.session_id,
                ),
                **scan_state_to_cursor(
                    scan,
                    bytes_size=cur_size,
                    mtime_ns=stat.st_mtime_ns,
                    append_safe=path_ends_with_newline(jsonl_path, cur_size),
                ),
            })
            if scan.session_id and scan.session_id != state_key:
                new_state.pop(scan.session_id, None)

        self._emit_nodes = nodes
        self._emit_edges = edges
        self._emit_aliases = aliases
        self.state = new_state
        self.last_run_stats = {
            **stats,
            "emitted_nodes": len(nodes),
            "emitted_edges": len(edges),
        }

    # --- Session materialization --------------------------------------------

    def _materialize_session(
        self,
        scan: SessionScan,
        *,
        jsonl_path: Path,
        nodes: list[NodeSpec],
        edges: list[EdgeSpec],
        aliases: dict[str, str],
        agent_emitted: set[str],
        skill_emitted: set[str],
        source_file_emitted: set[str],
    ) -> None:
        sid = scan.session_id  # type: ignore[assignment]
        session_node_id = f"session:{sid}"

        # Agent (deduped across sessions in this run)
        agent_id = f"agent:{scan.model or 'unknown'}:{scan.harness}"
        if agent_id not in agent_emitted:
            nodes.append(
                NodeSpec(
                    node_id=agent_id,
                    subtype="agent",
                    label=scan.model or "unknown",
                    attrs={
                        "agent_id": agent_id,
                        "model_name": scan.model or "unknown",
                        "harness": scan.harness,
                    },
                    source=self.ownership_id,
                )
            )
            agent_emitted.add(agent_id)
            if scan.model:
                aliases[scan.model] = agent_id

        # Pre-scan subagent transcripts so the parent session's token
        # counters can roll them up. We re-use the scans below when
        # emitting SubAgentSession nodes (no double parse).
        subagent_metas: list[tuple[str, dict]] = list(
            self._iter_subagents(jsonl_path, sid)
        )
        subagent_scans: dict[str, Any] = {}  # subagent_id -> SessionScan
        for sub_id, meta in subagent_metas:
            sub_path = Path(str(meta["__transcript_path"]))
            if (
                not sub_path.is_symlink()
                and sub_path.is_file()
                and sub_path.stat().st_size > 0
            ):
                try:
                    subagent_scans[sub_id] = scan_transcript(
                        sub_path,
                        capture_tool_use_events=self.capture_tool_use_events,
                    )
                except Exception as e:  # noqa: BLE001
                    logger.warning("subagent scan failed for %s: %s", sub_path, e)

        # Session
        # Roll up subagent token counters into the parent session's totals.
        _sub_scans = list(subagent_scans.values())
        sub_in = sum(s.total_input_tokens for s in _sub_scans)
        sub_out = sum(s.total_output_tokens for s in _sub_scans)
        sub_cr = sum(s.total_cache_read_tokens for s in _sub_scans)
        sub_cc = sum(s.total_cache_creation_tokens for s in _sub_scans)
        sub_calls = sum(s.model_calls for s in _sub_scans)

        sess_input = scan.total_input_tokens + sub_in
        sess_output = scan.total_output_tokens + sub_out
        sess_cache_read = scan.total_cache_read_tokens + sub_cr
        sess_cache_create = scan.total_cache_creation_tokens + sub_cc
        sess_calls = scan.model_calls + sub_calls

        cost_usd, cost_table_version = (None, None)
        if self.cost_estimation:
            from okg.substrate.library.sources._agent_sessions_cost import (
                compute_cost_usd,
            )

            cost_usd, cost_table_version = compute_cost_usd(
                scan.model,
                sess_input,
                sess_output,
                sess_cache_read,
                sess_cache_create,
                self._cost_table,
            )

        # Per dogfood-canary DQ audit (2026-05-19): synthesize a
        # `title` from the cwd basename (the worktree slug) +
        # started_at so the portfolio dashboard has something
        # human-readable to display. Before: 0/145 sessions had
        # title; the field was never set. The first chapter title
        # (when present) is a more meaningful summary; fall back
        # to worktree-slug if no chapters were recorded.
        worktree_basename = ""
        if scan.cwd:
            worktree_basename = scan.cwd.rstrip("/").rsplit("/", 1)[-1] or scan.cwd
        first_chapter_title = scan.chapters[0].title if scan.chapters else ""
        title = first_chapter_title or (
            f"{worktree_basename} @ {scan.started_at[:10]}"
            if worktree_basename and scan.started_at
            else worktree_basename or sid
        )

        session_attrs: dict[str, Any] = {
            "session_id": sid,
            "title": title,
            "transcript_path": str(self._display_path(jsonl_path)),
            "cwd": scan.cwd or "",
            "cc_version": scan.cc_version,
            "os": os_from_cwd(scan.cwd),
            "git_branch": scan.git_branch,
            "started_at": scan.started_at,
            "ended_at": scan.ended_at,
            "model": scan.model,
            "turn_count": scan.turn_count,
            "tool_call_count": scan.tool_call_count,
            "input_tokens": sess_input or None,
            "output_tokens": sess_output or None,
            "cache_read_tokens": sess_cache_read or None,
            "cache_creation_tokens": sess_cache_create or None,
            "total_tokens": (sess_input + sess_output) or None,
            "model_calls": sess_calls or None,
            "cost_usd_estimate": cost_usd,
            "cost_table_version": cost_table_version,
            "auto_allow_count": scan.auto_allow_count,
            "denial_count": sum(
                1 for p in scan.permission_events if p.kind == "denied"
            ),
            "hook_block_count": sum(
                1 for p in scan.permission_events if p.kind == "hook_blocked"
            ),
            "tool_error_count": scan.tool_error_count,
            "interrupt_count": sum(
                1 for p in scan.permission_events if p.kind == "interrupted"
            ),
            "permission_mode": scan.permission_mode,
        }
        # Drop null-valued optional fields so the schema layer doesn't complain
        session_attrs = {k: v for k, v in session_attrs.items() if v is not None}

        nodes.append(
            NodeSpec(
                node_id=session_node_id,
                subtype="agent_session",
                label=sid,
                attrs=session_attrs,
                source=self.ownership_id,
            )
        )
        aliases[sid] = session_node_id
        edges.append(
            EdgeSpec(src=agent_id, dst=session_node_id, edge_type="assigned_to")
        )

        # Chapters
        for ch in scan.chapters:
            chapter_id = f"chapter:{sid}:{ch.index}"
            ch_attrs: dict[str, Any] = {
                "chapter_id": chapter_id,
                "session_id": sid,
                "title": ch.title,
                "index": ch.index,
            }
            if ch.summary:
                ch_attrs["summary"] = ch.summary
            if ch.started_at:
                ch_attrs["started_at"] = ch.started_at
            nodes.append(
                NodeSpec(
                    node_id=chapter_id,
                    subtype="agent_chapter",
                    label=ch.title[:80],
                    attrs=ch_attrs,
                    source=self.ownership_id,
                )
            )
            edges.append(
                EdgeSpec(src=session_node_id, dst=chapter_id, edge_type="contains")
            )

        # Agent messages. Store bounded excerpts + hashes, not full
        # transcript payloads, so agents can navigate the trace without
        # turning the graph into a chat log.
        self._materialize_messages(
            owner_node_id=session_node_id,
            message_owner_id=sid,
            session_id=sid,
            scan=scan,
            nodes=nodes,
            edges=edges,
            include_chapter_edges=True,
        )

        # Spawn-task chips
        for chip in scan.chips:
            chip_id = f"chip:{sid}:{chip.index}"
            chip_attrs: dict[str, Any] = {
                "chip_id": chip_id,
                "session_id": sid,
                "title": chip.title,
                "prompt": chip.prompt,
            }
            if chip.tldr:
                chip_attrs["tldr"] = chip.tldr
            nodes.append(
                NodeSpec(
                    node_id=chip_id,
                    subtype="spawned_task_chip",
                    label=chip.title[:80],
                    attrs=chip_attrs,
                    source=self.ownership_id,
                )
            )
            edges.append(
                EdgeSpec(src=session_node_id, dst=chip_id, edge_type="contains")
            )
            # Also link to the parent chapter when the chip lived inside one
            if chip.chapter_index is not None and chip.chapter_index < len(
                scan.chapters
            ):
                chapter_id = f"chapter:{sid}:{chip.chapter_index}"
                edges.append(
                    EdgeSpec(src=chapter_id, dst=chip_id, edge_type="contains")
                )

        # Skills (intra-module references)
        for skill in scan.skills:
            skill_node_id = f"skill:{skill.name}"
            if skill_node_id not in skill_emitted:
                attrs: dict[str, Any] = {"name": skill.name}
                if skill.plugin:
                    attrs["plugin"] = skill.plugin
                nodes.append(
                    NodeSpec(
                        node_id=skill_node_id,
                        subtype="skill",
                        label=skill.name,
                        attrs=attrs,
                        source=self.ownership_id,
                    )
                )
                skill_emitted.add(skill_node_id)
                aliases[skill.name] = skill_node_id
            edges.append(
                EdgeSpec(src=session_node_id, dst=skill_node_id, edge_type="references")
            )

        # Permission events
        repo_for_session = resolve_repo(scan.cwd or "", self.cwd_to_repo)
        cwd_prefix_for_session = (scan.cwd.rstrip("/") + "/") if scan.cwd else None
        for idx, pev in enumerate(
            scan.permission_events,
            start=scan.permission_event_start_index,
        ):
            pev_id = f"pevent:{sid}:{idx}"
            pev_attrs: dict[str, Any] = {
                "event_id": pev_id,
                "session_id": sid,
                "kind": pev.kind,
                "timestamp": pev.timestamp,
            }
            for k in ("tool_name", "excerpt", "tool_input_summary", "chapter_index"):
                v = getattr(pev, k)
                if v is not None:
                    pev_attrs[k] = v
            nodes.append(
                NodeSpec(
                    node_id=pev_id,
                    subtype="permission_event",
                    label=f"{pev.kind}{f' ({pev.tool_name})' if pev.tool_name else ''}",
                    attrs=pev_attrs,
                    source=self.ownership_id,
                )
            )
            edges.append(
                EdgeSpec(src=session_node_id, dst=pev_id, edge_type="contains")
            )
            # Chapter-parent linkage when the event happened inside a chapter
            if (
                pev.chapter_index is not None
                and pev.chapter_index < scan.chapter_count
            ):
                chapter_id = f"chapter:{sid}:{pev.chapter_index}"
                edges.append(EdgeSpec(src=chapter_id, dst=pev_id, edge_type="contains"))
            # Cross-module: PermissionEvent → SourceFile when the halted
            # tool's input had a file_path under this session's cwd
            if pev.tool_input_file_path and repo_for_session and cwd_prefix_for_session:
                fp = pev.tool_input_file_path
                if fp.startswith(cwd_prefix_for_session):
                    relpath = fp[len(cwd_prefix_for_session) :]
                elif not fp.startswith("/"):
                    relpath = fp
                else:
                    relpath = None  # absolute path outside this cwd → skip
                if relpath:
                    edges.append(
                        EdgeSpec(
                            src=pev_id,
                            dst=f"file:{repo_for_session}:{relpath}",
                            edge_type="references",
                            attrs={"reason": "halted_tool_target"},
                        )
                    )

        # Tool-use events. Native-tool fine mode is allowlist-gated;
        # parseable MCP calls bypass that native allowlist so
        # self-observation telemetry is always visible. The scan loop
        # materializes the chosen set before we get here. file_path
        # is normalized to repo-relative (matches the convention
        # used by permission_event above + by source_file.path) so
        # the `tool_use_event affects source_file` cross-link rule
        # can use exact-match. Files outside the session's cwd are
        # not stored as file_path (treated as unlinkable).
        for tue in scan.tool_use_events:
            tue_id = f"tool_use:{sid}:{tue.turn_index}:{tue.block_index}"
            tue_attrs: dict[str, Any] = {
                "event_id": tue_id,
                "session_id": sid,
                "tool_name": tue.tool_name,
                "turn_index": tue.turn_index,
                "block_index": tue.block_index,
                "timestamp": tue.timestamp,
            }
            relpath: str | None = None
            if tue.file_path:
                fp = tue.file_path
                if cwd_prefix_for_session and fp.startswith(cwd_prefix_for_session):
                    relpath = fp[len(cwd_prefix_for_session) :]
                elif not fp.startswith("/"):
                    relpath = fp
                # else: absolute path outside cwd → skip; do NOT
                # store the absolute path (would be misleading
                # for the top-N-files query and unmatchable by
                # the cross-link rule)
            if relpath:
                tue_attrs["file_path"] = relpath
            for k in ("command", "input_excerpt", "chapter_index"):
                v = getattr(tue, k)
                if v is not None:
                    tue_attrs[k] = v
            # Native error signal (reduce-agent-friction
            # req.friction.event-observability). Absent = no result was
            # observed in the scanned chunk, an honest unknown.
            for k in ("is_error", "error_kind"):
                v = getattr(tue, k)
                if v is not None:
                    tue_attrs[k] = v
            for k in ("mcp_server", "mcp_deployment", "mcp_operator"):
                v = getattr(tue, k)
                if v is not None:
                    tue_attrs[k] = v
            nodes.append(
                NodeSpec(
                    node_id=tue_id,
                    subtype="tool_use_event",
                    label=tue.tool_name,
                    attrs=tue_attrs,
                    source=self.ownership_id,
                )
            )
            edges.append(
                EdgeSpec(
                    src=session_node_id,
                    dst=tue_id,
                    edge_type="contains",
                )
            )
            if (
                tue.chapter_index is not None
                and tue.chapter_index < scan.chapter_count
            ):
                chapter_id = f"chapter:{sid}:{tue.chapter_index}"
                edges.append(
                    EdgeSpec(
                        src=chapter_id,
                        dst=tue_id,
                        edge_type="contains",
                    )
                )
            if tue.message_index is not None:
                message_id = f"message:{sid}:{tue.message_index}"
                edges.append(
                    EdgeSpec(
                        src=message_id,
                        dst=tue_id,
                        edge_type="contains",
                    )
                )

        # Subagents — also scan their transcripts for skills + file edits
        # so subagent activity contributes to the cross-module graph.
        # Re-uses subagent_scans pre-collected above for token rollup.
        for sub_id, meta in subagent_metas:
            sub_node_id = f"subsession:{sub_id}"
            sub_transcript = Path(str(meta["__transcript_path"]))
            sub_attrs: dict[str, Any] = {
                "subagent_id": sub_id,
                "parent_session_id": sid,
                "agent_type": meta.get("agentType") or "other",
                "transcript_path": str(self._display_path(sub_transcript)),
            }
            if meta.get("description"):
                sub_attrs["description"] = meta["description"]
            # Token counters from the pre-collected scan (own transcript
            # only — does not include any nested subagents, matching
            # the spec).
            _ss = subagent_scans.get(sub_id)
            if _ss is not None:
                _in, _out = _ss.total_input_tokens, _ss.total_output_tokens
                _cr, _cc = _ss.total_cache_read_tokens, _ss.total_cache_creation_tokens
                if _in or _out or _cr or _cc:
                    sub_attrs["input_tokens"] = _in or None
                    sub_attrs["output_tokens"] = _out or None
                    sub_attrs["cache_read_tokens"] = _cr or None
                    sub_attrs["cache_creation_tokens"] = _cc or None
                    sub_attrs["total_tokens"] = (_in + _out) or None
                    sub_attrs["model_calls"] = _ss.model_calls or None
                if self.cost_estimation:
                    from okg.substrate.library.sources._agent_sessions_cost import (
                        compute_cost_usd,
                    )

                    _cost, _ver = compute_cost_usd(
                        _ss.model or scan.model,
                        _in,
                        _out,
                        _cr,
                        _cc,
                        self._cost_table,
                    )
                    if _cost is not None:
                        sub_attrs["cost_usd_estimate"] = _cost
                        sub_attrs["cost_table_version"] = _ver
            sub_attrs = {k: v for k, v in sub_attrs.items() if v is not None}
            nodes.append(
                NodeSpec(
                    node_id=sub_node_id,
                    subtype="sub_agent_session",
                    label=meta.get("description", sub_id)[:80],
                    attrs=sub_attrs,
                    source=self.ownership_id,
                )
            )
            edges.append(
                EdgeSpec(src=session_node_id, dst=sub_node_id, edge_type="contains")
            )
            edges.append(
                EdgeSpec(src=sub_node_id, dst=session_node_id, edge_type="member_of")
            )

            # Scan the subagent's own transcript for skills + file edits.
            # Subagent inherits the parent's cwd → repo mapping.
            if sub_transcript.is_file() and sub_transcript.stat().st_size > 0:
                self._materialize_subagent_activity(
                    sub_transcript=sub_transcript,
                    sub_node_id=sub_node_id,
                    sub_scan=_ss,
                    parent_cwd=scan.cwd,
                    nodes=nodes,
                    edges=edges,
                    aliases=aliases,
                    skill_emitted=skill_emitted,
                    source_file_emitted=source_file_emitted,
                )

        if self.emit_proposal_slug_refs:
            # Cross-module REFERENCES → openspec change slugs. The
            # okg-workspace deployment disables these direct edges and
            # relies on deterministic linkers once OpenSpec/PACT nodes
            # are present, so source admission does not depend on ingest
            # ordering for legacy change ids.
            chapter_texts = []
            for ch in scan.chapters:
                chapter_texts.append(ch.title or "")
                if ch.summary:
                    chapter_texts.append(ch.summary)
            for slug in extract_proposal_slugs(scan.git_branch, chapter_texts):
                change_id = f"change:{slug}"
                edges.append(
                    EdgeSpec(
                        src=session_node_id,
                        dst=change_id,
                        edge_type="references",
                        attrs={"match_method": "branch_or_chapter_slug"},
                    )
                )

        # Cross-module REFERENCES → compute_env RemoteHost (when loaded).
        # Dedupe hostnames per session (multiple ssh invocations to the
        # same host produce one edge with usernames aggregated as attrs).
        if self.emit_ssh_host_refs and scan.ssh_hosts:
            hosts_seen: dict[str, set[str]] = {}
            for ssh in scan.ssh_hosts:
                hosts_seen.setdefault(ssh.hostname, set()).add(ssh.username)
            for hostname, usernames in hosts_seen.items():
                edges.append(
                    EdgeSpec(
                        src=session_node_id,
                        dst=f"host:{hostname}",
                        edge_type="references",
                        attrs={
                            "reason": "ssh_command",
                            "observed_usernames": sorted(usernames),
                        },
                    )
                )

        # Cross-module AFFECTS → SourceFile
        # File paths in tool calls may be absolute (e.g., /Users/.../src/foo.py)
        # or repo-relative (src/foo.py). Normalize to repo-relative so the
        # file:<repo>:<relpath> ID matches what git_graph emits.
        repo = resolve_repo(scan.cwd or "", self.cwd_to_repo)
        if repo and scan.file_edits and scan.cwd:
            cwd_prefix = scan.cwd.rstrip("/") + "/"
            seen_files: set[str] = set()
            for fe in scan.file_edits:
                relpath = fe.file_path
                if relpath.startswith(cwd_prefix):
                    relpath = relpath[len(cwd_prefix) :]
                elif relpath.startswith("/"):
                    # Absolute path that doesn't live under this session's cwd —
                    # likely a different repo or unrelated file; skip the AFFECTS
                    # edge rather than emit a misleading one
                    continue
                fid = f"file:{repo}:{relpath}"
                if fid in seen_files:
                    continue
                seen_files.add(fid)
                _append_source_file_node(
                    nodes,
                    emitted=source_file_emitted,
                    repo=repo,
                    relpath=relpath,
                )
                edges.append(
                    EdgeSpec(
                        src=session_node_id,
                        dst=fid,
                        edge_type="affects",
                        attrs={"tool": fe.tool_name},
                    )
                )

    def _materialize_subagent_activity(
        self,
        *,
        sub_transcript: Path,
        sub_node_id: str,
        sub_scan: SessionScan | None,
        parent_cwd: str | None,
        nodes: list[NodeSpec],
        edges: list[EdgeSpec],
        aliases: dict[str, str],
        skill_emitted: set[str],
        source_file_emitted: set[str],
    ) -> None:
        """Re-use the same scan_transcript on a subagent's JSONL.

        Subagent transcripts share the same event shapes as parents, so
        we can extract skills and file edits identically. We emit edges
        from the SubAgentSession node (not the parent), so subagent
        activity is properly attributed.
        """
        if sub_scan is None:
            sub_scan = scan_transcript(sub_transcript)

        self._materialize_messages(
            owner_node_id=sub_node_id,
            message_owner_id=sub_node_id.removeprefix("subsession:"),
            session_id=sub_node_id.removeprefix("subsession:"),
            scan=sub_scan,
            nodes=nodes,
            edges=edges,
            include_chapter_edges=False,
        )

        # Skills invoked by the subagent
        for skill in sub_scan.skills:
            skill_node_id = f"skill:{skill.name}"
            if skill_node_id not in skill_emitted:
                attrs: dict[str, Any] = {"name": skill.name}
                if skill.plugin:
                    attrs["plugin"] = skill.plugin
                nodes.append(
                    NodeSpec(
                        node_id=skill_node_id,
                        subtype="skill",
                        label=skill.name,
                        attrs=attrs,
                        source=self.ownership_id,
                    )
                )
                skill_emitted.add(skill_node_id)
                aliases[skill.name] = skill_node_id
            edges.append(
                EdgeSpec(src=sub_node_id, dst=skill_node_id, edge_type="references")
            )

        # File edits by the subagent — AFFECTS to SourceFile
        repo = resolve_repo(parent_cwd or "", self.cwd_to_repo)
        if repo and sub_scan.file_edits and parent_cwd:
            cwd_prefix = parent_cwd.rstrip("/") + "/"
            seen_files: set[str] = set()
            for fe in sub_scan.file_edits:
                relpath = fe.file_path
                if relpath.startswith(cwd_prefix):
                    relpath = relpath[len(cwd_prefix) :]
                elif relpath.startswith("/"):
                    continue  # absolute path outside parent cwd
                fid = f"file:{repo}:{relpath}"
                if fid in seen_files:
                    continue
                seen_files.add(fid)
                _append_source_file_node(
                    nodes,
                    emitted=source_file_emitted,
                    repo=repo,
                    relpath=relpath,
                )
                edges.append(
                    EdgeSpec(
                        src=sub_node_id,
                        dst=fid,
                        edge_type="affects",
                        attrs={"tool": fe.tool_name},
                    )
                )

    def _materialize_messages(
        self,
        *,
        owner_node_id: str,
        message_owner_id: str,
        session_id: str,
        scan: SessionScan,
        nodes: list[NodeSpec],
        edges: list[EdgeSpec],
        include_chapter_edges: bool,
    ) -> None:
        for message in scan.messages:
            message_id = f"message:{message_owner_id}:{message.message_index}"
            message_attrs: dict[str, Any] = {
                "message_id": message_id,
                "session_id": session_id,
                "harness": scan.harness,
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
            for k in ("parent_message_id", "chapter_index"):
                v = getattr(message, k)
                if v is not None:
                    message_attrs[k] = v
            message_attrs.update(
                untrusted_web_tool_result_attrs(
                    message.untrusted_content_tool_names,
                )
            )
            nodes.append(
                NodeSpec(
                    node_id=message_id,
                    subtype="agent_message",
                    label=message.content_excerpt[:80] or message.role,
                    attrs=message_attrs,
                    source=self.ownership_id,
                )
            )
            edges.append(
                EdgeSpec(
                    src=owner_node_id,
                    dst=message_id,
                    edge_type="contains",
                )
            )
            if (
                include_chapter_edges
                and message.chapter_index is not None
                and message.chapter_index < scan.chapter_count
            ):
                edges.append(
                    EdgeSpec(
                        src=f"chapter:{session_id}:{message.chapter_index}",
                        dst=message_id,
                        edge_type="contains",
                    )
                )

    # --- Helpers -------------------------------------------------------------

    def _iter_session_files(self) -> Iterator[Path]:
        """Yield per-cwd JSONL files, applying since-filter on cold start."""
        if not self.claude_projects_root.is_dir():
            return
        since_dt = parse_since(self.since)

        for cwd_dir in self.claude_projects_root.iterdir():
            if not cwd_dir.is_dir():
                continue
            if not is_claude_project_dir_candidate(
                cwd_dir,
                projects_root=self.claude_projects_root,
                cwd_filter=self.cwd_filter or (),
                cwd_globs=self.cwd_globs or (),
                claude_worktree_roots=self.claude_worktree_roots,
            ):
                continue
            for jsonl in cwd_dir.glob("*.jsonl"):
                if jsonl.is_symlink() or not jsonl.is_file():
                    continue
                known = (
                    self._state_key_for_path(jsonl) in self.state
                    or jsonl.stem in self.state
                )
                if since_dt is not None and (not self.state or not known):
                    try:
                        mtime = datetime.fromtimestamp(
                            jsonl.stat().st_mtime, tz=timezone.utc
                        )
                    except OSError:
                        continue
                    if mtime < since_dt:
                        continue
                yield jsonl

    def _state_key_for_path(self, jsonl_path: Path) -> str:
        try:
            root = self.claude_projects_root.resolve()
            return jsonl_path.resolve().relative_to(root).as_posix()
        except (OSError, ValueError):
            return str(jsonl_path.resolve())

    def _display_path(self, path: Path) -> Path:
        if self.display_projects_root is None:
            return path
        try:
            relative = path.resolve().relative_to(
                self.claude_projects_root.resolve()
            )
        except (OSError, ValueError):
            return path
        return self.display_projects_root / relative

    def _filter_signature(self) -> str:
        return json.dumps(
            {
                "cwd_filter": [str(path) for path in self.cwd_filter or []],
                "cwd_globs": list(self.cwd_globs or []),
                "claude_worktree_roots": [
                    str(path) for path in self.claude_worktree_roots
                ],
            },
            sort_keys=True,
        )

    def _iter_subagents(
        self, parent_jsonl: Path, parent_session_id: str
    ) -> Iterator[tuple[str, dict]]:
        """Yield (subagent_id, meta_dict_with___transcript_path) pairs."""
        session_dir = parent_jsonl.parent / parent_session_id
        sub_dir = session_dir / "subagents"
        if (
            session_dir.is_symlink()
            or sub_dir.is_symlink()
            or not sub_dir.is_dir()
        ):
            return
        for meta_path in sub_dir.glob("agent-*.meta.json"):
            if meta_path.is_symlink():
                continue
            sub_id = meta_path.stem.removesuffix(
                ".meta"
            )  # "agent-abc.meta" → "agent-abc"
            transcript_path = meta_path.with_suffix("").with_suffix(".jsonl")
            try:
                meta = json.loads(meta_path.read_text())
            except (OSError, json.JSONDecodeError) as e:
                logger.warning("Skipping subagent meta %s: %s", meta_path, e)
                continue
            meta["__transcript_path"] = transcript_path
            yield sub_id, meta

    def _is_idle(self, last_ingested_at: str) -> bool:
        if not last_ingested_at:
            return False
        try:
            last = datetime.fromisoformat(last_ingested_at.replace("Z", "+00:00"))
        except ValueError:
            return False
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        delta_h = (datetime.now(timezone.utc) - last).total_seconds() / 3600.0
        return delta_h > self.idle_threshold_hours


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _append_reject_reason(
    state: SessionScanState | None,
    *,
    path: Path,
    size: int,
) -> str:
    if state is None:
        return "missing_cursor"
    if not state.append_safe:
        return "prior_not_append_safe"
    if size <= state.bytes:
        return "not_grown"
    if not state.last_line_hash:
        return "missing_last_line_hash"
    if not prior_last_line_hash_matches(path, state):
        return "last_line_hash_mismatch"
    return "unknown"


def _append_source_file_node(
    nodes: list[NodeSpec],
    *,
    emitted: set[str],
    repo: str,
    relpath: str,
) -> str:
    node_id = f"file:{repo}:{relpath}"
    if node_id not in emitted:
        emitted.add(node_id)
        nodes.append(
            NodeSpec(
                node_id=node_id,
                subtype="source_file",
                label=relpath,
                attrs={
                    "path": relpath,
                    "repo": repo,
                    "source_provider": "agent_sessions",
                },
                source="agent_sessions",
            )
        )
    return node_id


def _cwd_matches(
    cwd: str | None,
    filter_paths: list,
    glob_patterns: list[str] | None = None,
    claude_worktree_roots: tuple[Path | str, ...] = (),
) -> bool:
    """True if cwd equals or sits under any exact or globbed root."""
    if cwd is None:
        return False
    cwd_norm = normalize_claude_path(cwd)
    if is_repo_local_claude_worktree_cwd(cwd_norm):
        return is_allowed_claude_worktree_cwd(
            cwd_norm,
            claude_worktree_roots,
        )
    for p in filter_paths:
        ps = normalize_claude_path(p)
        if cwd_norm == ps or cwd_norm.startswith(ps + "/"):
            return True
    for pattern in glob_patterns or []:
        ps = normalize_claude_path(pattern)
        if fnmatch.fnmatch(cwd_norm, ps) or fnmatch.fnmatch(cwd_norm, ps + "/*"):
            return True
    return False
