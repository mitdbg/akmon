"""RemoteHostsFromSessions — extract RemoteHost nodes from agent transcripts.

Scans `~/.claude/projects/*/jsonl` files for `ssh user@host` patterns
in `Bash` tool calls. Emits one `RemoteHost` node per distinct
hostname (deduped, with observed usernames as a list attribute) plus
a `REFERENCES` edge from the relevant `AgentSession` to that
`RemoteHost`.

This source scans the same files as `agent_sessions.AgentSessionSource`
but for a different purpose. The duplicate scan is acknowledged as a
known cost; the alternative (sharing parsed data between sources)
would couple modules. JSONL files are small.

Cross-module: the `REFERENCES` edges target `session:<sessionId>`
IDs which only resolve when `agent_sessions` is also loaded.
Otherwise the edges are deferred (per the orchestrator's standard
behavior).

Incremental: stores per-jsonl byte length and cached observations in state.
The substrate adapter guards reuse of that cache with a complete-content
manifest because external transcript writers do not guarantee append-only
mutation. Emission is a function of transcript CONTENT only -- no wall-clock
input (issue #323).
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from okg.substrate.library.sources._claude_project_scope import (
    is_repo_local_claude_worktree_cwd,
    is_repo_local_claude_worktree_project_dir,
)
from okg.substrate.library.sources.source_specs import EdgeSpec, NodeSpec

logger = logging.getLogger(__name__)


# Match `ssh [flags...] user@host`. Intermediate tokens MUST be
# flag-shaped (start with `-`), optionally followed by a non-flag
# value (e.g., `-i key.pem`, `-p 2222`). This prevents over-matching
# across English prose like "use ssh keys with user@example.com".
# `ssh-keygen` / `ssh-add` don't match because we require `\bssh\s`
# (whitespace, not hyphen, after `ssh`).
_SSH_PATTERN = re.compile(
    r"\bssh\s+"
    r"(?:-\S+(?:\s+[^\s\-]\S*)?\s+){0,4}"
    r"([a-zA-Z][\w_-]*)@([a-zA-Z][\w.-]+)",
)


@dataclass
class RemoteHostsFromSessions:
    """Mine ssh patterns from agent transcripts; emit RemoteHost nodes.

    Parameters
    ----------
    claude_projects_root : Path
        Where Claude Code stores per-cwd session directories. Default
        ``~/.claude/projects``.
    cwd_filter : list[Path] | None
        If set, only sessions whose `cwd` matches one of these paths
        are scanned. Optional — defaults to scanning every cwd dir
        under `claude_projects_root`.
    idle_threshold_hours : int
        Sessions whose JSONL size hasn't grown AND were last scanned
        more than this many hours ago are skipped. Default 24.
    """

    claude_projects_root: Path = field(
        default_factory=lambda: Path("~/.claude/projects").expanduser()
    )
    cwd_filter: list[Path] | None = None
    idle_threshold_hours: int = 24
    session_files: list[Path] | None = None

    state: dict[str, Any] = field(default_factory=dict)
    _emit_nodes: list[NodeSpec] | None = None
    _emit_edges: list[EdgeSpec] | None = None
    _emit_aliases: dict[str, str] = field(default_factory=dict)

    ownership_id: str = "compute_env.remote_hosts"
    supports_incremental: bool = True
    run_state_kind: str = (
        "metadata"  # bind_run_state passes us state.metadata as a dict
    )

    def __post_init__(self) -> None:
        if isinstance(self.claude_projects_root, str):
            self.claude_projects_root = Path(self.claude_projects_root).expanduser()
        if self.cwd_filter:
            self.cwd_filter = [
                Path(p).expanduser() if isinstance(p, str) else p
                for p in self.cwd_filter
            ]
        if self.session_files:
            self.session_files = [
                Path(p).expanduser().resolve() for p in self.session_files
            ]

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
        return []

    # --- Implementation -----------------------------------------------------

    def _compute_emit(self) -> None:
        # Aggregate observations across all sessions
        # hostname → {usernames: set, sessions: set, first_ts, last_ts}
        observations: dict[str, dict[str, Any]] = {}

        per_session_state: dict[str, dict[str, Any]] = (
            self.state.get("per_session", {}) or {}
        )
        # Rebuild membership from the current enumeration. Starting from the
        # prior mapping retained deleted sessions forever and let a deletion-
        # only run take the stale "nothing changed" shortcut.
        new_per_session: dict[str, dict[str, Any]] = {}
        any_change = False

        for jsonl_path in self._iter_session_files():
            sid = jsonl_path.stem
            prior = per_session_state.get(sid)
            try:
                cur_size = jsonl_path.stat().st_size
            except OSError:
                # Unknown is not deletion. Preserve the prior entry so a
                # transient stat failure cannot silently erase cached scope.
                if prior is not None:
                    new_per_session[sid] = prior
                continue
            if prior and cur_size == prior.get("size"):
                # Size unchanged — carry forward prior observations without
                # re-scanning. No change signal.
                for hostname, hits in (prior.get("hits") or {}).items():
                    agg = observations.setdefault(hostname, _empty_obs(hostname))
                    agg["usernames"].update(hits.get("usernames", []))
                    agg["sessions"].add(sid)
                    agg["first_ts"] = _min_iso(agg["first_ts"], hits.get("ts"))
                    agg["last_ts"] = _max_iso(agg["last_ts"], hits.get("ts"))
                new_per_session[sid] = prior
                continue
            # New or grown file — must re-scan
            any_change = True

            # Scan (re-scan) this transcript
            session_hits, session_id_seen = self._scan_jsonl(jsonl_path)
            session_id = session_id_seen or sid
            for hostname, hits in session_hits.items():
                agg = observations.setdefault(hostname, _empty_obs(hostname))
                agg["usernames"].update(hits["usernames"])
                agg["sessions"].add(session_id)
                agg["first_ts"] = _min_iso(agg["first_ts"], hits["ts"])
                agg["last_ts"] = _max_iso(agg["last_ts"], hits["ts"])
            new_per_session[sid] = {
                "size": cur_size,
                "last_scanned": _now_iso(),
                "hits": {
                    h: {
                        "usernames": sorted(v["usernames"]),
                        "ts": v["ts"],
                    }
                    for h, v in session_hits.items()
                },
            }

        if set(new_per_session) != set(per_session_state):
            any_change = True

        nodes: list[NodeSpec] = []
        edges: list[EdgeSpec] = []
        aliases: dict[str, str] = {}

        # If no session file changed since the prior run, skip emission.
        # The aggregate is identical to what we already emitted; re-emitting
        # would force the backbone to upsert unchanged nodes.
        if per_session_state and not any_change:
            self._emit_nodes = nodes
            self._emit_edges = edges
            self._emit_aliases = aliases
            self.state = {"per_session": new_per_session}
            return

        for hostname, obs in sorted(observations.items()):
            host_id = f"host:{hostname}"
            attrs: dict[str, Any] = {
                "hostname": hostname,
                "observed_usernames": sorted(obs["usernames"]),
            }
            if obs["first_ts"]:
                attrs["first_observed_at"] = obs["first_ts"]
            if obs["last_ts"]:
                attrs["last_observed_at"] = obs["last_ts"]
            nodes.append(
                NodeSpec(
                    node_id=host_id,
                    subtype="remote_host",
                    label=hostname,
                    attrs=attrs,
                    source=self.ownership_id,
                )
            )
            aliases[hostname] = host_id
            for sid in sorted(obs["sessions"]):
                edges.append(
                    EdgeSpec(
                        src=f"session:{sid}",
                        dst=host_id,
                        edge_type="references",
                        attrs={"reason": "ssh_command"},
                    )
                )

        self._emit_nodes = nodes
        self._emit_edges = edges
        self._emit_aliases = aliases
        self.state = {"per_session": new_per_session}

    def _iter_session_files(self):
        if self.session_files is not None:
            for jsonl in self.session_files:
                if jsonl.is_file() and jsonl.suffix.lower() == ".jsonl":
                    yield jsonl
            return
        if not self.claude_projects_root.is_dir():
            return
        cwd_filter_strs = {str(p) for p in self.cwd_filter} if self.cwd_filter else None
        for cwd_dir in self.claude_projects_root.iterdir():
            if not cwd_dir.is_dir():
                continue
            if is_repo_local_claude_worktree_project_dir(cwd_dir):
                continue
            for jsonl in cwd_dir.glob("*.jsonl"):
                # We don't know the session's cwd until we open the file.
                # If cwd_filter is set, defer the check — we'll re-filter
                # inside _scan_jsonl. Yielding all and filtering later is
                # simpler than re-implementing path matching here.
                _ = cwd_filter_strs  # may be applied during scan
                yield jsonl

    def _scan_jsonl(
        self, jsonl_path: Path
    ) -> tuple[dict[str, dict[str, Any]], str | None]:
        """Stream-read a JSONL; return (host_observations, session_id_or_None).

        host_observations is `{hostname: {usernames: set, ts: iso8601 (last seen)}}`.
        """
        out: dict[str, dict[str, Any]] = {}
        session_id: str | None = None
        cwd: str | None = None
        try:
            fh = open(jsonl_path)
        except OSError:
            return out, session_id
        with fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    ev = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if session_id is None and "sessionId" in ev:
                    session_id = ev["sessionId"]
                if cwd is None and "cwd" in ev:
                    cwd = ev["cwd"]
                if ev.get("type") != "assistant":
                    continue
                msg = ev.get("message") or {}
                content = msg.get("content")
                if not isinstance(content, list):
                    continue
                ts = ev.get("timestamp")
                for block in content:
                    if not isinstance(block, dict) or block.get("type") != "tool_use":
                        continue
                    if block.get("name") != "Bash":
                        continue
                    inp = block.get("input") or {}
                    cmd = inp.get("command", "")
                    if not isinstance(cmd, str):
                        continue
                    for m in _SSH_PATTERN.finditer(cmd):
                        username, host = m.group(1), m.group(2)
                        bucket = out.setdefault(host, {"usernames": set(), "ts": ts})
                        bucket["usernames"].add(username)
                        bucket["ts"] = _max_iso(bucket["ts"], ts)

        # Apply cwd filter if set. Prefix match so a filter of
        # `/path/to/repo` also captures sessions whose cwd is a worktree
        # under that repo.
        if is_repo_local_claude_worktree_cwd(cwd):
            return {}, session_id
        if self.cwd_filter and cwd is not None:
            matched = False
            for p in self.cwd_filter:
                ps = str(p).rstrip("/")
                if cwd == ps or cwd.startswith(ps + "/"):
                    matched = True
                    break
            if not matched:
                return {}, session_id
        return out, session_id


def _empty_obs(hostname: str) -> dict[str, Any]:
    return {
        "hostname": hostname,
        "usernames": set(),
        "sessions": set(),
        "first_ts": None,
        "last_ts": None,
    }


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _min_iso(a: str | None, b: str | None) -> str | None:
    candidates = [x for x in (a, b) if x]
    return min(candidates) if candidates else None


def _max_iso(a: str | None, b: str | None) -> str | None:
    candidates = [x for x in (a, b) if x]
    return max(candidates) if candidates else None
