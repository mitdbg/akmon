"""Exact Git/Chronos workspace pairs for durable agent assignments.

The coordinator owns two resources for a writer assignment: a local Git
worktree and a private Chronos branch.  This module is deliberately an
orchestration boundary, not a new persistence system.  Assignment authority
is recorded in the Chronos branch metadata and in the immutable receipts
returned by this module.

The protocols make failure injection possible without production failpoints.
``GitWorkspaceAdapter`` and ``GraphWorkspaceAdapter`` are intentionally small
and have production implementations at the bottom of this module.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import uuid
from collections.abc import Iterator
from contextlib import contextmanager, nullcontext
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal, Mapping, Protocol, cast

from chronos_core.branching import BranchNotFoundError
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.graph_store_internal.common import quote_physical_table
from okg.substrate.library.agent_runtimes.ssh_stdio import (
    restricted_ssh_stdio_mcp_spec,
    validate_coordination_route,
)
from okg.substrate.library.sources._git_helpers import (
    git_ignored_paths,
    in_proc_ref_tips,
    in_proc_workdir,
    run_git,
)
from okg.substrate.library.sources.git_scope_probe import GitScopeProbe


LifecycleStatus = Literal[
    "staged",
    "active",
    "checkpointed",
    "handed_off",
    "retired",
    "cleaning",
    "closed",
    "abandoned",
]
CleanupDisposition = Literal[
    "pending",
    "retained",
    "abandoned",
    "partial",
    "complete",
]

# A pair keeps a bounded checkpoint lineage.  The newest handoff checkpoint
# may be retained as an audit anchor; older owned checkpoints are released at
# cleanup once no descendant can still fork from them.
MAX_CHECKPOINT_HISTORY = 8

# This is deliberately separate from a user's runtime configuration.  The
# lifecycle owns this one assignment-local file and may safely replace it;
# the repository root's ``.mcp.json`` is user/source content and is never a
# lifecycle target.
DEFAULT_COORDINATION_CONFIG = Path(".okg/coordination/mcp.json")


class PairedWorkspaceError(RuntimeError):
    """Base error for a refused or incomplete pair transition."""


class PairMismatchError(PairedWorkspaceError):
    """The receipt and the read-back resources do not describe one pair."""


class PairStaleError(PairMismatchError):
    """The receipt is no longer current and cannot grant a write transition."""


class PairProvisionError(PairedWorkspaceError):
    """Provisioning failed; the saga has attempted to roll back its resources."""


class PairCheckpointError(PairedWorkspaceError):
    """A Chronos checkpoint exists but its binding metadata was not committed."""

    def __init__(
        self,
        message: str,
        *,
        checkpoint: str,
        checkpoint_ref: str,
        partial: bool = True,
    ) -> None:
        super().__init__(message)
        self.checkpoint = checkpoint
        self.checkpoint_ref = checkpoint_ref
        # ``partial`` is true for adapters whose checkpoint call committed
        # before binding failed.  The production interval adapter performs
        # both operations in one transaction and reports false after rollback.
        self.partial = partial


def _is_graph_not_found(exc: BaseException) -> bool:
    """Recognize only the bounded absence signals used by graph adapters."""

    return isinstance(exc, (KeyError, BranchNotFoundError)) or type(exc).__name__ in {
        "BranchNotFoundError",
        "NotFoundError",
    }


@dataclass(frozen=True)
class GitWorkspaceState:
    """Content identity and cleanliness of one Git worktree."""

    repo_path: str
    worktree_path: str
    branch: str
    base_commit: str
    head_commit: str
    tree: str
    dirty: bool
    dirty_paths: tuple[str, ...] = ()

    @property
    def head_sha(self) -> str:
        return self.head_commit

    @property
    def tree_sha(self) -> str:
        return self.tree

    @property
    def base_sha(self) -> str:
        return self.base_commit

    @property
    def dirty_state(self) -> bool:
        return self.dirty


@dataclass(frozen=True)
class GraphWorkspaceState:
    """Identity of a private Chronos branch and its latest checkpoint."""

    deployment: str
    base_branch: str
    base_generation: str | None
    branch: str
    current_ref: str
    checkpoint: str | None = None
    checkpoint_ref: str | None = None
    checkpoint_ids: tuple[str, ...] = ()
    assignment_id: str | None = None
    assignment_generation: int | None = None
    authenticated_host: str | None = None
    lifecycle_status: LifecycleStatus = "staged"
    predecessor_digest: str | None = None
    cleanup_disposition: CleanupDisposition = "pending"
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))
        object.__setattr__(
            self,
            "checkpoint_ids",
            tuple(str(item) for item in self.checkpoint_ids if str(item)),
        )

    @property
    def checkpoint_id(self) -> str | None:
        return self.checkpoint

    @property
    def base_generation_id(self) -> str | None:
        return self.base_generation


@dataclass(frozen=True)
class PairedWorkspaceRequest:
    """Input for provisioning one assignment-bound pair.

    ``assignment_generation`` is a fencing token and must be positive.  The
    branch names must be explicit and unique for replacement; callers may use
    :func:`default_workspace_branch` to derive one safely.
    """

    assignment_id: str
    assignment_generation: int
    authenticated_host: str
    repo_path: str | Path
    worktree_path: str | Path
    git_branch: str
    graph_deployment: str
    graph_base_branch: str = "default"
    graph_branch: str | None = None
    git_base_commit: str | None = None
    graph_base_generation: str | None = None
    graph_base_checkpoint: str | None = None
    predecessor_digest: str | None = None
    # Ordinary lifecycle callers may omit this and receive a locally generated
    # nonce.  Authenticated assignment start supplies the server-owned attempt
    # id so a restart can adopt only the pair reserved for that exact attempt.
    provision_attempt_id: str | None = None
    grant_id: str | None = None
    mcp_route: str | None = None
    mcp_command: str = "okg"
    mcp_server_name: str = "okg-coordination"
    config_path: str | Path | None = None

    def __post_init__(self) -> None:
        _validate_assignment(self.assignment_id, self.assignment_generation)
        _validate_host(self.authenticated_host)
        _validate_git_branch(self.git_branch)
        if not self.grant_id or any(char.isspace() for char in self.grant_id):
            raise ValueError("grant_id must be explicitly supplied")
        if self.provision_attempt_id is not None and (
            not self.provision_attempt_id.strip()
            or self.provision_attempt_id.startswith("-")
            or any(char.isspace() for char in self.provision_attempt_id)
        ):
            raise ValueError("provision_attempt_id must be one non-option token")
        if self.mcp_command != "okg":
            raise ValueError("mcp_command must be the trusted okg client bridge")

    @property
    def branch(self) -> str:
        return self.git_branch

    @property
    def deployment(self) -> str:
        return self.graph_deployment

    @property
    def base_branch(self) -> str:
        return self.graph_base_branch

    @property
    def resolved_graph_branch(self) -> str:
        return self.graph_branch or self.git_branch


@dataclass(frozen=True)
class PairedWorkspaceReceipt:
    """Immutable proof of the exact pair returned by every transition."""

    assignment_id: str
    assignment_generation: int
    authenticated_host: str
    repo_path: str
    worktree_path: str
    git: GitWorkspaceState
    graph: GraphWorkspaceState
    lifecycle_status: LifecycleStatus
    predecessor_digest: str | None = None
    cleanup_disposition: CleanupDisposition = "pending"
    config_path: str = ""
    mcp_server_name: str = "okg-coordination"
    mcp_route: str = ""
    mcp_command: str = "okg"
    grant_id: str = ""

    def __post_init__(self) -> None:
        _validate_assignment(self.assignment_id, self.assignment_generation)
        _validate_host(self.authenticated_host)
        _validate_git_branch(self.git.branch)
        _validate_route(self.mcp_route)
        if (
            not self.mcp_command
            or self.mcp_command.startswith("-")
            or any(char.isspace() for char in self.mcp_command)
        ):
            raise ValueError("mcp_command must be one explicit non-option token")
        if not self.grant_id or any(char.isspace() for char in self.grant_id):
            raise ValueError("grant_id must be one non-empty token")

    @property
    def git_branch(self) -> str:
        return self.git.branch

    @property
    def git_base_commit(self) -> str:
        return self.git.base_commit

    @property
    def git_head_commit(self) -> str:
        return self.git.head_commit

    @property
    def git_tree(self) -> str:
        return self.git.tree

    @property
    def git_dirty(self) -> bool:
        return self.git.dirty

    @property
    def graph_deployment(self) -> str:
        return self.graph.deployment

    @property
    def graph_base_branch(self) -> str:
        return self.graph.base_branch

    @property
    def graph_base_generation(self) -> str | None:
        return self.graph.base_generation

    @property
    def graph_branch(self) -> str:
        return self.graph.branch

    @property
    def graph_current_ref(self) -> str:
        return self.graph.current_ref

    @property
    def graph_checkpoint(self) -> str | None:
        return self.graph.checkpoint

    @property
    def status(self) -> LifecycleStatus:
        return self.lifecycle_status

    @property
    def digest(self) -> str:
        return receipt_digest(self)


@dataclass(frozen=True)
class WorkspaceHandoff:
    """A clean, checkpoint-aligned receipt safe to pass to a successor."""

    handoff_id: str
    receipt: PairedWorkspaceReceipt
    predecessor_digest: str
    lifecycle_status: Literal["handed_off"] = "handed_off"

    @property
    def assignment_id(self) -> str:
        return self.receipt.assignment_id

    @property
    def assignment_generation(self) -> int:
        return self.receipt.assignment_generation

    @property
    def authenticated_host(self) -> str:
        return self.receipt.authenticated_host

    @property
    def git(self) -> GitWorkspaceState:
        return self.receipt.git

    @property
    def graph(self) -> GraphWorkspaceState:
        return self.receipt.graph


@dataclass(frozen=True)
class CleanupReport:
    """Read-back result of an idempotent cleanup attempt."""

    assignment_id: str
    assignment_generation: int
    git_removed: bool
    graph_removed: bool
    config_removed: bool
    complete: bool
    disposition: CleanupDisposition
    errors: tuple[str, ...] = ()
    retained_checkpoint_ids: tuple[str, ...] = ()
    compacted_checkpoint_ids: tuple[str, ...] = ()

    @property
    def closed(self) -> bool:
        return self.complete


class GitWorkspaceAdapter(Protocol):
    """Git operations required by :class:`PairedWorkspaceLifecycle`."""

    def inspect(self, path: str | Path) -> GitWorkspaceState: ...

    def branch_exists(self, repository: str | Path, branch: str) -> bool: ...

    def resolve_commit(self, repository: str | Path, commit_or_ref: str) -> str: ...

    def create_worktree(
        self,
        repository: str | Path,
        worktree: str | Path,
        branch: str,
        base_commit: str,
    ) -> GitWorkspaceState: ...

    def remove_worktree(self, repository: str | Path, worktree: str | Path) -> None: ...

    def remove_worktree_and_branch_guarded(
        self,
        repository: str | Path,
        worktree: str | Path,
        branch: str,
        *,
        expected_commit: str,
    ) -> None: ...

    def is_ancestor(
        self, repository: str | Path, ancestor: str, descendant: str
    ) -> bool: ...


class GraphWorkspaceAdapter(Protocol):
    """Chronos operations required by :class:`PairedWorkspaceLifecycle`."""

    def provision_lock(self, deployment: str, branch: str) -> Any: ...

    def latest_generation(self, deployment: str, branch: str) -> str | None: ...

    def branch_exists(self, deployment: str, branch: str) -> bool: ...

    def inspect(self, deployment: str, branch: str) -> GraphWorkspaceState: ...

    def create_branch(
        self,
        deployment: str,
        branch: str,
        base_branch: str,
        base_generation: str | None,
        metadata: Mapping[str, Any],
    ) -> GraphWorkspaceState: ...

    def create_branch_from_checkpoint(
        self,
        deployment: str,
        branch: str,
        checkpoint: str,
        metadata: Mapping[str, Any],
    ) -> GraphWorkspaceState: ...

    def checkpoint(
        self,
        deployment: str,
        branch: str,
        metadata: Mapping[str, Any],
    ) -> GraphWorkspaceState: ...

    def update_metadata(
        self, deployment: str, branch: str, metadata: Mapping[str, Any]
    ) -> GraphWorkspaceState: ...

    def update_metadata_guarded(
        self,
        deployment: str,
        branch: str,
        metadata: Mapping[str, Any],
        *,
        expected: Mapping[str, Any],
    ) -> GraphWorkspaceState: ...

    def remove_branch(self, deployment: str, branch: str) -> None: ...

    def checkpoint_matches_current(
        self,
        deployment: str,
        branch: str,
        checkpoint: str,
        checkpoint_ref: str,
        current_ref: str,
    ) -> bool: ...

    def handoff_if_checkpoint_current(
        self,
        deployment: str,
        branch: str,
        checkpoint: str,
        checkpoint_ref: str,
        current_ref: str,
        metadata: Mapping[str, Any],
        *,
        expected: Mapping[str, Any],
    ) -> GraphWorkspaceState: ...

    def validate_checkpoint(
        self, deployment: str, predecessor_branch: str, checkpoint: str
    ) -> None: ...

    def descendants(self, deployment: str, branch: str) -> tuple[str, ...]: ...

    def compact_checkpoints(
        self,
        deployment: str,
        branch: str,
        checkpoint_ids: tuple[str, ...],
        retain_checkpoint_ids: tuple[str, ...],
    ) -> tuple[tuple[str, ...], tuple[str, ...]]: ...

    def remove_branch_guarded(self, deployment: str, branch: str) -> None: ...


def default_workspace_branch(assignment_id: str, assignment_generation: int) -> str:
    """Return a deterministic, ref-safe branch name for one generation."""
    _validate_assignment(assignment_id, assignment_generation)
    token = hashlib.sha256(str(assignment_id).encode("utf-8")).hexdigest()[:12]
    # ``:`` is not legal in a Git ref.  Keep the assignment and generation
    # visibly separated with ordinary ref components instead.
    return f"coord/{token}/g{assignment_generation}"


_SAFE_GIT_BRANCH = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]*$")


def _validate_git_branch(branch: str) -> None:
    """Reject ref spellings that Git cannot safely resolve as a branch."""
    if (
        not _SAFE_GIT_BRANCH.fullmatch(branch)
        or branch.endswith((".", "/"))
        or branch.endswith(".lock")
        or ".." in branch
        or "@{" in branch
        or "//" in branch
    ):
        raise ValueError("git_branch is not a safe Git branch name")


_FULL_COMMIT_SHA = re.compile(r"^[0-9a-fA-F]{40}(?:[0-9a-fA-F]{24})?$")


def _is_full_commit_sha(value: str) -> bool:
    return bool(_FULL_COMMIT_SHA.fullmatch(str(value)))


def _validate_commit_ref(value: str) -> None:
    if (
        not str(value).strip()
        or any(char.isspace() or char == "\x00" for char in str(value))
        or str(value).startswith("-")
    ):
        raise ValueError("git base must be a safe commit/ref token")


def receipt_digest(receipt: PairedWorkspaceReceipt) -> str:
    """Hash pair identity, state, and lifecycle for successor fencing."""
    payload = {
        "assignment_id": receipt.assignment_id,
        "assignment_generation": receipt.assignment_generation,
        "authenticated_host": receipt.authenticated_host,
        "repo_path": receipt.repo_path,
        "worktree_path": receipt.worktree_path,
        "git": asdict(receipt.git),
        "graph": {
            "deployment": receipt.graph.deployment,
            "base_branch": receipt.graph.base_branch,
            "base_generation": receipt.graph.base_generation,
            "branch": receipt.graph.branch,
            "current_ref": receipt.graph.current_ref,
            "checkpoint": receipt.graph.checkpoint,
            "checkpoint_ref": receipt.graph.checkpoint_ref,
            "checkpoint_ids": list(receipt.graph.checkpoint_ids),
            "assignment_id": receipt.graph.assignment_id,
            "assignment_generation": receipt.graph.assignment_generation,
            "authenticated_host": receipt.graph.authenticated_host,
            "lifecycle_status": receipt.graph.lifecycle_status,
            "predecessor_digest": receipt.graph.predecessor_digest,
            "cleanup_disposition": receipt.graph.cleanup_disposition,
            "provision_attempt_id": receipt.graph.metadata.get(
                "provision_attempt_id", ""
            ),
        },
        "lifecycle_status": receipt.lifecycle_status,
        "predecessor_digest": receipt.predecessor_digest,
        "cleanup_disposition": receipt.cleanup_disposition,
        "config_path": receipt.config_path,
        "mcp_server_name": receipt.mcp_server_name,
        "mcp_route": receipt.mcp_route,
        "mcp_command": receipt.mcp_command,
        "grant_id": receipt.grant_id,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode(
            "utf-8"
        )
    ).hexdigest()


def _validate_assignment(assignment_id: str, assignment_generation: int) -> None:
    if not str(assignment_id).strip() or any(
        char.isspace() for char in str(assignment_id)
    ):
        raise ValueError("assignment_id must be one non-empty token")
    if isinstance(assignment_generation, bool) or int(assignment_generation) <= 0:
        raise ValueError("assignment_generation must be positive")


def _validate_host(host: str) -> str:
    value = str(host).strip()
    if not value or any(char.isspace() for char in value) or value.startswith("-"):
        raise ValueError("authenticated_host must be one non-option token")
    return value


def _validate_route(route: str | None) -> str:
    try:
        return validate_coordination_route(route)
    except ValueError as exc:
        raise ValueError("mcp_route must be one explicit non-option token") from exc


def _resolved(path: str | Path) -> str:
    value = Path(path).expanduser().resolve()
    return str(value)


def _absolute_lexical(path: str | Path) -> Path:
    """Return an absolute path without following symlinks.

    ``Path.resolve()`` is intentionally not used for a config target.  A
    symlink from the assignment directory to a tracked source file would
    otherwise look like a safe path after resolution, just before the write
    follows it.  ``abspath`` normalises ``.``/``..`` while retaining the
    directory entries that the safety checks need to inspect.
    """
    return Path(os.path.abspath(os.path.expanduser(str(path))))


def _reject_symlinked_path_chain(path: str | Path, *, label: str) -> None:
    """Reject a target or existing parent that can redirect a lifecycle write."""
    current = _absolute_lexical(path)
    while True:
        if current.is_symlink():
            raise ValueError(f"{label} path must not traverse a symlink")
        parent = current.parent
        if parent == current:
            return
        current = parent


def _atomic_write_private(path: Path, content: str) -> None:
    """Atomically replace a lifecycle-owned file with mode ``0600``."""
    _reject_symlinked_path_chain(path, label="private config")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.parent / f".{path.name}.{uuid.uuid4().hex}.tmp"
    fd = os.open(
        tmp_path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        0o600,
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            fd = -1
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp_path, path)
        os.chmod(path, 0o600)
    except Exception:  # noqa: BLE001 - preserve the write failure
        if fd >= 0:
            os.close(fd)
        try:
            tmp_path.unlink()
        except OSError:
            pass
        raise


def _atomic_write_preserving_mode(path: Path, content: str) -> None:
    """Atomically write Git metadata without changing an existing mode."""
    mode = stat.S_IMODE(path.stat().st_mode) if path.exists() else 0o600
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.parent / f".{path.name}.{uuid.uuid4().hex}.tmp"
    fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            fd = -1
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp_path, path)
        os.chmod(path, mode)
    except Exception:  # noqa: BLE001 - preserve the write failure
        if fd >= 0:
            os.close(fd)
        try:
            tmp_path.unlink()
        except OSError:
            pass
        raise


def _metadata_for(
    request: PairedWorkspaceRequest,
    *,
    git: GitWorkspaceState,
    graph: GraphWorkspaceState,
    lifecycle_status: LifecycleStatus,
    predecessor_digest: str | None,
    cleanup_disposition: CleanupDisposition,
) -> dict[str, Any]:
    """Build the complete assignment binding stored in Chronos metadata."""
    return {
        "kind": "okg_paired_workspace",
        "assignment_id": request.assignment_id,
        "assignment_generation": request.assignment_generation,
        "authenticated_host": request.authenticated_host,
        "repo_path": git.repo_path,
        "worktree_path": git.worktree_path,
        "git_branch": git.branch,
        "git_base_commit": git.base_commit,
        "git_head_commit": git.head_commit,
        "git_tree": git.tree,
        "git_dirty": git.dirty,
        "git_dirty_paths": list(git.dirty_paths),
        "deployment": graph.deployment,
        "base_branch": graph.base_branch,
        "base_generation": graph.base_generation,
        "base_checkpoint": request.graph_base_checkpoint,
        "branch": graph.branch,
        "current_ref": graph.current_ref,
        "checkpoint": graph.checkpoint,
        "checkpoint_ref": graph.checkpoint_ref,
        "checkpoint_ids": list(graph.checkpoint_ids),
        "compacted_checkpoint_ids": list(
            graph.metadata.get("compacted_checkpoint_ids", ())
        ),
        "audit_checkpoint": bool(graph.metadata.get("audit_checkpoint", False)),
        "provision_attempt_id": graph.metadata.get("provision_attempt_id", ""),
        "provision_invocation_id": graph.metadata.get("provision_invocation_id", ""),
        "lifecycle_status": lifecycle_status,
        "predecessor_digest": predecessor_digest,
        "cleanup_disposition": cleanup_disposition,
        "config_path": _resolved(request.config_path) if request.config_path else "",
        "mcp_server_name": request.mcp_server_name,
        "mcp_route": request.mcp_route,
        "mcp_command": request.mcp_command,
        "grant_id": request.grant_id or "",
    }


def _checkpoint_history(
    graph: GraphWorkspaceState,
    checkpoint: str | None = None,
) -> tuple[str, ...]:
    """Return the owned checkpoint lineage without duplicate IDs."""
    values = list(dict.fromkeys(str(item) for item in graph.checkpoint_ids if item))
    for candidate in (graph.checkpoint, checkpoint):
        if candidate and candidate not in values:
            values.append(candidate)
    if len(values) > MAX_CHECKPOINT_HISTORY:
        raise PairMismatchError(
            "checkpoint retention bound reached; compact before checkpointing"
        )
    return tuple(values)


def _merge_mcp_config(
    text: str,
    *,
    server_name: str,
    spec: Mapping[str, Any],
) -> str:
    """Upsert one restricted server while retaining unrelated JSON content."""
    if not text.strip():
        payload: dict[str, Any] = {}
    else:
        try:
            decoded = json.loads(text)
        except ValueError as exc:
            raise ValueError("existing MCP config is not valid JSON") from exc
        if not isinstance(decoded, dict):
            raise ValueError("existing MCP config must be a JSON object")
        payload = dict(decoded)
    servers = payload.get("mcpServers", {})
    if not isinstance(servers, Mapping):
        raise ValueError("existing MCP config mcpServers must be an object")
    merged_servers = dict(servers)
    merged_servers[server_name] = dict(spec)
    payload["mcpServers"] = merged_servers
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def _validate_managed_config(receipt: PairedWorkspaceReceipt) -> None:
    """Ensure the selected server remains a credential-free restricted route."""
    config_path = Path(receipt.config_path)
    if config_path.is_symlink():
        raise PairMismatchError("paired MCP config must not be a symlink")
    if not config_path.is_file():
        raise PairMismatchError("paired MCP config is missing")
    try:
        if stat.S_IMODE(config_path.stat().st_mode) != 0o600:
            raise PairMismatchError("paired MCP config must have private mode 0600")
    except OSError as exc:
        raise PairMismatchError("paired MCP config permissions are unreadable") from exc
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise PairMismatchError("paired MCP config is unreadable or invalid") from exc
    if not isinstance(payload, Mapping):
        raise PairMismatchError("paired MCP config must be an object")
    servers = payload.get("mcpServers")
    if not isinstance(servers, Mapping):
        raise PairMismatchError("paired MCP config mcpServers must be an object")
    selected = servers.get(receipt.mcp_server_name)
    if not isinstance(selected, Mapping):
        raise PairMismatchError("paired restricted MCP server is missing")
    command = selected.get("command")
    args = selected.get("args")
    environment = selected.get("env")
    if (
        not isinstance(command, str)
        or command != receipt.mcp_command
        or not isinstance(args, list)
        or args != ["agent-ssh-client"]
        or not isinstance(environment, Mapping)
        or set(environment) != {"OKG_AGENT_SSH_ROUTE", "OKG_AGENT_SSH_GRANT"}
        or not isinstance(environment.get("OKG_AGENT_SSH_ROUTE"), str)
        or not environment["OKG_AGENT_SSH_ROUTE"].strip()
        or environment["OKG_AGENT_SSH_ROUTE"] != receipt.mcp_route
        or not isinstance(environment.get("OKG_AGENT_SSH_GRANT"), str)
        or not environment["OKG_AGENT_SSH_GRANT"].strip()
        or environment["OKG_AGENT_SSH_GRANT"] != receipt.grant_id
    ):
        raise PairMismatchError("paired MCP server is not a restricted SSH/stdio route")
    serialized = json.dumps(payload, sort_keys=True).lower()
    if any(
        marker in serialized
        for marker in ("okg_dsn", "database_url", "postgres://", "postgresql://")
    ):
        raise PairMismatchError("paired MCP config contains a raw database credential")


def _metadata_identity_matches(
    metadata: Mapping[str, Any],
    receipt: PairedWorkspaceReceipt,
    *,
    allow_state_move: bool,
) -> None:
    """Check the complete server-owned binding, including required nulls.

    A missing field is not treated as a legacy-compatible value.  The branch
    metadata is the graph half's authority, so silently accepting old or
    partially-written bindings would allow a receipt to cross assignments.
    """
    expected: dict[str, Any] = {
        "kind": "okg_paired_workspace",
        "assignment_id": receipt.assignment_id,
        "assignment_generation": receipt.assignment_generation,
        "authenticated_host": receipt.authenticated_host,
        "repo_path": receipt.repo_path,
        "worktree_path": receipt.worktree_path,
        "git_branch": receipt.git.branch,
        "deployment": receipt.graph.deployment,
        "base_branch": receipt.graph.base_branch,
        "base_generation": receipt.graph.base_generation,
        "branch": receipt.graph.branch,
        "checkpoint": receipt.graph.checkpoint,
        "checkpoint_ref": receipt.graph.checkpoint_ref,
        "checkpoint_ids": list(receipt.graph.checkpoint_ids),
        "lifecycle_status": receipt.lifecycle_status,
        "predecessor_digest": receipt.predecessor_digest,
        "cleanup_disposition": receipt.cleanup_disposition,
        "config_path": receipt.config_path,
        "mcp_server_name": receipt.mcp_server_name,
        "mcp_route": receipt.mcp_route,
        "mcp_command": receipt.mcp_command,
        "grant_id": receipt.grant_id,
        "provision_attempt_id": receipt.graph.metadata.get("provision_attempt_id", ""),
        "current_ref": receipt.graph.current_ref,
        "base_checkpoint": receipt.graph.metadata.get("base_checkpoint"),
    }
    if not allow_state_move:
        expected.update(
            {
                "git_base_commit": receipt.git.base_commit,
                "git_head_commit": receipt.git.head_commit,
                "git_tree": receipt.git.tree,
                "git_dirty": receipt.git.dirty,
                "git_dirty_paths": list(receipt.git.dirty_paths),
            }
        )
    for key, value in expected.items():
        if key not in metadata:
            raise PairMismatchError(f"paired workspace metadata is missing {key}")
        if allow_state_move and key == "current_ref":
            continue
        if metadata[key] != value:
            if key == "lifecycle_status":
                raise PairStaleError(
                    f"paired workspace lifecycle is {metadata[key]!r}, receipt is "
                    f"{receipt.lifecycle_status!r}"
                )
            if key == "predecessor_digest":
                raise PairStaleError(
                    "paired workspace predecessor lineage does not match the receipt"
                )
            raise PairMismatchError(
                f"paired workspace metadata mismatch for {key}: "
                f"expected {value!r}, observed {metadata[key]!r}"
            )


def _coerce_git(value: Any, *, path: str | Path) -> GitWorkspaceState:
    if isinstance(value, GitWorkspaceState):
        return value
    source = value if isinstance(value, Mapping) else vars(value)
    worktree = _resolved(source.get("worktree_path", path))
    return GitWorkspaceState(
        repo_path=_resolved(
            source.get("repo_path", in_proc_workdir(Path(worktree)) or worktree)
        ),
        worktree_path=worktree,
        branch=str(source.get("branch", source.get("git_branch", ""))),
        base_commit=str(source.get("base_commit", source.get("base_sha", ""))),
        head_commit=str(source.get("head_commit", source.get("head_sha", ""))),
        tree=str(source.get("tree", source.get("tree_sha", ""))),
        dirty=bool(source.get("dirty", source.get("git_dirty", False))),
        dirty_paths=tuple(str(item) for item in source.get("dirty_paths", ())),
    )


def _coerce_graph(value: Any, *, deployment: str, branch: str) -> GraphWorkspaceState:
    if isinstance(value, GraphWorkspaceState):
        return value
    source = value if isinstance(value, Mapping) else vars(value)
    return GraphWorkspaceState(
        deployment=str(
            source.get("deployment", source.get("graph_deployment", deployment))
        ),
        base_branch=str(source.get("base_branch", "default")),
        base_generation=(
            None
            if source.get("base_generation", source.get("base_generation_id")) is None
            else str(source.get("base_generation", source.get("base_generation_id")))
        ),
        branch=str(source.get("branch", source.get("graph_branch", branch))),
        current_ref=str(source.get("current_ref", "")),
        checkpoint=(
            None
            if source.get("checkpoint", source.get("checkpoint_id")) is None
            else str(source.get("checkpoint", source.get("checkpoint_id")))
        ),
        checkpoint_ref=(
            None
            if source.get("checkpoint_ref") is None
            else str(source["checkpoint_ref"])
        ),
        checkpoint_ids=tuple(
            str(item) for item in source.get("checkpoint_ids", ()) if str(item)
        ),
        assignment_id=(
            None
            if source.get("assignment_id") is None
            else str(source["assignment_id"])
        ),
        assignment_generation=(
            None
            if source.get("assignment_generation") is None
            else int(source["assignment_generation"])
        ),
        authenticated_host=(
            None
            if source.get("authenticated_host") is None
            else str(source["authenticated_host"])
        ),
        lifecycle_status=cast(
            LifecycleStatus, str(source.get("lifecycle_status", "staged"))
        ),
        predecessor_digest=(
            None
            if source.get("predecessor_digest") is None
            else str(source["predecessor_digest"])
        ),
        cleanup_disposition=cast(
            CleanupDisposition, str(source.get("cleanup_disposition", "pending"))
        ),
        metadata=dict(source.get("metadata", {})),
    )


class PairedWorkspaceLifecycle:
    """Provision and fence one exact Git/Chronos workspace pair."""

    def __init__(
        self,
        git: GitWorkspaceAdapter,
        graph: GraphWorkspaceAdapter,
        *,
        config_filename: str = str(DEFAULT_COORDINATION_CONFIG),
    ) -> None:
        self.git = git
        self.graph = graph
        self.config_filename = config_filename

    @contextmanager
    def close_lock(self, request: PairedWorkspaceRequest) -> Iterator[None]:
        """Hold the pair's provision lock across one complete close saga.

        Close is a read/fence/write sequence rather than three independent
        lifecycle calls.  Production graph adapters expose a session lock for
        this branch; keeping it held through adoption, retirement, and local
        cleanup prevents a concurrent graph writer from moving the ref between
        the close digest and the guarded retirement.  Lightweight adapters may
        omit the lock, in which case the caller still receives the same
        fail-closed checks but cannot claim cross-process serialization.
        """

        request = self._normalise_request(request)
        provision_lock = getattr(self.graph, "provision_lock", None)
        if callable(provision_lock):
            with provision_lock(
                request.graph_deployment, request.resolved_graph_branch
            ):
                yield
            return
        yield

    def stage(self, request: PairedWorkspaceRequest) -> PairedWorkspaceReceipt:
        """Create both halves and config, rolling all created resources back."""
        request = self._normalise_request(request)
        _reject_symlinked_path_chain(request.worktree_path, label="paired worktree")
        repo_path = _resolved(request.repo_path)
        worktree_path = _resolved(request.worktree_path)
        config_path = str(
            self._config_path(
                worktree_path,
                request.config_path or Path(worktree_path) / self.config_filename,
            )
        )
        self._validate_paths(repo_path, worktree_path, config_path)
        request = replace(request, config_path=config_path)

        # Capture the exact pre-existing file so a failed saga can restore it
        # byte-for-byte.  The JSON validation is done before creating either
        # resource, which keeps an invalid config from producing a partial pair.
        original_config: str | None = None
        config_file = Path(config_path)
        self._validate_config_filesystem(worktree_path, config_file)
        if config_file.exists():
            if not config_file.is_file():
                raise ValueError("MCP config path is not a regular file")
            original_config = config_file.read_text(encoding="utf-8")
            _merge_mcp_config(
                original_config,
                server_name=request.mcp_server_name,
                spec={},
            )

        base_state = _coerce_git(self.git.inspect(repo_path), path=repo_path)
        base_commit = request.git_base_commit or base_state.head_commit
        _validate_commit_ref(base_commit)
        resolve_commit = getattr(self.git, "resolve_commit", None)
        if resolve_commit is not None:
            try:
                base_commit = str(resolve_commit(repo_path, base_commit))
            except Exception as exc:  # noqa: BLE001 - immutable base proof
                raise PairMismatchError("Git base ref could not be resolved") from exc
        if not _is_full_commit_sha(base_commit):
            raise PairMismatchError("Git base must be an immutable full commit SHA")
        base_generation = request.graph_base_generation
        base_checkpoint = request.graph_base_checkpoint
        if (
            base_generation is not None
            and base_checkpoint is not None
            and str(base_generation) != str(base_checkpoint)
        ):
            raise PairMismatchError(
                "graph base generation and checkpoint must name the same checkpoint"
            )
        if base_generation is None and base_checkpoint is not None:
            # A checkpoint is itself the immutable generation identity; do not
            # silently pair it with a moving/latest base generation.
            base_generation = base_checkpoint
        graph_branch = request.resolved_graph_branch
        if base_generation is None:
            base_generation = self.graph.latest_generation(
                request.graph_deployment, request.graph_base_branch
            )
        if base_generation is not None and base_checkpoint is None:
            validator = getattr(self.graph, "validate_checkpoint", None)
            if validator is not None:
                try:
                    validator(
                        request.graph_deployment,
                        request.graph_base_branch,
                        str(base_generation),
                    )
                except Exception as exc:  # noqa: BLE001 - exact base proof
                    raise PairMismatchError(
                        "graph base generation is not an owned checkpoint"
                    ) from exc
                base_checkpoint = str(base_generation)
        request = replace(
            request,
            graph_base_generation=(
                None if base_generation is None else str(base_generation)
            ),
            graph_base_checkpoint=base_checkpoint,
        )
        if self._exists_git(repo_path, request.git_branch) or self._exists_graph(
            request.graph_deployment, graph_branch
        ):
            raise PairMismatchError(
                "refusing to reuse an existing Git or Chronos branch"
            )

        git_attempted = False
        graph_attempted = False
        config_attempted = False
        attempt_nonce = request.provision_attempt_id or uuid.uuid4().hex
        # The server-issued attempt id is intentionally stable across exact
        # retries.  This invocation id is not: it identifies the one process
        # allowed to compensate a branch whose create call returned
        # ambiguously after another process may have won the race.
        invocation_id = uuid.uuid4().hex
        grant_id = request.grant_id
        if not grant_id:
            raise ValueError("grant_id must be explicitly supplied")
        # Reserve the Chronos branch first.  The nonce is durable authority for
        # this attempt, so a concurrent loser can never roll back a winner's
        # branch after an adapter raises following creation.
        reservation_git = GitWorkspaceState(
            repo_path=repo_path,
            worktree_path=worktree_path,
            branch=request.git_branch,
            base_commit=base_commit,
            head_commit="",
            tree="",
            dirty=False,
        )
        reservation_graph = GraphWorkspaceState(
            request.graph_deployment,
            request.graph_base_branch,
            base_generation,
            graph_branch,
            "",
            metadata={
                "provision_attempt_id": attempt_nonce,
                "provision_invocation_id": invocation_id,
            },
        )
        branch_metadata = _metadata_for(
            request,
            git=reservation_git,
            graph=reservation_graph,
            lifecycle_status="staged",
            predecessor_digest=request.predecessor_digest,
            cleanup_disposition="pending",
        )
        branch_metadata["provision_attempt_id"] = attempt_nonce
        branch_metadata["provision_invocation_id"] = invocation_id
        try:
            graph_attempted = True
            if request.graph_base_checkpoint is not None:
                create_from_checkpoint = getattr(
                    self.graph,
                    "create_branch_from_checkpoint",
                    None,
                )
                if create_from_checkpoint is None:
                    raise PairMismatchError(
                        "exact graph continuation requires checkpoint branch support"
                    )
                else:
                    provisional_graph = create_from_checkpoint(
                        request.graph_deployment,
                        graph_branch,
                        request.graph_base_checkpoint,
                        branch_metadata,
                    )
            else:
                provisional_graph = self.graph.create_branch(
                    request.graph_deployment,
                    graph_branch,
                    request.graph_base_branch,
                    base_generation,
                    branch_metadata,
                )
            graph_state = _coerce_graph(
                provisional_graph,
                deployment=request.graph_deployment,
                branch=graph_branch,
            )
            if (
                graph_state.branch != graph_branch
                or graph_state.deployment != request.graph_deployment
                or not graph_state.current_ref
                or graph_state.base_generation != base_generation
                or (
                    request.graph_base_checkpoint is not None
                    and graph_state.metadata.get("base_checkpoint")
                    != request.graph_base_checkpoint
                )
            ):
                raise PairMismatchError(
                    "Chronos branch did not read back the requested identity"
                )
            # Creation APIs may not know the continuation ref until after the
            # branch row exists.  Persist that actual ref before strict
            # read-back, rather than treating an empty placeholder as a
            # binding.
            branch_metadata["current_ref"] = graph_state.current_ref
            graph_state = _coerce_graph(
                self.graph.update_metadata(
                    request.graph_deployment, graph_branch, branch_metadata
                ),
                deployment=request.graph_deployment,
                branch=graph_branch,
            )

            git_attempted = True
            git_state = _coerce_git(
                self.git.create_worktree(
                    repo_path, worktree_path, request.git_branch, base_commit
                ),
                path=worktree_path,
            )
            if git_state.branch != request.git_branch or git_state.base_commit not in {
                "",
                base_commit,
            }:
                raise PairMismatchError(
                    "Git worktree did not read back the requested branch/base"
                )
            branch_metadata = _metadata_for(
                request,
                git=git_state,
                graph=graph_state,
                lifecycle_status="staged",
                predecessor_digest=request.predecessor_digest,
                cleanup_disposition="pending",
            )
            branch_metadata["provision_attempt_id"] = attempt_nonce
            branch_metadata["provision_invocation_id"] = invocation_id
            branch_metadata["current_ref"] = graph_state.current_ref
            graph_state = _coerce_graph(
                self.graph.update_metadata(
                    request.graph_deployment, graph_branch, branch_metadata
                ),
                deployment=request.graph_deployment,
                branch=graph_branch,
            )

            route = _validate_route(request.mcp_route)
            spec = restricted_ssh_stdio_mcp_spec(
                route,
                grant_id=grant_id,
                wrapper_command=request.mcp_command,
            )
            # The linked worktree may have materialised a tracked or
            # adapter-created target after the preflight above.  Re-check the
            # lexical path before reading or replacing anything; in
            # particular, never let ``read_text`` follow a newly-created
            # symlink to a source file.
            self._validate_config_filesystem(worktree_path, config_file)
            self._register_managed_config(worktree_path, config_path)
            # A source checkout may carry a tracked or adapter-created config
            # that only appears once the worktree exists.  Capture it before
            # replacing the selected server so a failed saga can restore it if
            # worktree removal itself is unavailable.
            if original_config is None and config_file.exists():
                original_config = config_file.read_text(encoding="utf-8")
                _merge_mcp_config(
                    original_config,
                    server_name=request.mcp_server_name,
                    spec={},
                )
            config_text = _merge_mcp_config(
                original_config
                if original_config is not None
                else (
                    config_file.read_text(encoding="utf-8")
                    if config_file.exists()
                    else "{}"
                ),
                server_name=request.mcp_server_name,
                spec=spec,
            )
            config_attempted = True
            _atomic_write_private(
                config_file,
                config_text,
            )

            receipt = PairedWorkspaceReceipt(
                assignment_id=request.assignment_id,
                assignment_generation=request.assignment_generation,
                authenticated_host=request.authenticated_host,
                repo_path=repo_path,
                worktree_path=worktree_path,
                git=git_state,
                graph=graph_state,
                lifecycle_status="staged",
                predecessor_digest=request.predecessor_digest,
                config_path=config_path,
                mcp_server_name=request.mcp_server_name,
                mcp_route=route,
                mcp_command=request.mcp_command,
                grant_id=grant_id,
            )
            return self._read_back(receipt, require_clean=True)
        except Exception as exc:
            rollback_errors: list[str] = []
            if config_attempted:
                try:
                    if original_config is None:
                        config_file.unlink(missing_ok=True)
                    else:
                        _atomic_write_private(config_file, original_config)
                except OSError as rollback_exc:
                    rollback_errors.append(f"config rollback: {rollback_exc}")
            git_cleanup_succeeded = not git_attempted
            if git_attempted:
                try:
                    if self._git_attempt_owned(
                        request,
                        repo_path=repo_path,
                        worktree_path=worktree_path,
                        base_commit=base_commit,
                    ):
                        self._remove_git_attempt(
                            request,
                            repo_path=repo_path,
                            worktree_path=worktree_path,
                            expected_commit=base_commit,
                        )
                        git_cleanup_succeeded = True
                    else:
                        git_cleanup_succeeded = False
                        rollback_errors.append(
                            "Git rollback ownership could not be proven"
                        )
                except Exception as rollback_exc:  # noqa: BLE001 - saga cleanup
                    git_cleanup_succeeded = False
                    rollback_errors.append(f"Git rollback: {rollback_exc}")
            if graph_attempted and git_cleanup_succeeded:
                try:
                    if self._graph_attempt_owned(
                        request,
                        graph_branch=graph_branch,
                        base_generation=base_generation,
                        attempt_nonce=attempt_nonce,
                        invocation_id=invocation_id,
                    ):
                        remove_guarded = getattr(
                            self.graph, "remove_branch_guarded", None
                        )
                        if remove_guarded is not None:
                            remove_guarded(request.graph_deployment, graph_branch)
                        else:
                            self.graph.remove_branch(
                                request.graph_deployment, graph_branch
                            )
                    else:
                        rollback_errors.append(
                            "graph rollback ownership could not be proven"
                        )
                except Exception as rollback_exc:  # noqa: BLE001 - saga cleanup
                    rollback_errors.append(f"graph rollback: {rollback_exc}")
            elif graph_attempted and git_attempted:
                rollback_errors.append(
                    "graph rollback retained until Git rollback succeeds"
                )
            suffix = (
                f"; rollback incomplete: {', '.join(rollback_errors)}"
                if rollback_errors
                else ""
            )
            raise PairProvisionError(
                f"paired workspace staging failed: {exc}{suffix}"
            ) from exc

    provision = stage

    def adopt(self, request: PairedWorkspaceRequest) -> PairedWorkspaceReceipt:
        """Read back a pair created by the exact same provisioning attempt.

        Adoption never creates or repairs either half.  Both the Git branch and
        Chronos branch must already exist, and the complete Chronos metadata,
        assignment generation, route, grant, base identities, and optional
        server-issued attempt id must match the request.
        """

        request = self._normalise_request(request)
        _reject_symlinked_path_chain(request.worktree_path, label="paired worktree")
        repo_path = _resolved(request.repo_path)
        worktree_path = _resolved(request.worktree_path)
        config_path = str(
            self._config_path(
                worktree_path,
                request.config_path or Path(worktree_path) / self.config_filename,
            )
        )
        self._validate_paths(repo_path, worktree_path, config_path)
        request = replace(request, config_path=config_path)
        graph_branch = request.resolved_graph_branch
        if not self._exists_git(
            repo_path, request.git_branch
        ) or not self._exists_graph(request.graph_deployment, graph_branch):
            raise PairMismatchError("paired workspace is not complete enough to adopt")

        git = _coerce_git(self.git.inspect(worktree_path), path=worktree_path)
        graph = _coerce_graph(
            self.graph.inspect(request.graph_deployment, graph_branch),
            deployment=request.graph_deployment,
            branch=graph_branch,
        )
        if git.branch != request.git_branch:
            raise PairMismatchError("adopted Git branch does not match the request")
        if request.git_base_commit is not None and git.base_commit not in {
            "",
            request.git_base_commit,
        }:
            raise PairMismatchError("adopted Git base does not match the request")
        if graph.base_branch != request.graph_base_branch:
            raise PairMismatchError(
                "adopted graph base branch does not match the request"
            )
        if (
            request.graph_base_generation is not None
            and graph.base_generation != request.graph_base_generation
        ):
            raise PairMismatchError(
                "adopted graph base generation does not match the request"
            )
        if graph.metadata.get("base_checkpoint") != request.graph_base_checkpoint:
            raise PairMismatchError(
                "adopted graph base checkpoint does not match the request"
            )
        if (
            request.provision_attempt_id is not None
            and graph.metadata.get("provision_attempt_id")
            != request.provision_attempt_id
        ):
            raise PairMismatchError(
                "adopted pair belongs to another provisioning attempt"
            )
        if graph.lifecycle_status not in {"staged", "active"}:
            raise PairStaleError(
                f"paired workspace lifecycle {graph.lifecycle_status!r} cannot be adopted"
            )
        receipt = PairedWorkspaceReceipt(
            assignment_id=request.assignment_id,
            assignment_generation=request.assignment_generation,
            authenticated_host=request.authenticated_host,
            repo_path=repo_path,
            worktree_path=worktree_path,
            git=git,
            graph=graph,
            lifecycle_status=graph.lifecycle_status,
            predecessor_digest=graph.predecessor_digest,
            cleanup_disposition=graph.cleanup_disposition,
            config_path=config_path,
            mcp_server_name=request.mcp_server_name,
            mcp_route=_validate_route(request.mcp_route),
            mcp_command=request.mcp_command,
            grant_id=str(request.grant_id or ""),
        )
        return self._read_back(receipt, require_clean=True)

    def adopt_for_close(
        self, request: PairedWorkspaceRequest
    ) -> PairedWorkspaceReceipt:
        """Read-only adoption of the exact pair named by a close plan.

        Close runs after the worker may have checkpointed or handed off its
        pair.  Unlike :meth:`adopt`, this path never stages a missing half or
        persists a missing Git ignore rule.  It validates the complete
        server-issued start plan, the persisted pair metadata, and a clean
        target before returning a receipt that ``retire`` may consume.
        """

        request = self._normalise_request(request)
        _reject_symlinked_path_chain(request.worktree_path, label="paired worktree")
        repo_path = _resolved(request.repo_path)
        worktree_path = _resolved(request.worktree_path)
        config_path = str(
            self._config_path(
                worktree_path,
                request.config_path or Path(worktree_path) / self.config_filename,
            )
        )
        self._validate_paths(repo_path, worktree_path, config_path)
        request = replace(request, config_path=config_path)

        # A close plan always comes from a finalized start plan.  Requiring
        # its immutable base and server-issued attempt here prevents a caller
        # from turning an ordinary request into authority for an unrelated
        # pair with the same branch name.
        if request.provision_attempt_id is None:
            raise PairMismatchError(
                "close adoption requires the exact provisioning attempt"
            )
        if request.git_base_commit is None or not _is_full_commit_sha(
            request.git_base_commit
        ):
            raise PairMismatchError(
                "close adoption requires an immutable Git base commit"
            )

        graph_branch = request.resolved_graph_branch
        if not self._exists_git(
            repo_path, request.git_branch
        ) or not self._exists_graph(request.graph_deployment, graph_branch):
            raise PairMismatchError("paired workspace is not complete enough to adopt")

        # A fresh production Git adapter needs this in-memory base observation
        # to distinguish the finalized start base from the worker's later
        # head.  It does not write the worktree, Git refs, or ignore files.
        register_base = getattr(self.git, "register_receipt_base", None)
        if callable(register_base):
            try:
                register_base(worktree_path, repo_path, request.git_base_commit)
            except Exception as exc:  # noqa: BLE001 - exact base proof
                raise PairMismatchError(
                    "Git close plan base commit could not be verified"
                ) from exc

        git = _coerce_git(self.git.inspect(worktree_path), path=worktree_path)
        graph = _coerce_graph(
            self.graph.inspect(request.graph_deployment, graph_branch),
            deployment=request.graph_deployment,
            branch=graph_branch,
        )

        if (
            git.repo_path != repo_path
            or git.worktree_path != worktree_path
            or git.branch != request.git_branch
        ):
            raise PairMismatchError("close plan does not own the current Git worktree")
        if git.base_commit.lower() != request.git_base_commit.lower():
            raise PairMismatchError("Git base commit does not match the close plan")
        if git.dirty:
            raise PairMismatchError("paired Git worktree is dirty")

        if (
            graph.deployment != request.graph_deployment
            or graph.branch != graph_branch
            or not graph.current_ref
            or graph.base_branch != request.graph_base_branch
            or graph.base_generation != request.graph_base_generation
            or graph.assignment_id != request.assignment_id
            or graph.assignment_generation != request.assignment_generation
            or graph.authenticated_host != request.authenticated_host
        ):
            raise PairMismatchError("graph identity does not match the close plan")
        if graph.metadata.get("base_checkpoint") != request.graph_base_checkpoint:
            raise PairMismatchError(
                "graph base checkpoint does not match the close plan"
            )
        if graph.metadata.get("provision_attempt_id") != request.provision_attempt_id:
            raise PairMismatchError(
                "paired workspace belongs to another provisioning attempt"
            )
        if graph.lifecycle_status not in {"active", "checkpointed", "handed_off"}:
            raise PairStaleError(
                f"paired workspace lifecycle {graph.lifecycle_status!r} "
                "cannot be adopted for close"
            )

        receipt = PairedWorkspaceReceipt(
            assignment_id=request.assignment_id,
            assignment_generation=request.assignment_generation,
            authenticated_host=request.authenticated_host,
            repo_path=repo_path,
            worktree_path=worktree_path,
            git=git,
            graph=graph,
            lifecycle_status=graph.lifecycle_status,
            predecessor_digest=graph.predecessor_digest,
            cleanup_disposition=graph.cleanup_disposition,
            config_path=config_path,
            mcp_server_name=request.mcp_server_name,
            mcp_route=_validate_route(request.mcp_route),
            mcp_command=request.mcp_command,
            grant_id=str(request.grant_id or ""),
        )

        # Validate persisted ownership and route content without registering
        # or rewriting anything.  The metadata check includes all start-plan
        # fields plus the Git and graph observations captured above.
        self._validate_config_filesystem(worktree_path, Path(config_path))
        _validate_managed_config(receipt)
        _metadata_identity_matches(graph.metadata, receipt, allow_state_move=False)

        if receipt.lifecycle_status in {"checkpointed", "handed_off"}:
            checkpoint = graph.checkpoint
            checkpoint_ref = graph.checkpoint_ref
            if (
                not checkpoint
                or not checkpoint_ref
                or checkpoint not in graph.checkpoint_ids
            ):
                raise PairMismatchError(
                    "close adoption requires a complete graph checkpoint binding"
                )
            matches_checkpoint = getattr(self.graph, "checkpoint_matches_current", None)
            if matches_checkpoint is None:
                matches = graph.current_ref == checkpoint_ref
            else:
                try:
                    matches = bool(
                        matches_checkpoint(
                            graph.deployment,
                            graph.branch,
                            checkpoint,
                            checkpoint_ref,
                            graph.current_ref,
                        )
                    )
                except Exception as exc:  # noqa: BLE001 - stale checkpoint fence
                    raise PairStaleError(
                        "recorded graph checkpoint could not be verified"
                    ) from exc
            if not matches:
                raise PairStaleError("graph has writes after the recorded checkpoint")

        return receipt

    def stage_or_adopt(self, request: PairedWorkspaceRequest) -> PairedWorkspaceReceipt:
        """Stage a new pair or adopt the complete pair from the same attempt."""

        request = self._normalise_request(request)
        _reject_symlinked_path_chain(request.worktree_path, label="paired worktree")
        repo_path = _resolved(request.repo_path)
        worktree_path = _resolved(request.worktree_path)
        config_path = str(
            self._config_path(
                worktree_path,
                request.config_path or Path(worktree_path) / self.config_filename,
            )
        )
        self._validate_paths(repo_path, worktree_path, config_path)
        request = replace(request, config_path=config_path)

        provision_lock = getattr(self.graph, "provision_lock", None)
        if callable(provision_lock):
            with provision_lock(
                request.graph_deployment, request.resolved_graph_branch
            ):
                return self._stage_or_adopt_locked(request, lock_held=True)
        return self._stage_or_adopt_locked(request, lock_held=False)

    def _stage_or_adopt_locked(
        self,
        request: PairedWorkspaceRequest,
        *,
        lock_held: bool,
    ) -> PairedWorkspaceReceipt:
        """Decide and transition while a provision lock is held when available."""
        git_exists = self._exists_git(_resolved(request.repo_path), request.git_branch)
        git_worktree_exists = self._git_present(_resolved(request.worktree_path))
        graph_exists = self._exists_graph(
            request.graph_deployment, request.resolved_graph_branch
        )
        if not git_exists and not git_worktree_exists and not graph_exists:
            return self.stage(request)
        if (git_exists or git_worktree_exists) and graph_exists:
            try:
                return self.adopt(request)
            except PairMismatchError:
                if lock_held and self._recover_provisional_pair(request):
                    return self.stage(request)
                raise
        if graph_exists and not git_exists and not git_worktree_exists and lock_held:
            if self._graph_reservation_matches(request):
                remove_guarded = getattr(self.graph, "remove_branch_guarded", None)
                if remove_guarded is not None:
                    remove_guarded(
                        request.graph_deployment, request.resolved_graph_branch
                    )
                else:
                    self.graph.remove_branch(
                        request.graph_deployment, request.resolved_graph_branch
                    )
                return self.stage(request)
        if graph_exists and not lock_held:
            raise PairMismatchError(
                "graph-only recovery requires an exclusive provisioning lock"
            )
        raise PairMismatchError("paired workspace has only one recoverable half")

    def activate(self, receipt: PairedWorkspaceReceipt) -> PairedWorkspaceReceipt:
        """Activate only a clean read-back pair with the exact receipt identity."""
        self._require_transition(receipt, {"staged", "active"})
        current = self._read_back(receipt, require_clean=True)
        if current.lifecycle_status == "active":
            return current
        graph = self._update_graph(
            current,
            lifecycle_status="active",
            predecessor_digest=current.predecessor_digest,
            cleanup_disposition="pending",
        )
        return replace(current, graph=graph, lifecycle_status="active")

    def checkpoint(self, receipt: PairedWorkspaceReceipt) -> PairedWorkspaceReceipt:
        """Checkpoint the exact clean Git state on the private graph branch."""
        self._require_transition(receipt, {"active", "checkpointed"})
        current = self._read_back(
            receipt, require_clean=True, allow_graph_ref_move=True
        )
        # The lineage is deliberately bounded.  Refuse before asking an
        # adapter to create another checkpoint, so an adapter without a
        # transaction cannot leave an unreportable superseded checkpoint.
        if len(_checkpoint_history(current.graph)) >= MAX_CHECKPOINT_HISTORY:
            raise PairMismatchError(
                "checkpoint retention bound reached; compact before checkpointing"
            )
        try:
            graph = _coerce_graph(
                self.graph.checkpoint(
                    current.graph.deployment,
                    current.graph.branch,
                    _metadata_for(
                        self._request_from_receipt(current),
                        git=current.git,
                        graph=current.graph,
                        lifecycle_status="checkpointed",
                        predecessor_digest=current.predecessor_digest,
                        cleanup_disposition="pending",
                    ),
                ),
                deployment=current.graph.deployment,
                branch=current.graph.branch,
            )
            checkpoint_ids = _checkpoint_history(current.graph, graph.checkpoint)
            graph = replace(graph, checkpoint_ids=checkpoint_ids)
            if (
                graph.lifecycle_status != "checkpointed"
                or graph.checkpoint is None
                or graph.checkpoint_ref is None
                or graph.checkpoint_ids != checkpoint_ids
                or graph.metadata.get("checkpoint") != graph.checkpoint
                or graph.metadata.get("checkpoint_ref") != graph.checkpoint_ref
                or graph.metadata.get("checkpoint_ids") != list(checkpoint_ids)
            ):
                raise PairCheckpointError(
                    "checkpoint adapter did not atomically return its complete binding",
                    checkpoint=graph.checkpoint or "",
                    checkpoint_ref=graph.checkpoint_ref or "",
                )
        except PairCheckpointError:
            raise
        except Exception as exc:  # noqa: BLE001 - checkpoint id may be owned
            if "graph" in locals() and graph.checkpoint and graph.checkpoint_ref:
                raise PairCheckpointError(
                    "checkpoint exists but continuation binding failed",
                    checkpoint=graph.checkpoint,
                    checkpoint_ref=graph.checkpoint_ref,
                ) from exc
            raise
        return replace(current, graph=graph, lifecycle_status="checkpointed")

    def refresh(self, receipt: PairedWorkspaceReceipt) -> PairedWorkspaceReceipt:
        """Accept a clean Git head move only when the original base is an ancestor."""
        self._require_transition(receipt, {"active", "checkpointed"})
        current_git = _coerce_git(
            self.git.inspect(receipt.worktree_path), path=receipt.worktree_path
        )
        if current_git.branch != receipt.git.branch:
            raise PairMismatchError("Git branch changed while refreshing the pair")
        if current_git.dirty:
            raise PairMismatchError("cannot refresh a dirty Git worktree")
        if not self.git.is_ancestor(
            receipt.repo_path, receipt.git.base_commit, current_git.head_commit
        ):
            raise PairStaleError(
                "new Git head is not descended from the recorded base commit"
            )
        # Read the complete graph binding immediately before writing the new
        # Git observation.  The head may move in this transition, but branch,
        # assignment, generation, status, checkpoint, and config identity may
        # not.
        current = self._read_back(
            receipt,
            require_clean=True,
            allow_graph_ref_move=True,
            allow_git_state_move=True,
        )
        if (
            current_git.head_commit == receipt.git.head_commit
            and current_git.tree == receipt.git.tree
        ):
            return current
        # ``inspect`` has no durable receipt context, so production adapters
        # report the current head as their local base observation.  Preserve
        # the receipt's original base for the ancestry fence across refreshes.
        moved = replace(
            current,
            git=replace(current_git, base_commit=receipt.git.base_commit),
            predecessor_digest=receipt_digest(receipt),
        )
        graph = self._update_graph(
            moved,
            lifecycle_status=receipt.lifecycle_status,
            predecessor_digest=moved.predecessor_digest,
            cleanup_disposition=receipt.cleanup_disposition,
        )
        return replace(moved, graph=graph)

    def rebase(self, receipt: PairedWorkspaceReceipt) -> PairedWorkspaceReceipt:
        """Record an externally completed clean rebase under the ancestry fence.

        Git mutation remains the caller's responsibility.  This explicit
        transition is intentionally the same checked path as ``refresh`` so a
        rebased head cannot bypass base, branch, cleanliness, or predecessor
        checks.
        """
        return self.refresh(receipt)

    refresh_after_rebase = rebase

    def handoff(self, receipt: PairedWorkspaceReceipt) -> WorkspaceHandoff:
        """Create a handoff only for a clean pair at its checkpoint ref."""
        self._require_transition(receipt, {"checkpointed"})
        current = self._read_back(receipt, require_clean=True)
        checkpoint_ref = current.graph.checkpoint_ref or current.graph.checkpoint
        if not current.graph.checkpoint or not checkpoint_ref:
            raise PairMismatchError("handoff requires a graph checkpoint")
        matches_checkpoint = getattr(self.graph, "checkpoint_matches_current", None)
        if matches_checkpoint is None:
            if current.graph.current_ref != checkpoint_ref:
                raise PairStaleError(
                    "graph current ref does not equal the checkpoint ref"
                )
        elif not matches_checkpoint(
            current.graph.deployment,
            current.graph.branch,
            current.graph.checkpoint,
            checkpoint_ref,
            current.graph.current_ref,
        ):
            raise PairStaleError("graph has writes after the recorded checkpoint")
        predecessor = receipt_digest(current)
        handoff_id = hashlib.sha256(
            f"handoff:{current.assignment_id}:{predecessor}".encode("utf-8")
        ).hexdigest()
        request = self._request_from_receipt(current)
        handed_graph = replace(
            current.graph,
            lifecycle_status="handed_off",
            predecessor_digest=predecessor,
            cleanup_disposition="pending",
        )
        metadata = _metadata_for(
            request,
            git=current.git,
            graph=handed_graph,
            lifecycle_status="handed_off",
            predecessor_digest=predecessor,
            cleanup_disposition="pending",
        )
        metadata["handoff_id"] = handoff_id
        # A handoff is the intentional audit retention point.  Cleanup may
        # release superseded checkpoints only after it has proved that this
        # branch has no descendants.
        metadata["audit_checkpoint"] = True
        atomic_handoff = getattr(self.graph, "handoff_if_checkpoint_current", None)
        if atomic_handoff is not None:
            handed = _coerce_graph(
                atomic_handoff(
                    current.graph.deployment,
                    current.graph.branch,
                    current.graph.checkpoint,
                    checkpoint_ref,
                    current.graph.current_ref,
                    metadata,
                    expected={
                        "assignment_id": current.assignment_id,
                        "assignment_generation": current.assignment_generation,
                        "lifecycle_status": current.lifecycle_status,
                        "current_ref": current.graph.current_ref,
                    },
                ),
                deployment=current.graph.deployment,
                branch=current.graph.branch,
            )
        else:
            handed = self._update_graph(
                current,
                lifecycle_status="handed_off",
                predecessor_digest=predecessor,
                cleanup_disposition="pending",
                extra={"handoff_id": handoff_id, "audit_checkpoint": True},
            )
        return WorkspaceHandoff(
            handoff_id=handoff_id,
            receipt=replace(current, graph=handed, lifecycle_status="handed_off"),
            predecessor_digest=predecessor,
        )

    def _replacement_retirement_state(
        self,
        predecessor: PairedWorkspaceReceipt,
        successor: PairedWorkspaceReceipt,
    ) -> bool | None:
        """Read authoritative lineage after a replacement write exception.

        ``None`` is intentionally different from ``False``: a failed read-back
        cannot prove that retirement did not commit, so the successor must be
        preserved for recovery instead of being destructively compensated.
        """
        try:
            observed = _coerce_graph(
                self.graph.inspect(
                    predecessor.graph.deployment, predecessor.graph.branch
                ),
                deployment=predecessor.graph.deployment,
                branch=predecessor.graph.branch,
            )
        except Exception as exc:
            error = PairProvisionError(
                "replacement outcome is uncertain; successor retained for recovery"
            )
            error.successor = successor
            error.rollback_incomplete = True
            raise error from exc
        if observed.lifecycle_status in {"active", "checkpointed", "handed_off"}:
            return False
        if observed.lifecycle_status != "retired":
            return None
        metadata = observed.metadata
        if (
            metadata.get("successor_branch") != successor.graph.branch
            or metadata.get("successor_assignment_generation")
            != successor.assignment_generation
            or metadata.get("successor_digest") != receipt_digest(successor)
        ):
            return None
        try:
            successor_current = self._read_back(successor, require_clean=True)
        except Exception as exc:
            error = PairProvisionError(
                "replacement outcome is uncertain; successor retained for recovery"
            )
            error.successor = successor
            error.rollback_incomplete = True
            raise error from exc
        if (
            successor_current.lifecycle_status != "active"
            or successor_current.graph.lifecycle_status != "active"
            or successor_current.graph.predecessor_digest != receipt_digest(predecessor)
        ):
            return None
        return True

    def replace(
        self,
        handoff: WorkspaceHandoff,
        request: PairedWorkspaceRequest | None = None,
    ) -> PairedWorkspaceReceipt:
        """Create a successor only from a handoff with a strictly newer generation."""
        old = handoff.receipt
        if old.lifecycle_status != "handed_off":
            raise PairStaleError("replacement requires a handed-off predecessor")
        if not old.graph.checkpoint:
            raise PairMismatchError("replacement requires a predecessor checkpoint")
        if request is None:
            raise PairMismatchError(
                "replacement requires an explicit successor request and grant"
            )
        else:
            if not request.grant_id or request.grant_id == old.grant_id:
                raise PairMismatchError(
                    "replacement grant must be nonempty and distinct from predecessor"
                )
            # Omitted continuation pins inherit the handoff's exact state;
            # callers cannot accidentally fall back to the repository's
            # moving default branch or current HEAD.
            request = replace(
                request,
                predecessor_digest=receipt_digest(old),
                git_base_commit=request.git_base_commit or old.git.head_commit,
                graph_base_branch=old.graph.branch,
                graph_base_generation=old.graph.checkpoint,
                graph_base_checkpoint=old.graph.checkpoint,
                grant_id=request.grant_id,
                mcp_route=request.mcp_route or old.mcp_route,
                mcp_command=request.mcp_command or old.mcp_command,
            )
        self._normalise_request(request)
        if request.assignment_id != old.assignment_id:
            raise PairMismatchError("replacement assignment_id differs from handoff")
        if request.assignment_generation <= old.assignment_generation:
            raise PairStaleError("replacement generation must be strictly newer")
        if request.authenticated_host != old.authenticated_host:
            raise PairMismatchError(
                "replacement authenticated host differs from handoff"
            )
        if request.git_base_commit != old.git.head_commit:
            raise PairMismatchError(
                "replacement must start from the handed-off Git head"
            )
        graph_continuation_proven = (
            request.graph_base_branch == old.graph.branch
            and request.graph_base_generation == old.graph.checkpoint
            and request.graph_base_checkpoint == old.graph.checkpoint
        )
        if not graph_continuation_proven:
            raise PairMismatchError(
                "replacement must start from the handed-off graph checkpoint"
            )
        self._validate_graph_checkpoint(old)
        if (
            request.git_branch == old.git.branch
            or request.resolved_graph_branch == old.graph.branch
        ):
            raise PairMismatchError(
                "replacement cannot reuse predecessor branch identity"
            )

        successor: PairedWorkspaceReceipt | None = None
        try:
            successor = self.activate(self.stage(request))
            retired_graph = self._update_graph(
                old,
                lifecycle_status="retired",
                predecessor_digest=old.predecessor_digest,
                cleanup_disposition="retained",
                extra={
                    "successor_assignment_generation": successor.assignment_generation,
                    "successor_branch": successor.graph.branch,
                    "successor_digest": receipt_digest(successor),
                },
            )
            _ = retired_graph
            return successor
        except Exception as exc:
            rollback_errors: list[str] = []
            if successor is not None:
                retirement_state = self._replacement_retirement_state(old, successor)
                if retirement_state is not False:
                    # Retirement may already be committed even when its
                    # post-commit read-back failed.  Preserve the real
                    # successor and surface an explicit recovery boundary;
                    # cleaning it here would destroy a valid handoff.
                    retained_error = PairProvisionError(
                        "replacement outcome is uncertain; successor retained for recovery"
                    )
                    retained_error.successor = successor
                    retained_error.rollback_incomplete = True
                    raise retained_error from exc
                # Fence the real successor before cleanup.  Passing a locally
                # edited receipt to cleanup would let its ownership check see
                # the still-active graph metadata and leave an orphan behind.
                try:
                    fenced = self.retire(successor, disposition="abandoned")
                    report = self.cleanup(fenced)
                    if not report.complete:
                        rollback_errors.append(
                            "successor cleanup incomplete: " + "; ".join(report.errors)
                        )
                except Exception as rollback_exc:  # noqa: BLE001 - saga compensation
                    rollback_errors.append(f"successor rollback: {rollback_exc}")
            if rollback_errors:
                raise PairProvisionError(
                    f"paired workspace replacement failed: {exc}; "
                    f"rollback incomplete: {'; '.join(rollback_errors)}"
                ) from exc
            raise

    def retire(
        self,
        receipt: PairedWorkspaceReceipt,
        *,
        disposition: Literal["retained", "abandoned"] = "retained",
        successor: PairedWorkspaceReceipt | None = None,
        close_attempt_id: str | None = None,
    ) -> PairedWorkspaceReceipt:
        """Fence a pair and record its retention/abandonment disposition.

        ``close_attempt_id`` is a durable marker for the close saga.  When it
        is supplied, the current graph ref and Git state are read back
        strictly (the generic replacement path may admit a graph ref move),
        and the marker is written in the same graph update as retirement.
        A later close retry can therefore distinguish an exact partially
        cleaned pair from an arbitrary target collision.
        """
        if close_attempt_id is not None and (
            not close_attempt_id.strip()
            or close_attempt_id.startswith("-")
            or any(char.isspace() for char in close_attempt_id)
        ):
            raise ValueError("close_attempt_id must be one non-option token")
        self._require_transition(receipt, {"active", "checkpointed", "handed_off"})
        current = self._read_back(
            receipt,
            require_clean=close_attempt_id is not None,
            allow_graph_ref_move=close_attempt_id is None,
        )
        extra: dict[str, Any] = {"compaction_disposition": disposition}
        if close_attempt_id is not None:
            extra.update(
                {
                    "close_attempt_id": close_attempt_id,
                    "close_state": "retired",
                    "close_current_ref": current.graph.current_ref,
                }
            )
        if successor is not None:
            if (
                successor.assignment_id != receipt.assignment_id
                or successor.assignment_generation <= receipt.assignment_generation
            ):
                raise PairMismatchError(
                    "successor must be a newer generation of the same assignment"
                )
            successor_current = self._read_back(successor, require_clean=True)
            if successor_current.lifecycle_status != "active":
                raise PairStaleError("successor is not the current active pair")
            if successor_current.graph.lifecycle_status != "active":
                raise PairStaleError("successor graph is not active")
            expected_lineage = receipt_digest(receipt)
            if successor_current.graph.predecessor_digest != expected_lineage:
                raise PairMismatchError(
                    "successor predecessor lineage does not name the retiring pair"
                )
            if (
                successor_current.graph.base_branch != receipt.graph.branch
                or successor_current.graph.base_generation != receipt.graph.checkpoint
                or successor_current.graph.metadata.get("base_checkpoint")
                != receipt.graph.checkpoint
                or successor_current.git.base_commit != receipt.git.head_commit
            ):
                raise PairMismatchError(
                    "successor does not continue the retiring pair exactly"
                )
            extra.update(
                {
                    "successor_assignment_generation": successor_current.assignment_generation,
                    "successor_branch": successor_current.graph.branch,
                    "successor_digest": receipt_digest(successor_current),
                }
            )
        graph = self._update_graph(
            current,
            lifecycle_status="retired",
            predecessor_digest=current.predecessor_digest,
            cleanup_disposition=cast(CleanupDisposition, disposition),
            extra=extra,
        )
        return replace(
            current,
            graph=graph,
            lifecycle_status="retired",
            cleanup_disposition=cast(CleanupDisposition, disposition),
        )

    def recover_close(
        self,
        request: PairedWorkspaceRequest,
        *,
        close_attempt_id: str,
    ) -> CleanupReport:
        """Resume cleanup for an exact close marker after a lost response.

        This is intentionally separate from :meth:`adopt_for_close`: a pair
        that has already been retired is not a live pair and must not be
        reactivated or staged.  Recovery accepts only the graph metadata
        marker written by ``retire(..., close_attempt_id=...)`` and then runs
        the ordinary guarded cleanup.  If the graph has already gone, an
        absence report is allowed only when every exact target resource is
        absent; an adoption exception alone is never sufficient evidence.
        """

        request = self._normalise_request(request)
        with self.close_lock(request):
            return self._recover_close_locked(
                request,
                close_attempt_id=close_attempt_id,
            )

    def _recover_close_locked(
        self,
        request: PairedWorkspaceRequest,
        *,
        close_attempt_id: str,
    ) -> CleanupReport:
        """Locked implementation of :meth:`recover_close` for orchestrators."""

        if (
            not close_attempt_id.strip()
            or close_attempt_id.startswith("-")
            or any(char.isspace() for char in close_attempt_id)
        ):
            raise ValueError("close_attempt_id must be one non-option token")
        request = self._normalise_request(request)
        repo_path = _resolved(request.repo_path)
        worktree_path = _resolved(request.worktree_path)
        config_path = str(
            self._config_path(
                worktree_path,
                request.config_path or Path(worktree_path) / self.config_filename,
            )
        )
        request = replace(request, config_path=config_path)
        graph_branch = request.resolved_graph_branch

        # A graph that has already been removed is a valid retry outcome only
        # when the exact Git/config targets are also absent.  This is the
        # terminal state produced after the marker-authorized cleanup; it does
        # not turn an arbitrary adoption error into a destructive action.
        if not self._exists_graph(request.graph_deployment, graph_branch):
            config_gone = not Path(config_path).exists()
            git_gone = not self._git_present(worktree_path) and not self._exists_git(
                repo_path, request.git_branch
            )
            if config_gone and git_gone:
                return CleanupReport(
                    assignment_id=request.assignment_id,
                    assignment_generation=request.assignment_generation,
                    git_removed=True,
                    graph_removed=True,
                    config_removed=True,
                    complete=True,
                    disposition="complete",
                )
            return CleanupReport(
                assignment_id=request.assignment_id,
                assignment_generation=request.assignment_generation,
                git_removed=git_gone,
                graph_removed=True,
                config_removed=config_gone,
                complete=False,
                disposition="partial",
                errors=(
                    "close graph authority is missing; refusing recovery of remaining resources",
                ),
            )

        graph = _coerce_graph(
            self.graph.inspect(request.graph_deployment, graph_branch),
            deployment=request.graph_deployment,
            branch=graph_branch,
        )
        marker = graph.metadata.get("close_attempt_id")
        if marker != close_attempt_id:
            raise PairMismatchError(
                "paired workspace does not carry the requested close attempt marker"
            )
        if graph.metadata.get("close_current_ref") != graph.current_ref:
            raise PairMismatchError(
                "paired workspace graph current ref moved during close recovery"
            )
        if graph.lifecycle_status not in {"retired", "abandoned", "cleaning"}:
            raise PairStaleError(
                f"paired workspace lifecycle {graph.lifecycle_status!r} cannot be recovered for close"
            )

        if self._git_present(worktree_path):
            metadata = graph.metadata
            base_commit = metadata.get("git_base_commit")
            if not isinstance(base_commit, str) or not _is_full_commit_sha(base_commit):
                raise PairMismatchError(
                    "close recovery metadata lacks an exact Git base commit"
                )
            # A restarted production adapter must restore the immutable base
            # observation before inspecting a linked worktree.  This is a
            # read-only registration; it does not create or mutate either
            # local resource.
            register_base = getattr(self.git, "register_receipt_base", None)
            if callable(register_base):
                try:
                    register_base(worktree_path, repo_path, base_commit)
                except Exception as exc:  # noqa: BLE001 - exact close fence
                    raise PairMismatchError(
                        "close recovery Git base could not be verified"
                    ) from exc
            git = _coerce_git(self.git.inspect(worktree_path), path=worktree_path)
            if git.branch in {"", "HEAD"}:
                # The only detached state admitted to close recovery is the
                # durable marker-owned boundary left after the exact branch
                # CAS delete.  A same-name ref recreated before retry is
                # ambiguous because a Git ref has no attempt identity; leave
                # it and the worktree untouched.
                if self._exists_git(repo_path, request.git_branch):
                    raise PairMismatchError(
                        "close recovery refuses a detached worktree with a recreated branch"
                    )
                # The graph marker remains the authority for the logical
                # branch identity after its Git ref was deleted.  Keep the
                # observed immutable state (head/base/tree/cleanliness) and
                # restore only the expected branch label for the receipt
                # metadata comparison below.
                git = replace(git, branch=request.git_branch)
        else:
            # Cleanup may have removed the linked worktree before a response
            # was lost.  Reconstruct its immutable state from the graph's
            # post-stage metadata; a remaining branch without a worktree is
            # still refused by cleanup as an ambiguous collision.
            metadata = graph.metadata
            required = (
                "repo_path",
                "worktree_path",
                "git_branch",
                "git_base_commit",
                "git_head_commit",
                "git_tree",
                "git_dirty",
                "git_dirty_paths",
            )
            if any(key not in metadata for key in required):
                raise PairMismatchError(
                    "close recovery metadata lacks the exact Git state"
                )
            try:
                git = GitWorkspaceState(
                    repo_path=_resolved(str(metadata["repo_path"])),
                    worktree_path=_resolved(str(metadata["worktree_path"])),
                    branch=str(metadata["git_branch"]),
                    base_commit=str(metadata["git_base_commit"]),
                    head_commit=str(metadata["git_head_commit"]),
                    tree=str(metadata["git_tree"]),
                    dirty=bool(metadata["git_dirty"]),
                    dirty_paths=tuple(
                        str(item) for item in metadata["git_dirty_paths"]
                    ),
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise PairMismatchError(
                    "close recovery metadata has an invalid Git state"
                ) from exc

        receipt = PairedWorkspaceReceipt(
            assignment_id=request.assignment_id,
            assignment_generation=request.assignment_generation,
            authenticated_host=request.authenticated_host,
            repo_path=repo_path,
            worktree_path=worktree_path,
            git=git,
            graph=graph,
            lifecycle_status=graph.lifecycle_status,
            predecessor_digest=graph.predecessor_digest,
            cleanup_disposition=graph.cleanup_disposition,
            config_path=config_path,
            mcp_server_name=request.mcp_server_name,
            mcp_route=_validate_route(request.mcp_route),
            mcp_command=request.mcp_command,
            grant_id=str(request.grant_id or ""),
        )
        if (
            git.repo_path != repo_path
            or git.worktree_path != worktree_path
            or git.branch != request.git_branch
        ):
            raise PairMismatchError("close recovery Git state does not match request")
        _metadata_identity_matches(graph.metadata, receipt, allow_state_move=False)
        return self.cleanup(receipt)

    def cleanup(self, receipt: PairedWorkspaceReceipt) -> CleanupReport:
        """Idempotently remove Git, graph branch, and config; never fake closure."""
        if receipt.lifecycle_status not in {
            "retired",
            "abandoned",
            "cleaning",
            "closed",
        }:
            raise PairStaleError("cleanup requires a retired or abandoned pair")
        errors: list[str] = []
        retained_checkpoint_ids = _checkpoint_history(receipt.graph)
        compacted_checkpoint_ids: tuple[str, ...] = ()
        observed_graph: GraphWorkspaceState | None = None
        close_marker: str | None = None
        detached_git_allowed = False
        if not receipt.config_path:
            errors.append("cleanup receipt has no persisted config path")
        config = Path(receipt.config_path)

        # A receipt is not authority by itself.  The graph branch must still
        # exist and prove this exact binding before either local resource is
        # destroyed.  If it has disappeared, only an already-empty pair can
        # be reported complete; remaining files/worktrees are left untouched
        # because their graph owner is no longer available to authorize them.
        graph_authority = False
        try:
            graph_present = self._exists_graph(
                receipt.graph.deployment, receipt.graph.branch
            )
            if not graph_present:
                if (
                    config.exists()
                    or self._git_present(receipt.worktree_path)
                    or self._exists_git(receipt.repo_path, receipt.git.branch)
                ):
                    errors.append(
                        "graph authority is missing; refusing local destructive cleanup"
                    )
            else:
                observed_graph = _coerce_graph(
                    self.graph.inspect(receipt.graph.deployment, receipt.graph.branch),
                    deployment=receipt.graph.deployment,
                    branch=receipt.graph.branch,
                )
                if observed_graph.lifecycle_status not in {
                    "retired",
                    "abandoned",
                    "cleaning",
                }:
                    raise PairStaleError("cannot clean an active Chronos pair")
                _metadata_identity_matches(
                    observed_graph.metadata,
                    receipt,
                    # A close marker means this branch is in an exact close
                    # saga.  Do not admit a graph ref move between the
                    # retirement fence and cleanup; replacement/compaction
                    # callers retain the older moving-ref behavior.
                    allow_state_move=not bool(
                        receipt.graph.metadata.get("close_attempt_id")
                    ),
                )
                close_marker_value = observed_graph.metadata.get("close_attempt_id")
                if close_marker_value is not None:
                    if (
                        receipt.graph.metadata.get("close_attempt_id")
                        != close_marker_value
                    ):
                        raise PairMismatchError(
                            "cleanup receipt close marker does not match graph authority"
                        )
                    if observed_graph.metadata.get("close_state") not in {
                        "retired",
                        "cleaning",
                    }:
                        raise PairStaleError(
                            "cleanup close marker is not in a recoverable state"
                        )
                    if (
                        observed_graph.metadata.get("close_current_ref")
                        != observed_graph.current_ref
                    ):
                        raise PairMismatchError(
                            "cleanup close marker current ref moved"
                        )
                    close_marker = str(close_marker_value)
                graph_authority = True
                descendants_reader = getattr(self.graph, "descendants", None)
                if descendants_reader is None:
                    errors.append("graph adapter cannot prove descendant-safe cleanup")
                else:
                    descendants = tuple(
                        str(item)
                        for item in descendants_reader(
                            receipt.graph.deployment, receipt.graph.branch
                        )
                    )
                    if descendants:
                        errors.append(
                            "graph branch has live descendants; refusing cascade cleanup: "
                            + ", ".join(descendants)
                        )
        except Exception as exc:  # noqa: BLE001 - return a non-closed report
            errors.append(f"graph ownership check: {exc}")

        # Validate the exact persisted config and Git worktree ownership while
        # graph authority is present.  These checks run before any unlink or
        # worktree removal and prevent a stale receipt from deleting a reused
        # local target.
        if graph_authority and config.exists():
            try:
                config.relative_to(Path(receipt.worktree_path))
                _validate_managed_config(receipt)
            except Exception as exc:  # noqa: BLE001 - return a partial report
                errors.append(f"config ownership check: {exc}")
        if graph_authority and self._git_present(receipt.worktree_path):
            try:
                observed_git = _coerce_git(
                    self.git.inspect(receipt.worktree_path),
                    path=receipt.worktree_path,
                )
                detached = observed_git.branch in {"", "HEAD"}
                if (
                    observed_git.repo_path != receipt.repo_path
                    or observed_git.worktree_path != receipt.worktree_path
                    or (observed_git.branch != receipt.git.branch and not detached)
                ):
                    raise PairMismatchError(
                        "cleanup receipt does not own the Git worktree"
                    )
                if detached:
                    if close_marker is None:
                        raise PairMismatchError(
                            "detached Git worktree has no durable close marker"
                        )
                    if self._exists_git(receipt.repo_path, receipt.git.branch):
                        raise PairMismatchError(
                            "close cleanup refuses a detached worktree with a recreated branch"
                        )
                    if (
                        observed_git.base_commit != receipt.git.base_commit
                        or observed_git.head_commit != receipt.git.head_commit
                        or observed_git.tree != receipt.git.tree
                        or observed_git.dirty != receipt.git.dirty
                        or tuple(observed_git.dirty_paths)
                        != tuple(receipt.git.dirty_paths)
                    ):
                        raise PairMismatchError(
                            "detached Git worktree state changed during close"
                        )
                    detached_git_allowed = True
            except Exception as exc:  # noqa: BLE001 - return a partial report
                errors.append(f"Git ownership check: {exc}")

        # Checkpoint rows are separate from the branch row.  Release only
        # owned, superseded rows and retain the latest handoff checkpoint as
        # the bounded audit anchor.  The adapter performs this under the same
        # branch lock and transaction as its metadata update; an adapter that
        # cannot prove that boundary simply leaves the rows retained.
        if graph_authority and not errors:
            compact = getattr(self.graph, "compact_checkpoints", None)
            if compact is not None:
                retain = ()
                if receipt.cleanup_disposition == "retained":
                    # Retained branches are an explicit audit disposition:
                    # preserve their bounded lineage.  Abandoned branches
                    # release superseded rows below; both outcomes are
                    # reported separately and never imply deletion.
                    retain = retained_checkpoint_ids
                elif observed_graph is not None and observed_graph.metadata.get(
                    "audit_checkpoint", False
                ):
                    latest = observed_graph.checkpoint or receipt.graph.checkpoint
                    retain = (latest,) if latest else ()
                try:
                    compacted_checkpoint_ids, retained_checkpoint_ids = compact(
                        receipt.graph.deployment,
                        receipt.graph.branch,
                        retained_checkpoint_ids,
                        retain,
                    )
                except Exception as exc:  # noqa: BLE001 - no local cleanup on uncertainty
                    errors.append(f"checkpoint compaction: {exc}")

        try:
            if graph_authority and not errors and config.exists():
                config.unlink()
        except OSError as exc:
            errors.append(f"config cleanup: {exc}")
        try:
            if graph_authority and not errors:
                worktree_exists = self._git_present(receipt.worktree_path)
                branch_exists = self._exists_git(receipt.repo_path, receipt.git.branch)
                if worktree_exists:
                    remove_combined = getattr(
                        self.git, "remove_worktree_and_branch_guarded", None
                    )
                    if detached_git_allowed and branch_exists:
                        raise PairProvisionError(
                            "close cleanup saw a recreated branch for a detached worktree"
                        )
                    if detached_git_allowed and not callable(remove_combined):
                        raise PairProvisionError(
                            "close cleanup requires guarded detached Git recovery"
                        )
                    if callable(remove_combined):
                        remove_combined(
                            receipt.repo_path,
                            receipt.worktree_path,
                            receipt.git.branch,
                            expected_commit=receipt.git.head_commit,
                        )
                    else:
                        self.git.remove_worktree(
                            receipt.repo_path, receipt.worktree_path
                        )
                elif branch_exists:
                    raise PairProvisionError(
                        "Git branch has no owned worktree; refusing cleanup"
                    )
                if self._git_present(receipt.worktree_path) or self._exists_git(
                    receipt.repo_path, receipt.git.branch
                ):
                    raise PairProvisionError("Git cleanup was incomplete")
        except Exception as exc:  # noqa: BLE001 - report saga state
            errors.append(f"Git cleanup: {exc}")
        try:
            if (
                graph_authority
                and not errors
                and self._exists_graph(receipt.graph.deployment, receipt.graph.branch)
            ):
                remove_guarded = getattr(self.graph, "remove_branch_guarded", None)
                if remove_guarded is not None:
                    remove_guarded(receipt.graph.deployment, receipt.graph.branch)
                else:
                    # Legacy/fake adapters own a non-cascading remove
                    # primitive.  ProductionGraphWorkspaceAdapter always
                    # supplies the guarded boundary above.
                    self.graph.remove_branch(
                        receipt.graph.deployment, receipt.graph.branch
                    )
        except Exception as exc:  # noqa: BLE001 - report saga state
            errors.append(f"graph cleanup: {exc}")

        config_gone = not config.exists()
        git_gone = not self._git_present(
            receipt.worktree_path
        ) and not self._exists_git(receipt.repo_path, receipt.git.branch)
        try:
            graph_gone = not self._exists_graph(
                receipt.graph.deployment,
                receipt.graph.branch,
            )
        except Exception as exc:  # noqa: BLE001 - unknown graph state is not closed
            graph_gone = False
            errors.append(f"graph cleanup read-back: {exc}")
        complete = config_gone and git_gone and graph_gone and not errors
        disposition: CleanupDisposition = "complete" if complete else "partial"
        return CleanupReport(
            assignment_id=receipt.assignment_id,
            assignment_generation=receipt.assignment_generation,
            git_removed=git_gone,
            graph_removed=graph_gone,
            config_removed=config_gone,
            complete=complete,
            disposition=disposition,
            errors=tuple(errors),
            retained_checkpoint_ids=retained_checkpoint_ids,
            compacted_checkpoint_ids=compacted_checkpoint_ids,
        )

    def compact(self, receipt: PairedWorkspaceReceipt) -> CleanupReport:
        """Release a retired Chronos branch, then perform the final cleanup."""
        retired = (
            receipt
            if receipt.lifecycle_status in {"retired", "abandoned"}
            else self.retire(receipt)
        )
        return self.cleanup(retired)

    def _normalise_request(
        self, request: PairedWorkspaceRequest
    ) -> PairedWorkspaceRequest:
        _validate_assignment(request.assignment_id, request.assignment_generation)
        host = _validate_host(request.authenticated_host)
        if request.graph_branch and request.graph_branch != request.git_branch:
            raise PairMismatchError("Git and graph branch identities must be equal")
        if not request.git_branch:
            raise ValueError("git_branch must be one non-option token")
        _validate_git_branch(request.git_branch)
        if (
            not request.graph_deployment.strip()
            or not request.graph_base_branch.strip()
        ):
            raise ValueError("graph deployment and base branch are required")
        route = _validate_route(request.mcp_route)
        if (
            not request.mcp_command
            or request.mcp_command.startswith("-")
            or any(char.isspace() for char in request.mcp_command)
        ):
            raise ValueError("mcp_command must be one explicit non-option token")
        if request.mcp_command != "okg":
            raise ValueError("mcp_command must be the trusted okg client bridge")
        if request.grant_id is not None and (
            not request.grant_id.strip()
            or any(char.isspace() for char in request.grant_id)
        ):
            raise ValueError("grant_id must be one non-empty token")
        if request.provision_attempt_id is not None and (
            not request.provision_attempt_id.strip()
            or request.provision_attempt_id.startswith("-")
            or any(char.isspace() for char in request.provision_attempt_id)
        ):
            raise ValueError("provision_attempt_id must be one non-option token")
        return replace(
            request,
            authenticated_host=host,
            graph_branch=request.git_branch,
            mcp_route=route,
        )

    @staticmethod
    def _validate_paths(repo_path: str, worktree_path: str, config_path: str) -> None:
        if not Path(repo_path).is_dir():
            raise ValueError(f"repository path is not a directory: {repo_path}")
        if Path(repo_path) == Path(worktree_path):
            raise ValueError("worktree path must differ from repository path")
        try:
            Path(config_path).relative_to(Path(worktree_path))
        except ValueError as exc:
            raise ValueError("MCP config must be inside the worktree") from exc
        relative = Path(config_path).relative_to(Path(worktree_path))
        if relative.parts[:2] != (".okg", "coordination"):
            raise ValueError(
                "MCP config must be the assignment-local .okg/coordination path"
            )
        if Path(worktree_path).exists() and not Path(worktree_path).is_dir():
            raise ValueError("worktree path is not a directory")

    @staticmethod
    def _config_path(worktree_path: str, requested: str | Path) -> Path:
        """Resolve a lexical, assignment-local config target without symlinks."""
        path = _absolute_lexical(requested)
        worktree = Path(worktree_path)
        try:
            relative = path.relative_to(worktree)
        except ValueError as exc:
            raise ValueError("MCP config must be inside the worktree") from exc
        if relative.parts[:2] != (".okg", "coordination"):
            raise ValueError(
                "MCP config must be the assignment-local .okg/coordination path"
            )
        if not relative.name or relative.name in {".", ".."}:
            raise ValueError("MCP config path is not a regular file target")
        return path

    @staticmethod
    def _validate_config_filesystem(worktree_path: str, config_path: Path) -> None:
        """Reject symlinked config paths and parent directories before writes."""
        worktree = Path(worktree_path)
        try:
            relative = config_path.relative_to(worktree)
        except ValueError as exc:
            raise ValueError("MCP config must be inside the worktree") from exc
        current = worktree
        for component in relative.parts:
            current = current / component
            if current.is_symlink():
                raise ValueError("MCP config path must not traverse a symlink")
        if config_path.exists() and not config_path.is_file():
            raise ValueError("MCP config path is not a regular file")

    def _exists_git(self, repository: str, branch: str) -> bool:
        try:
            return bool(self.git.branch_exists(repository, branch))
        except AttributeError:
            return False

    def _exists_graph(self, deployment: str, branch: str) -> bool:
        try:
            return bool(self.graph.branch_exists(deployment, branch))
        except AttributeError:
            try:
                self.graph.inspect(deployment, branch)
            except Exception as exc:  # noqa: BLE001 - adapter compatibility boundary
                if not _is_graph_not_found(exc):
                    raise
                missing = True
            else:
                missing = False
            if missing:
                return False
            return True

    def _git_present(self, path: str) -> bool:
        return Path(path).exists()

    def _git_attempt_owned(
        self,
        request: PairedWorkspaceRequest,
        *,
        repo_path: str,
        worktree_path: str,
        base_commit: str,
        allow_detached: bool = False,
    ) -> bool:
        """Prove a possibly-created worktree belongs to this stage attempt."""
        if not self._git_present(worktree_path):
            return True
        try:
            state = _coerce_git(self.git.inspect(worktree_path), path=worktree_path)
        except Exception as exc:
            raise PairProvisionError(
                "Git rollback ownership inspection failed"
            ) from exc
        return (
            state.repo_path == repo_path
            and state.worktree_path == worktree_path
            and (
                state.branch == request.git_branch
                or (allow_detached and state.branch in {"", "HEAD"})
            )
            and state.base_commit in {"", base_commit}
        )

    def _remove_git_attempt(
        self,
        request: PairedWorkspaceRequest,
        *,
        repo_path: str,
        worktree_path: str,
        expected_commit: str,
    ) -> None:
        """Remove an owned worktree and its exact temporary branch.

        ``git worktree remove`` deliberately leaves the branch ref behind.
        Failed provisioning owns that ref, so compensation must remove it too;
        the combined guarded adapter operation uses an expected-ref
        compare-and-delete while the linked worktree ownership is still
        proven and refuses branches attached to any other worktree.
        """
        remove_combined = getattr(self.git, "remove_worktree_and_branch_guarded", None)
        if callable(remove_combined):
            remove_combined(
                repo_path,
                worktree_path,
                request.git_branch,
                expected_commit=expected_commit,
            )
        else:
            # Legacy/fake adapters remove the branch together with their
            # worktree mapping.  If a branch survives that operation, do not
            # delete it after the ownership proof has become unlinked.
            self.git.remove_worktree(repo_path, worktree_path)
        if self._exists_git(repo_path, request.git_branch):
            raise PairProvisionError(
                "Git branch cleanup was incomplete; graph reservation retained"
            )

    def _graph_reservation_matches(
        self,
        request: PairedWorkspaceRequest,
        *,
        git_state: GitWorkspaceState | None = None,
    ) -> bool:
        """Prove a graph-only branch is a complete reservation for this retry.

        A branch left by a crashed stage is safe to replace only when its
        reservation metadata is complete and agrees with the exact request.
        The invocation marker is intentionally checked only for presence: a
        retry must adopt or replace a prior invocation, not claim its marker.
        """
        if request.provision_attempt_id is None:
            return False
        worktree_path = _resolved(request.worktree_path)
        if git_state is None and self._git_present(worktree_path):
            return False
        if git_state is not None and not self._git_present(worktree_path):
            return False
        graph_branch = request.resolved_graph_branch
        try:
            observed = _coerce_graph(
                self.graph.inspect(request.graph_deployment, graph_branch),
                deployment=request.graph_deployment,
                branch=graph_branch,
            )
            base_state = _coerce_git(
                self.git.inspect(_resolved(request.repo_path)),
                path=_resolved(request.repo_path),
            )
        except Exception as exc:
            raise PairMismatchError("graph reservation inspection failed") from exc

        base_generation = request.graph_base_generation
        base_checkpoint = request.graph_base_checkpoint
        if (
            base_generation is not None
            and base_checkpoint is not None
            and str(base_generation) != str(base_checkpoint)
        ):
            return False
        if base_generation is None and base_checkpoint is not None:
            base_generation = base_checkpoint
        if base_generation is None:
            try:
                latest = self.graph.latest_generation(
                    request.graph_deployment, request.graph_base_branch
                )
            except Exception as exc:
                raise PairMismatchError(
                    "graph base generation inspection failed"
                ) from exc
            base_generation = None if latest is None else str(latest)
        if base_generation is not None and base_checkpoint is None:
            validator = getattr(self.graph, "validate_checkpoint", None)
            if validator is not None:
                try:
                    validator(
                        request.graph_deployment,
                        request.graph_base_branch,
                        str(base_generation),
                    )
                except Exception as exc:
                    raise PairMismatchError(
                        "graph base checkpoint validation failed"
                    ) from exc
                base_checkpoint = str(base_generation)

        base_commit = request.git_base_commit or base_state.head_commit
        try:
            _validate_commit_ref(base_commit)
            resolve_commit = getattr(self.git, "resolve_commit", None)
            if resolve_commit is not None:
                base_commit = str(
                    resolve_commit(_resolved(request.repo_path), base_commit)
                )
        except Exception as exc:
            raise PairMismatchError("Git base commit resolution failed") from exc
        if not _is_full_commit_sha(base_commit):
            return False
        if (
            observed.deployment != request.graph_deployment
            or observed.branch != graph_branch
            or observed.current_ref == ""
            or observed.base_branch != request.graph_base_branch
            or observed.base_generation != base_generation
        ):
            return False

        reservation_request = replace(
            request,
            graph_base_generation=(
                None if base_generation is None else str(base_generation)
            ),
            graph_base_checkpoint=base_checkpoint,
        )
        reservation_git = GitWorkspaceState(
            repo_path=_resolved(request.repo_path),
            worktree_path=worktree_path,
            branch=request.git_branch,
            base_commit=base_commit,
            head_commit="",
            tree="",
            dirty=False,
        )
        reservation_graph = GraphWorkspaceState(
            deployment=request.graph_deployment,
            base_branch=request.graph_base_branch,
            base_generation=base_generation,
            branch=graph_branch,
            current_ref="",
            metadata={
                "provision_attempt_id": request.provision_attempt_id,
                "provision_invocation_id": "reservation",
            },
        )
        metadata = observed.metadata
        expected_metadata = [
            _metadata_for(
                reservation_request,
                git=reservation_git,
                graph=reservation_graph,
                lifecycle_status="staged",
                predecessor_digest=request.predecessor_digest,
                cleanup_disposition="pending",
            )
        ]
        if git_state is not None:
            expected_metadata.append(
                _metadata_for(
                    reservation_request,
                    git=git_state,
                    graph=reservation_graph,
                    lifecycle_status="staged",
                    predecessor_digest=request.predecessor_digest,
                    cleanup_disposition="pending",
                )
            )
        else:
            # A hard crash after Git cleanup can leave the graph with the
            # post-Git binding even though the worktree is gone.  Preserve
            # that recorded state as a recoverable reservation only when its
            # base still matches the exact retry request.
            recorded_base = metadata.get("git_base_commit")
            if recorded_base == base_commit:
                try:
                    recorded_git = GitWorkspaceState(
                        repo_path=_resolved(request.repo_path),
                        worktree_path=worktree_path,
                        branch=request.git_branch,
                        base_commit=str(recorded_base),
                        head_commit=str(metadata["git_head_commit"]),
                        tree=str(metadata["git_tree"]),
                        dirty=bool(metadata["git_dirty"]),
                        dirty_paths=tuple(
                            str(item) for item in metadata["git_dirty_paths"]
                        ),
                    )
                except (KeyError, TypeError):
                    recorded_git = None
                if recorded_git is not None:
                    expected_metadata.append(
                        _metadata_for(
                            reservation_request,
                            git=recorded_git,
                            graph=reservation_graph,
                            lifecycle_status="staged",
                            predecessor_digest=request.predecessor_digest,
                            cleanup_disposition="pending",
                        )
                    )
        invocation_id = metadata.get("provision_invocation_id")
        if not isinstance(invocation_id, str) or not invocation_id.strip():
            return False
        metadata_matches = False
        for expected in expected_metadata:
            candidate_matches = True
            for key, value in expected.items():
                if key == "provision_invocation_id":
                    continue
                if key == "current_ref":
                    if metadata.get(key) not in {"", observed.current_ref}:
                        candidate_matches = False
                        break
                    continue
                if key not in metadata or metadata[key] != value:
                    candidate_matches = False
                    break
            if candidate_matches:
                metadata_matches = True
                break
        if not metadata_matches:
            return False
        # Reuse the same stable binding check used by compensation.  Recovery
        # deliberately does not pass an invocation id, since this is a retry
        # of a reservation created by an earlier process.
        return self._graph_attempt_owned(
            reservation_request,
            graph_branch=graph_branch,
            base_generation=base_generation,
            attempt_nonce=request.provision_attempt_id,
        )

    def _recover_provisional_pair(
        self,
        request: PairedWorkspaceRequest,
    ) -> bool:
        """Compensate an exact, config-incomplete pair before restaging it."""
        if request.provision_attempt_id is None:
            return False
        worktree_path = _resolved(request.worktree_path)
        graph_branch = request.resolved_graph_branch
        try:
            graph = _coerce_graph(
                self.graph.inspect(request.graph_deployment, graph_branch),
                deployment=request.graph_deployment,
                branch=graph_branch,
            )
        except Exception as exc:
            raise PairMismatchError("provisional graph inspection failed") from exc
        base_commit = str(graph.metadata.get("git_base_commit", ""))
        if not _is_full_commit_sha(base_commit):
            return False
        git: GitWorkspaceState | None = None
        if self._git_present(worktree_path):
            try:
                git = _coerce_git(self.git.inspect(worktree_path), path=worktree_path)
            except Exception as exc:
                raise PairMismatchError("provisional Git inspection failed") from exc
            if not self._git_attempt_owned(
                request,
                repo_path=_resolved(request.repo_path),
                worktree_path=worktree_path,
                base_commit=base_commit,
                allow_detached=True,
            ):
                return False
            reservation_git = (
                git
                if git.branch == request.git_branch
                else replace(git, branch=request.git_branch)
            )
            if not self._graph_reservation_matches(request, git_state=reservation_git):
                return False
            # A detached worktree can be the remainder of a crash after its
            # temporary branch was deleted.  The combined Git boundary
            # removes only the exact detached worktree when the ref is gone;
            # if it reappears, the boundary refuses the ambiguous pair.
            if git.branch in {"", "HEAD"} and self._exists_git(
                request.repo_path, request.git_branch
            ):
                return False
        else:
            # A process may die after removing the linked worktree but before
            # deleting its branch ref.  The graph reservation remains the
            # durable identity fence for this exact retry.
            # An existing branch without its owned worktree is deliberately
            # refused: graph metadata cannot authorize an unlinked same-name
            # ref, even when the tip happens to match.
            if self._exists_git(request.repo_path, request.git_branch):
                return False
            if not self._graph_reservation_matches(request):
                return False
        remove_guarded = getattr(self.graph, "remove_branch_guarded", None)
        if git is not None:
            try:
                self._remove_git_attempt(
                    request,
                    repo_path=_resolved(request.repo_path),
                    worktree_path=worktree_path,
                    expected_commit=base_commit,
                )
            except Exception as exc:
                raise PairProvisionError(
                    "provisional Git cleanup failed; graph reservation retained"
                ) from exc
        try:
            if remove_guarded is not None:
                remove_guarded(request.graph_deployment, graph_branch)
            else:
                self.graph.remove_branch(request.graph_deployment, graph_branch)
        except Exception as exc:
            raise PairProvisionError(
                "provisional graph cleanup failed after Git cleanup"
            ) from exc
        return True

    def _graph_attempt_owned(
        self,
        request: PairedWorkspaceRequest,
        *,
        graph_branch: str,
        base_generation: str | None,
        attempt_nonce: str | None = None,
        invocation_id: str | None = None,
    ) -> bool:
        """Prove a possibly-created Chronos branch has this binding."""
        try:
            observed = _coerce_graph(
                self.graph.inspect(request.graph_deployment, graph_branch),
                deployment=request.graph_deployment,
                branch=graph_branch,
            )
        except Exception as exc:
            if not _is_graph_not_found(exc):
                raise PairProvisionError(
                    "graph rollback ownership inspection failed"
                ) from exc
            missing = True
        else:
            missing = False
        if missing:
            return True
        metadata = observed.metadata
        expected = {
            "kind": "okg_paired_workspace",
            "assignment_id": request.assignment_id,
            "assignment_generation": request.assignment_generation,
            "authenticated_host": request.authenticated_host,
            "repo_path": _resolved(request.repo_path),
            "worktree_path": _resolved(request.worktree_path),
            "git_branch": request.git_branch,
            "deployment": request.graph_deployment,
            "base_branch": request.graph_base_branch,
            "base_generation": base_generation,
            "base_checkpoint": request.graph_base_checkpoint,
            "branch": graph_branch,
            "config_path": _resolved(request.config_path)
            if request.config_path
            else "",
            "mcp_server_name": request.mcp_server_name,
            "mcp_route": request.mcp_route,
            "mcp_command": request.mcp_command,
            "grant_id": request.grant_id or "",
        }
        if attempt_nonce is not None:
            expected["provision_attempt_id"] = attempt_nonce
        if invocation_id is not None:
            expected["provision_invocation_id"] = invocation_id
        return (
            observed.deployment == request.graph_deployment
            and observed.branch == graph_branch
            and all(metadata.get(key) == value for key, value in expected.items())
        )

    def _validate_graph_checkpoint(self, receipt: PairedWorkspaceReceipt) -> None:
        validator = getattr(self.graph, "validate_checkpoint", None)
        if validator is None:
            return
        try:
            validator(
                receipt.graph.deployment,
                receipt.graph.branch,
                receipt.graph.checkpoint or "",
            )
        except Exception as exc:  # noqa: BLE001 - adapter-specific validation
            raise PairMismatchError(
                "handoff checkpoint does not belong to its predecessor branch"
            ) from exc

    def _register_managed_config(self, worktree: str, config: str) -> None:
        register = getattr(self.git, "register_managed_path", None)
        if register is None:
            raise PairMismatchError(
                "Git adapter cannot prove the coordination config is ignored"
            )
        try:
            result = register(worktree, config)
        except PairMismatchError:
            raise
        except Exception as exc:  # noqa: BLE001 - surface a fail-closed boundary
            raise PairMismatchError(
                "Git could not persist and verify the coordination config ignore"
            ) from exc
        if result is False:
            raise PairMismatchError(
                "Git could not prove the coordination config is ignored"
            )

    def _require_transition(
        self, receipt: PairedWorkspaceReceipt, allowed: set[LifecycleStatus]
    ) -> None:
        if receipt.lifecycle_status not in allowed:
            raise PairStaleError(
                f"receipt status {receipt.lifecycle_status!r} cannot perform this transition"
            )

    def _read_back(
        self,
        receipt: PairedWorkspaceReceipt,
        *,
        require_clean: bool,
        allow_graph_ref_move: bool = False,
        allow_git_state_move: bool = False,
    ) -> PairedWorkspaceReceipt:
        self._register_managed_config(receipt.worktree_path, receipt.config_path)
        _validate_managed_config(receipt)
        register_base = getattr(self.git, "register_receipt_base", None)
        if register_base is not None:
            register_base(
                receipt.worktree_path,
                receipt.repo_path,
                receipt.git.base_commit,
            )
        git = _coerce_git(
            self.git.inspect(receipt.worktree_path), path=receipt.worktree_path
        )
        if (
            git.repo_path != receipt.repo_path
            or git.worktree_path != receipt.worktree_path
        ):
            raise PairMismatchError("Git read-back path does not match the receipt")
        if git.branch != receipt.git.branch:
            raise PairMismatchError("Git read-back branch does not match the receipt")
        if git.base_commit not in {"", receipt.git.base_commit}:
            raise PairMismatchError(
                "Git read-back base commit does not match the receipt"
            )
        graph = _coerce_graph(
            self.graph.inspect(receipt.graph.deployment, receipt.graph.branch),
            deployment=receipt.graph.deployment,
            branch=receipt.graph.branch,
        )
        if (
            graph.branch != receipt.graph.branch
            or graph.deployment != receipt.graph.deployment
        ):
            raise PairMismatchError(
                "Chronos read-back branch/deployment does not match"
            )
        if (
            graph.base_branch != receipt.graph.base_branch
            or graph.base_generation != receipt.graph.base_generation
        ):
            raise PairMismatchError("Chronos base identity does not match the receipt")
        if (
            graph.assignment_id != receipt.assignment_id
            or graph.assignment_generation != receipt.assignment_generation
        ):
            raise PairMismatchError(
                "Chronos assignment identity does not match the receipt"
            )
        if graph.authenticated_host != receipt.authenticated_host:
            raise PairMismatchError(
                "Chronos authenticated host does not match the receipt"
            )
        _metadata_identity_matches(
            graph.metadata, receipt, allow_state_move=allow_graph_ref_move
        )
        if not allow_graph_ref_move and graph.current_ref != receipt.graph.current_ref:
            raise PairStaleError("Chronos current ref changed; receipt is stale")
        if graph.lifecycle_status != receipt.lifecycle_status:
            raise PairStaleError(
                "Chronos lifecycle metadata no longer matches the receipt"
            )
        # A graph branch may legitimately move while facts are being written;
        # a Git head move is admitted only by the explicit ``refresh`` path.
        if not allow_git_state_move and (
            git.head_commit != receipt.git.head_commit or git.tree != receipt.git.tree
        ):
            raise PairStaleError("Git head/tree changed; refresh before continuing")
        if require_clean and git.dirty:
            raise PairMismatchError("paired Git worktree is dirty")
        return replace(receipt, git=git, graph=graph)

    def _update_graph(
        self,
        receipt: PairedWorkspaceReceipt,
        *,
        lifecycle_status: LifecycleStatus,
        predecessor_digest: str | None,
        cleanup_disposition: CleanupDisposition,
        extra: Mapping[str, Any] | None = None,
    ) -> GraphWorkspaceState:
        request = self._request_from_receipt(receipt)
        graph = replace(
            receipt.graph,
            lifecycle_status=lifecycle_status,
            predecessor_digest=predecessor_digest,
            cleanup_disposition=cleanup_disposition,
        )
        metadata = _metadata_for(
            request,
            git=receipt.git,
            graph=graph,
            lifecycle_status=lifecycle_status,
            predecessor_digest=predecessor_digest,
            cleanup_disposition=cleanup_disposition,
        )
        metadata.update(extra or {})
        guarded = getattr(self.graph, "update_metadata_guarded", None)
        if guarded is not None:
            return _coerce_graph(
                guarded(
                    receipt.graph.deployment,
                    receipt.graph.branch,
                    metadata,
                    expected={
                        "assignment_id": receipt.assignment_id,
                        "assignment_generation": receipt.assignment_generation,
                        "lifecycle_status": receipt.lifecycle_status,
                        "current_ref": receipt.graph.current_ref,
                    },
                ),
                deployment=receipt.graph.deployment,
                branch=receipt.graph.branch,
            )
        return _coerce_graph(
            self.graph.update_metadata(
                receipt.graph.deployment, receipt.graph.branch, metadata
            ),
            deployment=receipt.graph.deployment,
            branch=receipt.graph.branch,
        )

    @staticmethod
    def _request_from_receipt(
        receipt: PairedWorkspaceReceipt,
    ) -> PairedWorkspaceRequest:
        return PairedWorkspaceRequest(
            assignment_id=receipt.assignment_id,
            assignment_generation=receipt.assignment_generation,
            authenticated_host=receipt.authenticated_host,
            repo_path=receipt.repo_path,
            worktree_path=receipt.worktree_path,
            git_branch=receipt.git.branch,
            graph_deployment=receipt.graph.deployment,
            graph_base_branch=receipt.graph.base_branch,
            graph_branch=receipt.graph.branch,
            git_base_commit=receipt.git.base_commit,
            graph_base_generation=receipt.graph.base_generation,
            graph_base_checkpoint=cast(
                str | None,
                receipt.graph.metadata.get("base_checkpoint"),
            ),
            predecessor_digest=receipt.predecessor_digest,
            config_path=receipt.config_path,
            mcp_server_name=receipt.mcp_server_name,
            mcp_route=receipt.mcp_route,
            mcp_command=receipt.mcp_command,
            grant_id=receipt.grant_id,
            provision_attempt_id=cast(
                str | None,
                receipt.graph.metadata.get("provision_attempt_id"),
            ),
        )


# Names that make the role of this module obvious to callers using either the
# ``lifecycle`` or ``orchestrator`` vocabulary.
PairedWorkspaceOrchestrator = PairedWorkspaceLifecycle
PairedWorkspaceHandoff = WorkspaceHandoff
PairedWorkspaceCleanupReport = CleanupReport


class ProductionGitWorkspaceAdapter:
    """Git adapter using ``GitScopeProbe`` reads and safe argv subprocesses."""

    def __init__(self) -> None:
        self._repository_by_worktree: dict[str, str] = {}
        self._managed_paths: dict[str, str] = {}
        self._base_commits: dict[str, str] = {}

    def register_managed_path(self, worktree: str | Path, path: str | Path) -> bool:
        """Persist and prove an ignore rule for the lifecycle-owned MCP file.

        The old implementation only kept this fact in ``_managed_paths`` and
        filtered the path out of an in-memory probe.  That made a restarted
        coordinator report a clean pair even though Git would report the
        file as untracked.  The rule now lives in Git's repository-local
        ``info/exclude``.  It is durable across adapter restarts and leaves
        the user's global excludes and tracked source files untouched.
        """
        worktree_path = Path(worktree).expanduser().resolve()
        worktree_lexical = _absolute_lexical(worktree)
        managed_lexical = _absolute_lexical(path)
        try:
            relative = managed_lexical.relative_to(worktree_lexical)
        except ValueError as exc:
            raise ValueError("managed path must be inside the worktree") from exc
        managed_path = worktree_path / relative
        if relative.parts[:2] != (".okg", "coordination"):
            raise ValueError(
                "managed path must be the assignment-local .okg/coordination path"
            )
        if any(
            (worktree_path / component).is_symlink() for component in relative.parts
        ):
            raise ValueError("managed path must not traverse a symlink")
        if not worktree_path.is_dir():
            raise ValueError("managed path worktree is not a directory")
        tracked = run_git(
            worktree_path,
            ["ls-files", "--stage", "--", relative.as_posix()],
        ).strip()
        if tracked:
            raise PairMismatchError(
                "refusing to manage a tracked coordination config path"
            )
        exclude_path_text = run_git(
            worktree_path, ["rev-parse", "--git-path", "info/exclude"]
        ).strip()
        if not exclude_path_text:
            raise PairMismatchError("Git did not return a local exclude path")
        exclude_path = Path(exclude_path_text)
        if not exclude_path.is_absolute():
            exclude_path = worktree_path / exclude_path
        exclude_path = exclude_path.absolute()
        if exclude_path.is_symlink():
            raise PairMismatchError("Git local exclude path must not be a symlink")
        pattern = "/" + relative.as_posix()
        if exclude_path.exists() and not exclude_path.is_file():
            raise PairMismatchError("Git local exclude path is not a regular file")
        existing = (
            exclude_path.read_text(encoding="utf-8") if exclude_path.exists() else ""
        )
        lines = existing.splitlines()
        if pattern not in lines:
            updated = existing
            if updated and not updated.endswith("\n"):
                updated += "\n"
            updated += pattern + "\n"
            _atomic_write_preserving_mode(exclude_path, updated)
        ignored = git_ignored_paths(worktree_path, {relative.as_posix()})
        if relative.as_posix() not in ignored:
            raise PairMismatchError(
                "Git could not prove the coordination config is ignored"
            )
        self._managed_paths[str(worktree_path)] = str(managed_path)
        return True

    def register_receipt_base(
        self,
        worktree: str | Path,
        repository: str | Path,
        base_commit: str,
    ) -> None:
        """Restore durable base identity after an adapter process restart."""
        worktree_path = Path(worktree).expanduser().resolve()
        repository_path = Path(repository).expanduser().resolve()
        if not worktree_path.is_dir() or not repository_path.is_dir():
            raise PairMismatchError("Git receipt paths are no longer directories")
        discovered = self._discover_repository_root(worktree_path)
        if discovered is None or discovered.resolve() != repository_path:
            raise PairMismatchError("Git receipt repository does not own the worktree")
        try:
            _validate_commit_ref(base_commit)
        except ValueError as exc:
            raise PairMismatchError(
                "Git receipt has no safe durable base commit"
            ) from exc
        if not _is_full_commit_sha(base_commit):
            raise PairMismatchError("Git receipt base is not a full commit SHA")
        try:
            resolved = self.resolve_commit(repository_path, str(base_commit))
        except Exception as exc:  # noqa: BLE001 - receipt must name a live object
            raise PairMismatchError("Git receipt base commit is not valid") from exc
        if resolved.lower() != str(base_commit).lower():
            raise PairMismatchError("Git receipt base commit changed")
        self._repository_by_worktree[str(worktree_path)] = str(repository_path)
        self._base_commits[str(worktree_path)] = resolved

    def inspect(self, path: str | Path) -> GitWorkspaceState:
        worktree = Path(path).expanduser().resolve()
        root = self._repository_by_worktree.get(str(worktree))
        root_path = (
            Path(root) if root is not None else self._discover_repository_root(worktree)
        )
        root = str(root_path) if root_path is not None else None
        if root is None:
            raise PairMismatchError(f"path is not inside a Git worktree: {worktree}")
        managed = self._managed_paths.get(str(worktree))
        managed_rel: str | None = None
        if managed is not None:
            managed_rel = Path(managed).relative_to(worktree).as_posix()

        def scope_filter(rel_path: str) -> bool:
            return managed_rel is None or rel_path != managed_rel

        snapshot = GitScopeProbe(
            repo_root=worktree,
            config={"include_untracked": True},
            # The probe requires at least one fingerprintable emission target;
            # this lifecycle emits no facts, so fingerprint its own stable
            # implementation while reusing the probe's exact Git snapshot.
            emit_targets=(GitScopeProbe,),
            scope_filter=scope_filter,
        ).capture_snapshot()
        return GitWorkspaceState(
            repo_path=root,
            worktree_path=str(worktree),
            branch=str(snapshot["branch"]),
            base_commit=self._base_commits.get(
                str(worktree), str(snapshot["head_sha"])
            ),
            head_commit=str(snapshot["head_sha"]),
            tree=str(snapshot["tree_sha"]),
            dirty=bool(snapshot["dirty_manifest"]),
            dirty_paths=tuple(sorted(str(item) for item in snapshot["dirty_manifest"])),
        )

    def branch_exists(self, repository: str | Path, branch: str) -> bool:
        refs = in_proc_ref_tips(Path(repository).expanduser().resolve())
        return f"refs/heads/{branch}" in refs

    def resolve_commit(self, repository: str | Path, commit_or_ref: str) -> str:
        """Resolve a caller ref to a full immutable commit object ID."""
        repository_path = Path(repository).expanduser().resolve()
        if not repository_path.is_dir():
            raise ValueError("invalid Git repository target")
        _validate_commit_ref(commit_or_ref)
        resolved = run_git(
            repository_path,
            [
                "rev-parse",
                "--verify",
                "--end-of-options",
                f"{commit_or_ref}^{{commit}}",
            ],
        ).strip()
        if not _is_full_commit_sha(resolved):
            raise ValueError("Git did not return a full commit SHA")
        return resolved.lower()

    def create_worktree(
        self,
        repository: str | Path,
        worktree: str | Path,
        branch: str,
        base_commit: str,
    ) -> GitWorkspaceState:
        repository_path = Path(repository).expanduser().resolve()
        # Keep the caller's lexical target so a symlinked worktree or parent
        # cannot be silently resolved into a different authority location.
        worktree_path = _absolute_lexical(worktree)
        _reject_symlinked_path_chain(worktree_path, label="paired worktree")
        if not repository_path.is_dir() or worktree_path == repository_path:
            raise ValueError("invalid repository/worktree target")
        if worktree_path.exists() and any(worktree_path.iterdir()):
            raise ValueError("refusing to create a worktree in a non-empty directory")
        _validate_git_branch(branch)
        resolved_base = self.resolve_commit(repository_path, base_commit)
        # Ask Git's own parser as the final authority; the conservative local
        # rule above catches obvious injection/ref-reuse forms before this
        # command is reached.
        run_git(repository_path, ["check-ref-format", "--branch", branch])
        worktree_path.parent.mkdir(parents=True, exist_ok=True)
        # ``run_git`` constructs [git, -C, repository, *args], preserving every
        # target as one argv element.  Never interpolate a shell command here.
        _reject_symlinked_path_chain(worktree_path.parent, label="paired worktree")
        run_git(
            repository_path,
            ["worktree", "add", "-b", branch, str(worktree_path), resolved_base],
        )
        self._repository_by_worktree[str(worktree_path)] = str(repository_path)
        self._base_commits[str(worktree_path)] = resolved_base
        return self.inspect(worktree_path)

    @staticmethod
    def _discover_repository_root(worktree: Path) -> Path | None:
        """Resolve the main checkout from a linked worktree's ``.git`` file."""
        discovered = in_proc_workdir(worktree)
        git_marker = worktree / ".git"
        if not git_marker.is_file():
            return discovered
        try:
            first_line = git_marker.read_text(encoding="utf-8").splitlines()[0]
            prefix, raw_gitdir = first_line.split(":", 1)
            if prefix.strip().lower() != "gitdir":
                return discovered
            gitdir = Path(raw_gitdir.strip())
            if not gitdir.is_absolute():
                gitdir = (worktree / gitdir).resolve()
            # A linked marker points at <root>/.git/worktrees/<name>.
            candidate = gitdir.parent.parent.parent
            return candidate if (candidate / ".git").is_dir() else discovered
        except (OSError, IndexError, ValueError):
            return discovered

    def remove_worktree(self, repository: str | Path, worktree: str | Path) -> None:
        repository_path = Path(repository).expanduser().resolve()
        worktree_path = _absolute_lexical(worktree)
        _reject_symlinked_path_chain(worktree_path, label="paired worktree")
        if worktree_path == repository_path:
            raise ValueError("refusing to remove the repository root")
        if not worktree_path.exists():
            return
        listing = run_git(repository_path, ["worktree", "list", "--porcelain"])
        registered: set[str] = set()
        for line in listing.splitlines():
            if line.startswith("worktree "):
                registered.add(str(Path(line[9:].strip()).resolve()))
        if str(worktree_path) not in registered:
            raise ValueError("refusing to remove an unregistered worktree target")
        run_git(repository_path, ["worktree", "remove", "--force", str(worktree_path)])
        self._repository_by_worktree.pop(str(worktree_path), None)
        self._base_commits.pop(str(worktree_path), None)
        self._managed_paths.pop(str(worktree_path), None)

    def remove_worktree_and_branch_guarded(
        self,
        repository: str | Path,
        worktree: str | Path,
        branch: str,
        *,
        expected_commit: str,
    ) -> None:
        """Remove one linked worktree and its exact temporary branch.

        The linked worktree is validated before ``worktree remove`` makes the
        branch unlinked.  The branch is then deleted with Git's
        expected-old-value fence; a crash between these commands leaves a
        branch-only artifact that recovery deliberately refuses.
        """
        repository_path = Path(repository).expanduser().resolve()
        worktree_path = _absolute_lexical(worktree)
        _reject_symlinked_path_chain(worktree_path, label="paired worktree")
        if worktree_path == repository_path:
            raise ValueError("refusing to remove the repository root")
        _validate_git_branch(branch)
        if not _is_full_commit_sha(expected_commit):
            raise ValueError("guarded Git branch cleanup requires a full commit SHA")
        branch_ref = f"refs/heads/{branch}"

        def worktree_entry() -> tuple[str | None, str | None, str | None, bool]:
            listing = run_git(repository_path, ["worktree", "list", "--porcelain"])
            registered_path: str | None = None
            registered_head: str | None = None
            registered_branch: str | None = None
            attached_elsewhere = False
            for block in listing.strip().split("\n\n"):
                lines = block.splitlines()
                if not lines or not lines[0].startswith("worktree "):
                    continue
                candidate_path = str(Path(lines[0][9:].strip()).resolve())
                candidate_head: str | None = None
                candidate_branch: str | None = None
                for line in lines[1:]:
                    if line.startswith("HEAD "):
                        candidate_head = line[5:].strip()
                    elif line.startswith("branch "):
                        candidate_branch = line[7:].strip()
                if (
                    candidate_path != str(worktree_path)
                    and candidate_branch == branch_ref
                ):
                    attached_elsewhere = True
                if candidate_path == str(worktree_path):
                    registered_path = candidate_path
                    registered_head = candidate_head
                    registered_branch = candidate_branch
            return (
                registered_path,
                registered_head,
                registered_branch,
                attached_elsewhere,
            )

        (
            registered_path,
            registered_head,
            registered_branch,
            attached_elsewhere,
        ) = worktree_entry()
        if registered_path != str(worktree_path):
            raise ValueError("refusing to remove an unregistered worktree target")
        if attached_elsewhere:
            raise ValueError("refusing to remove a branch attached elsewhere")
        if not self.branch_exists(repository_path, branch):
            if registered_branch is not None:
                raise ValueError("refusing a worktree with a missing branch ref")
            if registered_head != expected_commit.lower():
                raise ValueError("guarded Git cleanup saw a changed worktree head")
            # A crash after the branch CAS but before worktree removal leaves
            # an owned detached checkout.  There is no branch to delete, so
            # remove only the exact worktree reservation.
            run_git(
                repository_path,
                ["worktree", "remove", "--force", str(worktree_path)],
            )
            self._repository_by_worktree.pop(str(worktree_path), None)
            self._base_commits.pop(str(worktree_path), None)
            self._managed_paths.pop(str(worktree_path), None)
            return
        current = self.resolve_commit(repository_path, branch_ref)
        if current.lower() != expected_commit.lower():
            raise ValueError("guarded Git branch cleanup saw a changed branch head")
        if registered_branch != branch_ref:
            # A detached checkout is recoverable only after its branch ref is
            # already gone.  Never relink a same-name ref recreated by an
            # unrelated actor after a detach crash.
            raise ValueError("refusing to delete an unlinked Git branch")
        if registered_head != expected_commit.lower():
            raise ValueError("guarded Git cleanup saw a changed worktree head")
        # Detach first, then re-read the exact path/head before the CAS branch
        # delete.  A crash after this point leaves a detached checkout with
        # the old ref; recovery refuses that ambiguous unlinked branch.  A
        # crash after ref deletion leaves worktree-only state, which recovery
        # can remove from the exact detached path.
        run_git(worktree_path, ["checkout", "--detach", expected_commit.lower()])
        (
            registered_path,
            detached_head,
            detached_branch,
            attached_elsewhere,
        ) = worktree_entry()
        if (
            registered_path != str(worktree_path)
            or attached_elsewhere
            or detached_branch is not None
        ):
            raise ValueError("guarded Git cleanup could not detach worktree")
        if detached_head != expected_commit.lower():
            raise ValueError("guarded Git cleanup saw a changed detached head")
        current = self.resolve_commit(repository_path, branch_ref)
        if current.lower() != expected_commit.lower():
            raise ValueError("guarded Git branch cleanup saw a changed branch head")
        run_git(
            repository_path, ["update-ref", "-d", branch_ref, expected_commit.lower()]
        )
        # Remove the detached checkout only after its branch ref is gone.
        run_git(
            repository_path,
            ["worktree", "remove", "--force", str(worktree_path)],
        )
        self._repository_by_worktree.pop(str(worktree_path), None)
        self._base_commits.pop(str(worktree_path), None)
        self._managed_paths.pop(str(worktree_path), None)

    def is_ancestor(
        self, repository: str | Path, ancestor: str, descendant: str
    ) -> bool:
        # pygit2's in-process graph walk avoids a second subprocess and exposes
        # the exact same object database as the GitScopeProbe reads.
        import pygit2

        repo = pygit2.Repository(str(Path(repository).expanduser().resolve()))
        try:
            return (
                bool(repo.descendant_of(descendant, ancestor)) or ancestor == descendant
            )
        except (KeyError, ValueError, pygit2.GitError):
            return False


class ProductionGraphWorkspaceAdapter:
    """ChronosGraphStore adapter; assignment identity lives in branch metadata."""

    def __init__(self, store: ChronosGraphStore):
        self.store = store

    @classmethod
    def from_store(cls, store: ChronosGraphStore) -> "ProductionGraphWorkspaceAdapter":
        return cls(store)

    @staticmethod
    def _caller_owns_transaction(context: Any) -> bool:
        """Identify an explicitly supplied transaction, not stale adapter state.

        ``db.in_transaction`` is also true after an ordinary autocommit read
        until the connection is finalized.  Treating that state as caller
        ownership leaves advisory locks open across lifecycle calls.  Chronos
        exposes ``autocommit`` as the ownership boundary: only a deliberately
        non-autocommit context with an active transaction is caller-owned.
        """
        return bool(
            context.db.in_transaction and getattr(context, "autocommit", True) is False
        )

    @classmethod
    def _prepare_transaction(cls, context: Any) -> bool:
        """Return whether the caller owns the transaction and clear stale reads."""
        caller_owned = cls._caller_owns_transaction(context)
        if not caller_owned and context.db.in_transaction:
            # A normal Chronos adapter context can have an open transaction
            # from setup/read metadata.  Finish it before taking a lifecycle
            # lock so this operation owns a bounded transaction.
            context.db.commit()
        return caller_owned

    @contextmanager
    def provision_lock(self, deployment: str, branch: str) -> Iterator[None]:
        """Hold a session lock across the complete pair decision and stage.

        Lifecycle adapter methods intentionally commit their own transactions,
        so this uses a session-level advisory lock rather than a transaction
        lock.  A process crash closes the Chronos connection and PostgreSQL
        releases the lock automatically.
        """
        context = self.store.context
        if context.backend_name != "interval" or context.db.dialect != "postgres":
            raise PairProvisionError(
                "paired workspace provisioning lock requires interval/Postgres"
            )
        if context.db.in_transaction:
            context.db.commit()
        lock_key = f"{deployment}:{branch}"
        locked = False
        try:
            context.db.execute(
                "SELECT pg_advisory_lock(hashtext(?), hashtext(?))",
                ["okg.paired_workspace", lock_key],
            )
            context.db.commit()
            locked = True
            yield
        except Exception:
            if not locked and context.db.in_transaction:
                context.db.rollback()
            raise
        finally:
            if locked:
                try:
                    if context.db.in_transaction:
                        context.db.commit()
                    context.db.execute(
                        "SELECT pg_advisory_unlock(hashtext(?), hashtext(?))",
                        ["okg.paired_workspace", lock_key],
                    )
                    context.db.commit()
                except Exception:
                    if context.db.in_transaction:
                        context.db.rollback()
                    raise

    def latest_generation(self, deployment: str, branch: str) -> str | None:
        context = self.store.context
        owns_read_transaction = not self._prepare_transaction(context)
        try:
            generation = self.store.latest_generation(
                deployment_name=deployment,
                branch=branch,
                status="published",
            )
        except Exception:
            if owns_read_transaction and context.db.in_transaction:
                context.db.rollback()
            raise
        if owns_read_transaction and context.db.in_transaction:
            context.db.commit()
        return None if generation is None else str(generation.checkpoint_id)

    def branch_exists(self, deployment: str, branch: str) -> bool:
        try:
            self.inspect(deployment, branch)
        except BranchNotFoundError:
            return False
        return True

    def inspect(self, deployment: str, branch: str) -> GraphWorkspaceState:
        context = self.store.context
        owns_read_transaction = not self._prepare_transaction(context)
        # BranchInfo is cached by the interval backend, but this adapter is
        # also used by independent lifecycle connections.  Never report a
        # stale receipt after another connection has committed a transition.
        backend = getattr(context, "_backend", None)
        cache = getattr(backend, "_branch_info_cache", None)
        if isinstance(cache, dict):
            cache.pop(branch, None)
        try:
            info = context.get_branch(branch)
        except Exception:
            if owns_read_transaction and context.db.in_transaction:
                context.db.rollback()
            raise
        if owns_read_transaction and context.db.in_transaction:
            context.db.commit()
        metadata = dict(info.metadata or {})
        checkpoint = metadata.get("checkpoint")
        checkpoint_ref = metadata.get("checkpoint_ref")
        if checkpoint and checkpoint_ref is None:
            try:
                checkpoint_ref = self.store.get_generation(str(checkpoint)).ref
            except Exception:  # noqa: BLE001 - stale optional metadata is surfaced as None
                checkpoint_ref = None
        # ``get_generation`` above can start a fresh read transaction after
        # the branch lookup.  Finalize that transaction as well, otherwise a
        # later lifecycle operation may accidentally treat it as caller-owned.
        if owns_read_transaction and context.db.in_transaction:
            context.db.commit()
        lifecycle_status = cast(
            LifecycleStatus, str(metadata.get("lifecycle_status", "staged"))
        )
        return GraphWorkspaceState(
            deployment=str(metadata.get("deployment", deployment)),
            base_branch=str(metadata.get("base_branch", "default")),
            base_generation=(
                None
                if metadata.get("base_generation") is None
                else str(metadata["base_generation"])
            ),
            branch=str(metadata.get("branch", branch)),
            current_ref=str(info.current_ref),
            checkpoint=None if checkpoint is None else str(checkpoint),
            checkpoint_ref=None if checkpoint_ref is None else str(checkpoint_ref),
            checkpoint_ids=tuple(
                str(item) for item in metadata.get("checkpoint_ids", ()) if str(item)
            ),
            assignment_id=(
                None
                if metadata.get("assignment_id") is None
                else str(metadata["assignment_id"])
            ),
            assignment_generation=(
                None
                if metadata.get("assignment_generation") is None
                else int(metadata["assignment_generation"])
            ),
            authenticated_host=(
                None
                if metadata.get("authenticated_host") is None
                else str(metadata["authenticated_host"])
            ),
            lifecycle_status=lifecycle_status,
            predecessor_digest=(
                None
                if metadata.get("predecessor_digest") is None
                else str(metadata["predecessor_digest"])
            ),
            cleanup_disposition=cast(
                CleanupDisposition, str(metadata.get("cleanup_disposition", "pending"))
            ),
            metadata=metadata,
        )

    def validate_checkpoint(
        self, deployment: str, predecessor_branch: str, checkpoint: str
    ) -> None:
        """Require an immutable checkpoint owned by this deployment/branch."""
        if not checkpoint:
            raise PairMismatchError("checkpoint identifier is empty")
        context = self.store.context
        owns_read_transaction = not self._prepare_transaction(context)
        try:
            info = self.store.get_generation(checkpoint)
        except Exception:
            if owns_read_transaction and context.db.in_transaction:
                context.db.rollback()
            raise
        if owns_read_transaction and context.db.in_transaction:
            context.db.commit()
        if str(info.branch_id) != predecessor_branch:
            raise PairMismatchError("checkpoint belongs to another Chronos branch")
        if str(getattr(info, "checkpoint_id", checkpoint)) != str(checkpoint):
            raise PairMismatchError("checkpoint identifier is not canonical")
        metadata = dict(info.metadata or {})
        checkpoint_deployment = metadata.get("deployment_name")
        if str(checkpoint_deployment) != deployment:
            raise PairMismatchError("checkpoint belongs to another deployment")
        if metadata.get("branch") != predecessor_branch:
            raise PairMismatchError("checkpoint has no trusted branch binding")
        if metadata.get("kind") not in {"okg_generation", "okg_paired_workspace"}:
            raise PairMismatchError("checkpoint has no trusted generation kind")
        if metadata.get("status") not in {"published", "staged", "checkpointed"}:
            raise PairMismatchError("checkpoint has no trusted lifecycle status")

    def checkpoint_matches_current(
        self,
        deployment: str,
        branch: str,
        checkpoint: str,
        checkpoint_ref: str,
        current_ref: str,
    ) -> bool:
        """Check the continuation segment has no writes after ``checkpoint``.

        Interval Chronos intentionally gives a checkpoint a sibling snapshot
        ref and keeps the branch on a new mutable continuation ref.  Equality
        of those refs would therefore be fabricated.  The bounded check below
        instead requires the exact continuation pointer and verifies that no
        graph row was written by that segment after checkpoint creation.
        """
        context = self.store.context
        owns_read_transaction = not self._prepare_transaction(context)
        try:
            branch_info = self.store.context.get_branch(branch)
            if str(branch_info.current_ref) != str(current_ref):
                if owns_read_transaction and context.db.in_transaction:
                    context.db.commit()
                return False
            checkpoint_info = self.store.get_generation(checkpoint)
            if str(checkpoint_info.branch_id) != branch or str(
                checkpoint_info.ref
            ) != str(checkpoint_ref):
                if owns_read_transaction and context.db.in_transaction:
                    context.db.commit()
                return False
            metadata = dict(checkpoint_info.metadata or {})
            if metadata.get("deployment_name") not in {None, deployment}:
                if owns_read_transaction and context.db.in_transaction:
                    context.db.commit()
                return False
            if str(branch_info.metadata.get("checkpoint")) != str(checkpoint):
                if owns_read_transaction and context.db.in_transaction:
                    context.db.commit()
                return False
            if str(branch_info.metadata.get("checkpoint_ref")) != str(checkpoint_ref):
                if owns_read_transaction and context.db.in_transaction:
                    context.db.commit()
                return False
            result = not self._current_segment_has_graph_writes(str(current_ref))
            if owns_read_transaction and context.db.in_transaction:
                context.db.commit()
            return result
        except (KeyError, BranchNotFoundError):
            if owns_read_transaction and context.db.in_transaction:
                context.db.rollback()
            return False

    def handoff_if_checkpoint_current(
        self,
        deployment: str,
        branch: str,
        checkpoint: str,
        checkpoint_ref: str,
        current_ref: str,
        metadata: Mapping[str, Any],
        *,
        expected: Mapping[str, Any],
    ) -> GraphWorkspaceState:
        """Fence checkpoint cleanliness and handoff under the graph-write lock."""
        context = self.store.context
        if context.backend_name != "interval" or context.db.dialect != "postgres":
            raise PairStaleError("atomic Chronos handoff requires interval/Postgres")
        owns_transaction = not self._prepare_transaction(context)
        try:
            transaction = (
                context.conn.transaction() if owns_transaction else nullcontext()
            )
            with transaction:
                # Graph fact writers use this exact advisory lock before their
                # interval splice.  Keep it held through dirty probing and the
                # lifecycle metadata update so a writer cannot race the gate.
                context.db.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(?), hashtext(?))",
                    ["okg.chronos_graph", branch],
                )
                row = context.db.execute(
                    """
                    SELECT current_segment_id, metadata
                      FROM _chronos_branch_interval_branches
                     WHERE branch_id = ?
                     FOR UPDATE
                    """,
                    (branch,),
                ).fetchone()
                if row is None:
                    raise PairMismatchError("Chronos branch disappeared during handoff")
                raw = row["metadata"]
                observed_metadata = (
                    json.loads(raw or "{}") if isinstance(raw, str) else dict(raw or {})
                )
                for key, value in expected.items():
                    observed = (
                        str(row["current_segment_id"])
                        if key == "current_ref"
                        else observed_metadata.get(key)
                    )
                    if observed != value:
                        raise PairStaleError(f"Chronos handoff lost the {key} fence")
                if (
                    str(observed_metadata.get("checkpoint")) != str(checkpoint)
                    or str(observed_metadata.get("checkpoint_ref"))
                    != str(checkpoint_ref)
                    or str(row["current_segment_id"]) != str(current_ref)
                ):
                    raise PairStaleError("Chronos checkpoint ref is no longer current")
                checkpoint_row = context.db.execute(
                    """
                    SELECT branch_id, segment_id, metadata
                      FROM _chronos_branch_interval_checkpoints
                     WHERE checkpoint_id = ?
                    """,
                    (checkpoint,),
                ).fetchone()
                if checkpoint_row is None:
                    raise PairMismatchError("Chronos handoff checkpoint is missing")
                checkpoint_metadata = checkpoint_row["metadata"]
                checkpoint_metadata = (
                    json.loads(checkpoint_metadata or "{}")
                    if isinstance(checkpoint_metadata, str)
                    else dict(checkpoint_metadata or {})
                )
                if (
                    str(checkpoint_row["branch_id"]) != branch
                    or str(checkpoint_row["segment_id"]) != str(checkpoint_ref)
                    or str(checkpoint_metadata.get("deployment_name")) != deployment
                ):
                    raise PairMismatchError(
                        "Chronos handoff checkpoint has the wrong owner"
                    )
                if self._current_segment_has_graph_writes(str(current_ref)):
                    raise PairStaleError(
                        "graph has writes after the recorded checkpoint"
                    )
                updated = context.db.execute(
                    """
                    UPDATE _chronos_branch_interval_branches
                       SET metadata = ?
                     WHERE branch_id = ?
                       AND current_segment_id = ?
                    """,
                    (dict(metadata), branch, row["current_segment_id"]),
                )
                if updated.rowcount != 1:
                    raise PairStaleError("Chronos branch moved during handoff")
        except PairedWorkspaceError:
            raise
        except Exception as exc:  # noqa: BLE001 - preserve handoff fence boundary
            raise PairStaleError("atomic Chronos handoff failed") from exc
        backend = getattr(context, "_backend", None)
        cache = getattr(backend, "_branch_info_cache", None)
        if isinstance(cache, dict):
            cache.pop(branch, None)
        context._metadata_epoch += 1
        return self.inspect(deployment, branch)

    def _current_segment_has_graph_writes(self, current_ref: str) -> bool:
        """Return whether any registered interval table owns current rows.

        Workspace handoff is a graph-wide cleanliness gate.  Restricting the
        probe to graph_nodes/graph_edges misses registered rollups and future
        mutable tables, so inspect every physical table Chronos registered and
        fail closed if its writer-column schema cannot be read.
        """
        try:
            segment_id = int(current_ref)
        except (TypeError, ValueError):
            return True
        db = self.store.context.db
        physical_tables = {
            str(row["physical_table"])
            for row in db.execute(
                """
                SELECT physical_table
                  FROM _chronos_branch_tables
                 WHERE backend = 'interval'
                """
            ).fetchall()
        }
        schema_columns, _ = db.table_defs("_chronos_branch_table_schema_versions")
        if schema_columns:
            physical_tables.update(
                str(row["physical_table"])
                for row in db.execute(
                    """
                    SELECT physical_table
                      FROM _chronos_branch_table_schema_versions
                     WHERE backend = 'interval'
                    """
                ).fetchall()
            )
        for physical_name in physical_tables:
            physical = quote_physical_table(physical_name)
            found = db.execute(
                f"SELECT 1 FROM {physical} WHERE writer_segment_id = ? LIMIT 1",
                (segment_id,),
            ).fetchone()
            if found is not None:
                return True
        return False

    def create_branch(
        self,
        deployment: str,
        branch: str,
        base_branch: str,
        base_generation: str | None,
        metadata: Mapping[str, Any],
    ) -> GraphWorkspaceState:
        self.store.create_branch(
            branch,
            from_branch=base_branch,
            metadata={
                **dict(metadata),
                "deployment": deployment,
                "deployment_name": deployment,
                "base_branch": base_branch,
                "base_generation": base_generation,
                "branch": branch,
            },
        )
        return self.inspect(deployment, branch)

    def create_branch_from_checkpoint(
        self,
        deployment: str,
        branch: str,
        checkpoint: str,
        metadata: Mapping[str, Any],
    ) -> GraphWorkspaceState:
        """Fork the successor from the exact handoff checkpoint."""
        predecessor_branch = metadata.get("base_branch")
        if not isinstance(predecessor_branch, str) or not predecessor_branch:
            raise PairMismatchError("checkpoint fork requires a predecessor branch")
        self.validate_checkpoint(deployment, predecessor_branch, checkpoint)
        context = self.store.context
        if context.backend_name != "interval" or context.db.dialect != "postgres":
            raise PairProvisionError(
                "atomic checkpoint branch creation requires interval/Postgres"
            )
        backend_create = getattr(
            getattr(context, "_backend", None),
            "_create_branch_from_checkpoint_unlocked",
            None,
        )
        if not callable(backend_create):
            raise PairProvisionError(
                "Chronos adapter does not expose atomic checkpoint branch creation"
            )
        binding = {
            **dict(metadata),
            "deployment": deployment,
            "deployment_name": deployment,
            "branch": branch,
            "base_checkpoint": checkpoint,
        }
        owns_transaction = not self._prepare_transaction(context)
        try:
            transaction = (
                context.conn.transaction() if owns_transaction else nullcontext()
            )
            with transaction:
                context.db.execute(
                    "SELECT pg_advisory_xact_lock(?, ?)",
                    [1326131011, 1],
                )
                backend_create(branch, checkpoint)
                updated = context.db.execute(
                    """
                    UPDATE _chronos_branch_interval_branches
                       SET metadata = ?
                     WHERE branch_id = ?
                    """,
                    (binding, branch),
                )
                if updated.rowcount != 1:
                    raise PairProvisionError(
                        "checkpoint branch binding was not persisted"
                    )
        except PairedWorkspaceError:
            raise
        except Exception as exc:  # noqa: BLE001 - transaction rolls back creation
            raise PairProvisionError(
                "atomic checkpoint branch creation failed"
            ) from exc
        cache = getattr(context._backend, "_branch_info_cache", None)
        if isinstance(cache, dict):
            cache.pop(branch, None)
        context._metadata_epoch += 1
        return self.inspect(deployment, branch)

    def checkpoint(
        self,
        deployment: str,
        branch: str,
        metadata: Mapping[str, Any],
    ) -> GraphWorkspaceState:
        context = self.store.context
        if context.backend_name != "interval" or context.db.dialect != "postgres":
            raise PairMismatchError(
                "atomic Chronos checkpointing requires interval/Postgres"
            )
        backend_create = getattr(
            getattr(context, "_backend", None),
            "_create_checkpoint_unlocked",
            None,
        )
        if not callable(backend_create):
            raise PairMismatchError(
                "Chronos adapter does not expose atomic checkpoint creation"
            )
        existing_checkpoint_ids = list(
            dict.fromkeys(
                str(item) for item in metadata.get("checkpoint_ids", ()) if item
            )
        )
        if len(existing_checkpoint_ids) >= MAX_CHECKPOINT_HISTORY:
            raise PairMismatchError(
                "checkpoint retention bound reached; compact before checkpointing"
            )
        checkpoint = self.store.new_generation_id(
            deployment_name=deployment,
            branch=branch,
        )
        payload = {
            "kind": "okg_generation",
            "deployment_name": deployment,
            "branch": branch,
            "status": "staged",
            "catalog_version_id": None,
            "created_at": datetime.now(timezone.utc).isoformat(),
            **dict(metadata),
        }
        owns_transaction = not self._prepare_transaction(context)
        info = None
        try:
            transaction = (
                context.conn.transaction() if owns_transaction else nullcontext()
            )
            with transaction:
                # The same branch-local writer lock used by graph fact writers
                # serializes the segment split and the binding update.  This
                # keeps a checkpoint from being cut concurrently with writes.
                context.db.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(?), hashtext(?))",
                    ["okg.chronos_graph", branch],
                )
                authoritative = context.db.execute(
                    """
                    SELECT current_segment_id, metadata
                      FROM _chronos_branch_interval_branches
                     WHERE branch_id = ?
                     FOR UPDATE
                    """,
                    (branch,),
                ).fetchone()
                if authoritative is None:
                    raise PairMismatchError(
                        "Chronos branch disappeared before checkpointing"
                    )
                raw_authoritative = authoritative["metadata"]
                authoritative_metadata = (
                    json.loads(raw_authoritative or "{}")
                    if isinstance(raw_authoritative, str)
                    else dict(raw_authoritative or {})
                )
                required = (
                    "assignment_id",
                    "assignment_generation",
                    "base_branch",
                    "base_generation",
                    "base_checkpoint",
                    "branch",
                    "checkpoint",
                    "checkpoint_ref",
                    "checkpoint_ids",
                    "current_ref",
                    "deployment",
                    "lifecycle_status",
                )
                missing = [key for key in required if key not in authoritative_metadata]
                if missing:
                    raise PairMismatchError(
                        "Chronos checkpoint binding is missing " + ", ".join(missing)
                    )
                if authoritative_metadata["assignment_id"] != metadata.get(
                    "assignment_id"
                ) or authoritative_metadata["assignment_generation"] != metadata.get(
                    "assignment_generation"
                ):
                    raise PairStaleError(
                        "Chronos assignment fence changed before checkpointing"
                    )
                if authoritative_metadata["deployment"] != deployment or (
                    authoritative_metadata["branch"] != branch
                ):
                    raise PairMismatchError(
                        "Chronos branch binding changed before checkpointing"
                    )
                if authoritative_metadata["lifecycle_status"] not in {
                    "active",
                    "checkpointed",
                }:
                    raise PairStaleError(
                        "Chronos branch is not checkpointable in its current state"
                    )
                if str(authoritative["current_segment_id"]) != str(
                    metadata.get("current_ref")
                ) or str(authoritative_metadata["current_ref"]) != str(
                    metadata.get("current_ref")
                ):
                    raise PairStaleError(
                        "Chronos current ref changed before checkpointing"
                    )
                authoritative_ids = tuple(
                    dict.fromkeys(
                        str(item)
                        for item in authoritative_metadata["checkpoint_ids"]
                        if item
                    )
                )
                requested_ids = tuple(
                    dict.fromkeys(
                        str(item) for item in metadata.get("checkpoint_ids", ()) if item
                    )
                )
                if authoritative_ids != requested_ids:
                    raise PairStaleError(
                        "Chronos checkpoint lineage changed before checkpointing"
                    )
                if len(authoritative_ids) >= MAX_CHECKPOINT_HISTORY:
                    raise PairMismatchError(
                        "checkpoint retention bound reached; compact before checkpointing"
                    )
                authoritative_checkpoint = authoritative_metadata["checkpoint"]
                authoritative_checkpoint_ref = authoritative_metadata["checkpoint_ref"]
                if authoritative_checkpoint is None:
                    if (
                        authoritative_checkpoint_ref not in {None, ""}
                        or authoritative_ids
                    ):
                        raise PairMismatchError(
                            "Chronos checkpoint lineage is internally inconsistent"
                        )
                elif (
                    not authoritative_checkpoint_ref
                    or str(authoritative_checkpoint) not in authoritative_ids
                ):
                    raise PairMismatchError(
                        "Chronos checkpoint lineage is internally inconsistent"
                    )
                for key in (
                    "base_branch",
                    "base_generation",
                    "base_checkpoint",
                    "checkpoint",
                    "checkpoint_ref",
                ):
                    if authoritative_metadata[key] != metadata.get(key):
                        raise PairStaleError(
                            f"Chronos {key} changed before checkpointing"
                        )
                binding_keys = {
                    "kind",
                    "assignment_id",
                    "assignment_generation",
                    "authenticated_host",
                    "repo_path",
                    "worktree_path",
                    "git_branch",
                    "git_base_commit",
                    "git_head_commit",
                    "git_tree",
                    "git_dirty",
                    "git_dirty_paths",
                    "deployment",
                    "base_branch",
                    "base_generation",
                    "base_checkpoint",
                    "branch",
                    "current_ref",
                    "checkpoint",
                    "checkpoint_ref",
                    "checkpoint_ids",
                    "compacted_checkpoint_ids",
                    "audit_checkpoint",
                    "provision_attempt_id",
                    "lifecycle_status",
                    "predecessor_digest",
                    "cleanup_disposition",
                    "config_path",
                    "mcp_server_name",
                    "mcp_route",
                    "mcp_command",
                    "grant_id",
                }
                checkpoint_payload = dict(payload)
                checkpoint_payload.update(
                    {
                        key: value
                        for key, value in authoritative_metadata.items()
                        if key not in binding_keys
                    }
                )
                checkpoint_payload.update(
                    {
                        key: value
                        for key, value in metadata.items()
                        if key in binding_keys
                    }
                )
                info = backend_create(checkpoint, branch, checkpoint_payload)
                branch_row = context.db.execute(
                    """
                    SELECT current_segment_id
                      FROM _chronos_branch_interval_branches
                     WHERE branch_id = ?
                    """,
                    (branch,),
                ).fetchone()
                if branch_row is None:
                    raise PairMismatchError(
                        "Chronos branch disappeared during checkpoint"
                    )
                checkpoint_ids = list(authoritative_ids)
                if checkpoint not in checkpoint_ids:
                    checkpoint_ids.append(checkpoint)
                # Preserve server-owned metadata added by other graph layers;
                # the lifecycle binding is merged over the locked authoritative
                # row only after all fence checks above pass.
                merged = dict(authoritative_metadata)
                merged.update(
                    {
                        key: value
                        for key, value in metadata.items()
                        if key in binding_keys
                    }
                )
                merged.update(
                    {
                        "checkpoint": checkpoint,
                        "checkpoint_ref": info.ref,
                        "current_ref": str(branch_row["current_segment_id"]),
                        "checkpoint_ids": checkpoint_ids,
                        "lifecycle_status": "checkpointed",
                    }
                )
                updated = context.db.execute(
                    """
                    UPDATE _chronos_branch_interval_branches
                       SET metadata = ?
                     WHERE branch_id = ?
                    """,
                    (merged, branch),
                )
                if updated.rowcount != 1:
                    raise PairMismatchError(
                        "Chronos checkpoint binding was not persisted"
                    )
        except PairCheckpointError:
            raise
        except Exception as exc:  # noqa: BLE001 - preserve owned checkpoint id
            raise PairCheckpointError(
                "Chronos checkpoint creation or binding failed",
                checkpoint=str(checkpoint),
                checkpoint_ref=str(getattr(locals().get("info"), "ref", "")),
                partial=not owns_transaction,
            ) from exc
        cache = getattr(context._backend, "_branch_info_cache", None)
        if isinstance(cache, dict):
            cache.pop(branch, None)
        context._metadata_epoch += 1
        return self.inspect(deployment, branch)

    def update_metadata(
        self, deployment: str, branch: str, metadata: Mapping[str, Any]
    ) -> GraphWorkspaceState:
        self.store.context.update_branch_metadata(branch, dict(metadata))
        return self.inspect(deployment, branch)

    def update_metadata_guarded(
        self,
        deployment: str,
        branch: str,
        metadata: Mapping[str, Any],
        *,
        expected: Mapping[str, Any],
    ) -> GraphWorkspaceState:
        """Atomically compare assignment fences and replace interval metadata."""
        context = self.store.context
        if context.backend_name != "interval" or context.db.dialect != "postgres":
            # The SQL shape below is intentionally not approximated for other
            # backends.  A non-CAS metadata write would silently defeat the
            # assignment-generation fence.
            raise PairStaleError(
                "Chronos guarded metadata updates require interval/Postgres"
            )
        owns_transaction = not self._prepare_transaction(context)
        try:
            transaction = (
                context.conn.transaction() if owns_transaction else nullcontext()
            )
            with transaction:
                # Freeze transitions must serialize with every graph-fact
                # writer.  Use the same transaction-scoped branch lock as
                # ``writes.py`` before reading the fence or changing status.
                context.db.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(?), hashtext(?))",
                    ["okg.chronos_graph", branch],
                )
                row = context.db.execute(
                    """
                    SELECT current_segment_id, metadata
                      FROM _chronos_branch_interval_branches
                     WHERE branch_id = ?
                     FOR UPDATE
                    """,
                    (branch,),
                ).fetchone()
                if row is None:
                    raise PairMismatchError("Chronos branch disappeared during update")
                raw_metadata = row["metadata"]
                if isinstance(raw_metadata, str):
                    observed_metadata = json.loads(raw_metadata or "{}")
                else:
                    observed_metadata = dict(raw_metadata or {})
                for key, value in expected.items():
                    observed = (
                        str(row["current_segment_id"])
                        if key == "current_ref"
                        else observed_metadata.get(key)
                    )
                    if observed != value:
                        raise PairStaleError(
                            f"Chronos guarded update lost the {key} fence"
                        )
                updated = context.db.execute(
                    """
                    UPDATE _chronos_branch_interval_branches
                       SET metadata = ?
                     WHERE branch_id = ?
                       AND current_segment_id = ?
                    """,
                    (
                        dict(metadata),
                        branch,
                        row["current_segment_id"],
                    ),
                )
                if updated.rowcount != 1:
                    raise PairStaleError(
                        "Chronos branch moved before guarded metadata update"
                    )
        except PairedWorkspaceError:
            raise
        except Exception as exc:  # noqa: BLE001 - preserve the fence boundary
            raise PairStaleError("Chronos guarded metadata update failed") from exc
        backend = getattr(context, "_backend", None)
        cache = getattr(backend, "_branch_info_cache", None)
        if isinstance(cache, dict):
            cache.pop(branch, None)
        context._metadata_epoch += 1
        return self.inspect(deployment, branch)

    def descendants(self, deployment: str, branch: str) -> tuple[str, ...]:
        """Return every branch below ``branch`` before a cascade is allowed."""
        context = self.store.context
        if context.backend_name != "interval" or context.db.dialect != "postgres":
            raise PairMismatchError(
                "Chronos descendant inspection requires interval/Postgres"
            )
        owns_transaction = not self._prepare_transaction(context)
        try:
            row = context.db.execute(
                "SELECT 1 FROM _chronos_branch_interval_branches WHERE branch_id = ?",
                (branch,),
            ).fetchone()
            if row is None:
                raise PairMismatchError(
                    "Chronos branch disappeared during descendant inspection"
                )
            rows = context.db.execute(
                """
                WITH RECURSIVE descendants(branch_id) AS (
                    SELECT branch_id
                      FROM _chronos_branch_interval_branches
                     WHERE parent_branch_id = ?
                    UNION ALL
                    SELECT child.branch_id
                      FROM _chronos_branch_interval_branches AS child
                      JOIN descendants AS parent
                        ON child.parent_branch_id = parent.branch_id
                )
                SELECT branch_id FROM descendants ORDER BY branch_id
                """,
                (branch,),
            ).fetchall()
            result = tuple(str(item["branch_id"]) for item in rows)
            if owns_transaction and context.db.in_transaction:
                context.db.commit()
            return result
        except Exception:
            if owns_transaction and context.db.in_transaction:
                context.db.rollback()
            raise

    def remove_branch_guarded(self, deployment: str, branch: str) -> None:
        """Delete only a childless branch under the branch-creation lock."""
        context = self.store.context
        if context.backend_name != "interval" or context.db.dialect != "postgres":
            raise PairMismatchError(
                "guarded Chronos branch removal requires interval/Postgres"
            )
        backend = getattr(context, "_backend", None)
        remove = getattr(backend, "delete_branch", None)
        if not callable(remove):
            raise PairMismatchError("Chronos adapter lacks guarded branch removal")
        owns_transaction = not self._prepare_transaction(context)
        try:
            transaction = (
                context.conn.transaction() if owns_transaction else nullcontext()
            )
            with transaction:
                # create_branch_from_checkpoint serializes with this lock, while
                # ordinary forks lock the parent row below.  Holding both makes
                # the child probe and non-cascading delete one operation.
                context.db.execute(
                    "SELECT pg_advisory_xact_lock(?, ?)",
                    [1326131011, 1],
                )
                row = context.db.execute(
                    """
                    SELECT child_count
                      FROM _chronos_branch_interval_branches
                     WHERE branch_id = ?
                     FOR UPDATE
                    """,
                    (branch,),
                ).fetchone()
                if row is None:
                    raise PairMismatchError("Chronos branch disappeared during cleanup")
                child = context.db.execute(
                    """
                    SELECT branch_id
                      FROM _chronos_branch_interval_branches
                     WHERE parent_branch_id = ?
                     LIMIT 1
                    """,
                    (branch,),
                ).fetchone()
                if child is not None or int(row["child_count"]) != 0:
                    raise PairStaleError(
                        "Chronos branch has live descendants; refusing cascade deletion"
                    )
                remove(branch)
        except PairedWorkspaceError:
            raise
        except Exception as exc:  # noqa: BLE001 - transaction rollback boundary
            raise PairStaleError("guarded Chronos branch cleanup failed") from exc
        cache = getattr(backend, "_branch_info_cache", None)
        if isinstance(cache, dict):
            cache.pop(branch, None)
        context._metadata_epoch += 1

    def compact_checkpoints(
        self,
        deployment: str,
        branch: str,
        checkpoint_ids: tuple[str, ...],
        retain_checkpoint_ids: tuple[str, ...],
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """Release owned checkpoint rows in one locked, auditable transaction."""
        context = self.store.context
        if context.backend_name != "interval" or context.db.dialect != "postgres":
            raise PairMismatchError(
                "Chronos checkpoint compaction requires interval/Postgres"
            )
        owned = tuple(dict.fromkeys(str(item) for item in checkpoint_ids if item))
        retain = tuple(
            dict.fromkeys(str(item) for item in retain_checkpoint_ids if item)
        )
        if any(item not in owned for item in retain):
            raise PairMismatchError("checkpoint retention names an unowned checkpoint")
        if len(owned) > MAX_CHECKPOINT_HISTORY:
            raise PairMismatchError("checkpoint lineage exceeds the retention bound")
        owns_transaction = not self._prepare_transaction(context)
        deleted: list[str] = []
        retained: list[str] = []
        try:
            transaction = (
                context.conn.transaction() if owns_transaction else nullcontext()
            )
            with transaction:
                # Coordinate with checkpoint-fork creation as well as ordinary
                # parent-row forks before deleting any checkpoint base.
                context.db.execute(
                    "SELECT pg_advisory_xact_lock(?, ?)",
                    [1326131011, 1],
                )
                context.db.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(?), hashtext(?))",
                    ["okg.chronos_graph", branch],
                )
                branch_row = context.db.execute(
                    """
                    SELECT metadata
                      FROM _chronos_branch_interval_branches
                     WHERE branch_id = ?
                     FOR UPDATE
                    """,
                    (branch,),
                ).fetchone()
                if branch_row is None:
                    raise PairMismatchError(
                        "Chronos branch disappeared during compaction"
                    )
                raw_branch_metadata = branch_row["metadata"]
                branch_metadata = (
                    json.loads(raw_branch_metadata or "{}")
                    if isinstance(raw_branch_metadata, str)
                    else dict(raw_branch_metadata or {})
                )
                prior_compacted = tuple(
                    dict.fromkeys(
                        str(item)
                        for item in branch_metadata.get("compacted_checkpoint_ids", ())
                        if item
                    )
                )
                for checkpoint in owned:
                    row = context.db.execute(
                        """
                        SELECT branch_id, metadata
                          FROM _chronos_branch_interval_checkpoints
                         WHERE checkpoint_id = ?
                        """,
                        (checkpoint,),
                    ).fetchone()
                    if row is None:
                        if checkpoint in prior_compacted:
                            continue
                        raise PairMismatchError(
                            f"owned Chronos checkpoint {checkpoint!r} is missing"
                        )
                    checkpoint_metadata = row["metadata"]
                    checkpoint_metadata = (
                        json.loads(checkpoint_metadata or "{}")
                        if isinstance(checkpoint_metadata, str)
                        else dict(checkpoint_metadata or {})
                    )
                    if (
                        str(row["branch_id"]) != branch
                        or str(checkpoint_metadata.get("deployment_name")) != deployment
                        or checkpoint_metadata.get("assignment_id")
                        != branch_metadata.get("assignment_id")
                        or checkpoint_metadata.get("assignment_generation")
                        != branch_metadata.get("assignment_generation")
                    ):
                        raise PairMismatchError(
                            f"Chronos checkpoint {checkpoint!r} is not owned by this pair"
                        )
                    if checkpoint in retain:
                        retained.append(checkpoint)
                        continue
                    removed = context.db.execute(
                        """
                        DELETE FROM _chronos_branch_interval_checkpoints
                         WHERE checkpoint_id = ? AND branch_id = ?
                        """,
                        (checkpoint, branch),
                    )
                    if removed.rowcount != 1:
                        raise PairStaleError(
                            f"Chronos checkpoint {checkpoint!r} moved during compaction"
                        )
                    deleted.append(checkpoint)
                if deleted:
                    all_compacted = list(prior_compacted)
                    all_compacted.extend(
                        item for item in deleted if item not in all_compacted
                    )
                    branch_metadata["compacted_checkpoint_ids"] = all_compacted
                    updated = context.db.execute(
                        """
                        UPDATE _chronos_branch_interval_branches
                           SET metadata = ?
                         WHERE branch_id = ?
                        """,
                        (branch_metadata, branch),
                    )
                    if updated.rowcount != 1:
                        raise PairStaleError(
                            "Chronos branch binding moved during compaction"
                        )
        except PairedWorkspaceError:
            raise
        except Exception as exc:  # noqa: BLE001 - transaction rollback boundary
            raise PairStaleError("Chronos checkpoint compaction failed") from exc
        backend = getattr(context, "_backend", None)
        cache = getattr(backend, "_branch_info_cache", None)
        if isinstance(cache, dict):
            cache.pop(branch, None)
        context._metadata_epoch += 1
        return tuple(deleted), tuple(retained)

    def remove_branch(self, deployment: str, branch: str) -> None:
        self.store.delete_branch(branch)


__all__ = [
    "CleanupDisposition",
    "CleanupReport",
    "GitWorkspaceAdapter",
    "GitWorkspaceState",
    "GraphWorkspaceAdapter",
    "GraphWorkspaceState",
    "PairMismatchError",
    "PairProvisionError",
    "PairStaleError",
    "PairedWorkspaceCleanupReport",
    "PairedWorkspaceError",
    "PairedWorkspaceHandoff",
    "PairedWorkspaceLifecycle",
    "PairedWorkspaceOrchestrator",
    "PairedWorkspaceReceipt",
    "PairedWorkspaceRequest",
    "ProductionGitWorkspaceAdapter",
    "ProductionGraphWorkspaceAdapter",
    "WorkspaceHandoff",
    "default_workspace_branch",
    "receipt_digest",
]
