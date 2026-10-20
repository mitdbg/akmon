"""Server-owned target authorities for the coordination forced-command route.

The SSH process is the only process which has both the native coordination
ledger and the target checkout/graph.  This module composes those two halves
without adding a wire operation or another persistence system.  All values
which select a target come from :class:`CoordinationServerRoute`; request
fields are used only as native idempotency inputs.

The classes in this module intentionally keep the native writer as the source
of assignment authority.  Git, Chronos, and PACT are read through fresh
adapters for every attestation so an observer cannot accidentally turn a
cached receipt into live state.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import os
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.agent_runtimes.assignment_close import (
    AssignmentCloseError,
    AssignmentCloseOrchestrator,
    AssignmentClosePlan,
    CleanupProof,
    close_pair_proof,
    close_pair_proof_digest,
    cleanup_proof_digest,
)
from okg.substrate.library.agent_runtimes.assignment_handshake import (
    AssignmentHandshakeError,
    AssignmentStartOrchestrator,
    AssignmentStartPlan,
    pair_proof,
    pair_proof_digest,
)
from okg.substrate.library.agent_runtimes.paired_workspace import (
    GraphWorkspaceState,
    GitWorkspaceState,
    PairedWorkspaceError,
    PairedWorkspaceLifecycle,
    PairedWorkspaceReceipt,
    PairedWorkspaceRequest,
    ProductionGitWorkspaceAdapter,
    ProductionGraphWorkspaceAdapter,
    _metadata_identity_matches,
    _validate_managed_config,
)


_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_TOKEN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@+-]{0,511}$")
_COMMIT = re.compile(r"^[0-9a-fA-F]{40}(?:[0-9a-fA-F]{24})?$")
_PACT_MAX_BYTES = 256 * 1024
_CLOSE_BLOCKERS = frozenset(
    {"pact_incomplete", "git_dirty", "github_incomplete", "checkpoint_stale"}
)
_CALLBACKS = (
    "pair_state_verifier",
    "close_readiness_verifier",
    "cleanup_state_verifier",
    "workspace_observer",
    "pact_observer",
    "target_observer",
)


class ProductionAuthorityError(RuntimeError):
    """A fail-closed composition or target observation refusal."""

    def __init__(self, reason_code: str = "writer_unavailable") -> None:
        reason = str(reason_code or "writer_unavailable").strip().lower()
        if not re.fullmatch(r"[a-z0-9_]{1,96}", reason):
            reason = "writer_unavailable"
        self.reason_code = reason
        super().__init__(reason)


# Descriptive compatibility spelling for callers which treat this as a
# builder/authority failure rather than a generic runtime error.
ProductionAuthorityUnavailable = ProductionAuthorityError


def _text(value: Any, *, field: str = "value") -> str:
    if not isinstance(value, str):
        raise ProductionAuthorityError("writer_unavailable")
    value = value.strip()
    if not value or len(value) > 512 or any(c in value for c in "\x00\r\n"):
        raise ProductionAuthorityError("writer_unavailable")
    return value


def _token(value: Any, *, field: str = "value") -> str:
    value = _text(value, field=field)
    if not _TOKEN.fullmatch(value):
        raise ProductionAuthorityError("writer_unavailable")
    return value


def _digest(value: Any) -> str:
    value = _text(value)
    if not _SHA256.fullmatch(value.lower()):
        raise ProductionAuthorityError("writer_unavailable")
    return value.lower()


def _mapping(value: Any) -> Mapping[str, Any]:
    if hasattr(value, "as_dict"):
        value = value.as_dict()
    if not isinstance(value, Mapping):
        raise ProductionAuthorityError("writer_unavailable")
    return value


def _canonical_digest(value: Any) -> str:
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
            default=str,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise ProductionAuthorityError("writer_unavailable") from exc
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _route_values(route: Any) -> tuple[str, str, Path, str, str]:
    """Copy only the five root-selected route values into an authority."""

    try:
        selected = _token(getattr(route, "route"), field="route")
        deployment = _token(getattr(route, "deployment_name"), field="deployment")
        root = Path(getattr(route, "repo_root"))
        dsn = _text(getattr(route, "dsn"), field="dsn")
        expected = _text(getattr(route, "expected_git_commit"), field="commit").lower()
    except (AttributeError, TypeError, ValueError) as exc:
        raise ProductionAuthorityError("writer_unavailable") from exc
    if not root.is_absolute() or not _COMMIT.fullmatch(expected):
        raise ProductionAuthorityError("writer_unavailable")
    try:
        root = root.resolve(strict=True)
    except OSError as exc:
        raise ProductionAuthorityError("writer_unavailable") from exc
    if not root.is_dir():
        raise ProductionAuthorityError("writer_unavailable")
    if "${" in dsn or any(c in dsn for c in "\x00\r\n"):
        raise ProductionAuthorityError("writer_unavailable")
    return selected, deployment, root, expected, dsn


def _assignment_attrs(assignment: Mapping[str, Any]) -> Mapping[str, Any]:
    attrs = assignment.get("attrs")
    return attrs if isinstance(attrs, Mapping) else {}


@dataclass(frozen=True, slots=True)
class _AuthorityPlan:
    """An exact start plan plus the target route binding."""

    plan: AssignmentStartPlan
    route: str
    deployment: str
    repo_root: Path
    expected_git_commit: str

    def request(self) -> PairedWorkspaceRequest:
        return self.plan.paired_request()


class _ManagedLifecycle:
    """Lifecycle proxy which owns one fresh graph connection."""

    def __init__(
        self,
        lifecycle: PairedWorkspaceLifecycle,
        store: Any,
    ) -> None:
        self._lifecycle = lifecycle
        self._store = store
        self._closed = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self._lifecycle, name)

    @property
    def git(self) -> Any:
        return self._lifecycle.git

    @property
    def graph(self) -> Any:
        return self._lifecycle.graph

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        close = getattr(self._store, "close", None)
        if callable(close):
            try:
                close()
            except Exception:
                # A best-effort close must not replace an already-sanitized
                # native result or prevent later opened resources from being
                # closed by the orchestrator.
                pass

    def __enter__(self) -> "_ManagedLifecycle":
        return self

    def __exit__(self, _type: Any, _value: Any, _traceback: Any) -> None:
        self.close()


class ProductionTargetObserver:
    """Fresh live Git/graph/PACT observer bound to one opaque route.

    The observer intentionally does not cache an assignment or a prior
    observation.  Native code supplies the locked assignment row for every
    invocation and this object reads the exact worktree and graph branch named
    by that row's finalized start plan.
    """

    def __init__(self, authority: "ProductionAuthority") -> None:
        self._authority = authority
        self.target_route = authority.route
        self.target_route_digest = authority.target_route_digest
        self.target_route_generation = 1
        # Native read-model integrations occasionally retain a named observer
        # object.  Both names point to this bound, live observer, never to a
        # caller-provided snapshot.
        self.live_observation = self
        self.bound_observation = self

    def observe(self, assignment: Mapping[str, Any]) -> dict[str, Any]:
        assignment = _mapping(assignment)
        bootstrap = self._authority.is_bootstrap_assignment(assignment)
        plan = (
            self._authority.bootstrap_plan_from_assignment(assignment)
            if bootstrap
            else self._authority.plan_from_assignment(assignment)
        )
        if not bootstrap:
            self._authority.validate_assignment_binding(assignment, plan)
        try:
            receipt = self._authority.observe_pair(plan, assignment)
            pact = self._authority.observe_pact(assignment, plan)
            workspace = {
                "dirty": bool(receipt.git.dirty),
                "branch": receipt.git.branch,
                "head_commit": receipt.git.head_commit,
                "status": receipt.lifecycle_status,
                "fresh": True,
                # Native status preserves these finite fields in its target
                # projection.  A checkpointed/handed-off lifecycle is valid
                # only after observe_pair has proved its checkpoint current.
                "checkpoint_fresh": True,
                "checkpoint_stale": False,
                "graph": {
                    "deployment": receipt.graph.deployment,
                    "branch": receipt.graph.branch,
                    "current_ref": receipt.graph.current_ref,
                    "checkpoint": receipt.graph.checkpoint,
                    "checkpoint_ref": receipt.graph.checkpoint_ref,
                    "checkpoint_ids": list(receipt.graph.checkpoint_ids),
                    "lifecycle_status": receipt.graph.lifecycle_status,
                    "base_generation": receipt.graph.base_generation,
                },
            }
            attrs = _assignment_attrs(assignment)
            route_generation = (
                self.target_route_generation
                if bootstrap
                else attrs.get("target_route_generation")
            )
            route_digest = (
                self.target_route_digest
                if bootstrap
                else _text(attrs.get("target_route_digest"), field="digest")
            )
            if (
                isinstance(route_generation, bool)
                or not isinstance(route_generation, int)
                or route_generation < 1
            ):
                raise ProductionAuthorityError("target_observer_unavailable")
            return {
                "trusted": True,
                "authority": "server",
                "assignment_id": plan.plan.assignment_id,
                "assignment_generation": plan.plan.assignment_generation,
                "target_route": self.target_route,
                "target_route_generation": route_generation,
                "target_route_digest": route_digest,
                "binding_authority": "bootstrap-chief" if bootstrap else "enrollment",
                "workspace": workspace,
                "pact": pact,
            }
        except ProductionAuthorityError as exc:
            if exc.reason_code == "target_observer_unavailable":
                raise
            raise ProductionAuthorityError("target_observer_unavailable") from exc
        except Exception as exc:  # noqa: BLE001 - observer is fail closed
            raise ProductionAuthorityError("target_observer_unavailable") from exc

    __call__ = observe


# Explicit aliases make composition readable to native read-model callers.
ProductionWorkspaceObserver = ProductionTargetObserver
ProductionPactObserver = ProductionTargetObserver


class ProductionAuthority:
    """One server-selected Git/graph/PACT authority composition."""

    def __init__(
        self,
        *,
        route: str,
        deployment_name: str,
        repo_root: Path,
        expected_git_commit: str,
        dsn: str,
        writer_cls: type[Any],
        graph_store_factory: Callable[[str], Any] | None = None,
        git_adapter_factory: Callable[[], Any] | None = None,
        pact_event_loader: Callable[[str], list[Mapping[str, Any]]] | None = None,
        external_github_reconciler: Callable[..., Any] | None = None,
    ) -> None:
        self.route = _token(route, field="route")
        self.deployment_name = _token(deployment_name, field="deployment")
        self.repo_root = Path(repo_root).resolve(strict=True)
        self.expected_git_commit = _text(expected_git_commit).lower()
        self.dsn = _text(dsn, field="dsn")
        if not _COMMIT.fullmatch(self.expected_git_commit):
            raise ProductionAuthorityError("writer_unavailable")
        if not isinstance(writer_cls, type):
            raise ProductionAuthorityError("writer_unavailable")
        self.writer_cls = writer_cls
        self.graph_store_factory = graph_store_factory or self._connect_graph
        self.git_adapter_factory = git_adapter_factory or ProductionGitWorkspaceAdapter
        self.pact_event_loader = pact_event_loader
        self.external_github_reconciler = external_github_reconciler
        self.target_route_digest = _canonical_digest(
            {
                "route": self.route,
                "deployment_name": self.deployment_name,
                "repo_root": str(self.repo_root),
                "expected_git_commit": self.expected_git_commit,
                "dsn": self.dsn,
            }
        )
        self.target_observer = ProductionTargetObserver(self)
        self.workspace_observer = self.target_observer
        self.pact_observer = self.target_observer
        self.writer = self._construct_writer()

    @classmethod
    def from_route(
        cls,
        route: Any,
        writer_cls: type[Any],
        **kwargs: Any,
    ) -> "ProductionAuthority":
        selected, deployment, root, expected, dsn = _route_values(route)
        return cls(
            route=selected,
            deployment_name=deployment,
            repo_root=root,
            expected_git_commit=expected,
            dsn=dsn,
            writer_cls=writer_cls,
            **kwargs,
        )

    @staticmethod
    def _connect_graph(dsn: str) -> ChronosGraphStore:
        return ChronosGraphStore.connect(dsn, backend="interval", ensure_schema=False)

    def _construct_writer(self) -> Any:
        """Construct the native writer only with all production callbacks."""

        try:
            signature = inspect.signature(self.writer_cls)
        except (TypeError, ValueError) as exc:
            raise ProductionAuthorityError("writer_unavailable") from exc
        parameters = signature.parameters
        # A broad **kwargs-only constructor is not enough: silently accepting
        # omitted callbacks would make the target authority optional.  Native
        # deployment writers must advertise every production seam explicitly.
        if any(name not in parameters for name in _CALLBACKS):
            raise ProductionAuthorityError("writer_unavailable")
        callbacks = {
            "pair_state_verifier": self.pair_state_verifier,
            "close_readiness_verifier": self.close_readiness_verifier,
            "cleanup_state_verifier": self.cleanup_state_verifier,
            "workspace_observer": self.workspace_observer,
            "pact_observer": self.pact_observer,
            "target_observer": self.target_observer,
        }
        try:
            return self.writer_cls(
                self.dsn,
                deployment_name=self.deployment_name,
                **callbacks,
            )
        except Exception as exc:  # noqa: BLE001 - no DB/path detail escapes
            raise ProductionAuthorityError("writer_unavailable") from exc

    def lifecycle_for(self, plan: _AuthorityPlan) -> _ManagedLifecycle:
        self.validate_plan(plan)
        try:
            store = self.graph_store_factory(self.dsn)
            graph = ProductionGraphWorkspaceAdapter.from_store(store)
            git = self.git_adapter_factory()
            resolve_commit = getattr(git, "resolve_commit", None)
            if callable(resolve_commit):
                resolved = str(
                    resolve_commit(self.repo_root, plan.plan.git_base_commit)
                ).lower()
                if resolved != plan.plan.git_base_commit.lower():
                    raise ValueError("Git base commit is not immutable")
            lifecycle = PairedWorkspaceLifecycle(git, graph)
            return _ManagedLifecycle(lifecycle, store)
        except ProductionAuthorityError:
            raise
        except Exception as exc:  # noqa: BLE001 - close any partially opened store
            close = locals().get("store")
            if close is not None and callable(getattr(close, "close", None)):
                try:
                    close.close()
                except Exception:
                    pass
            raise ProductionAuthorityError("writer_unavailable") from exc

    def validate_plan(
        self, candidate: _AuthorityPlan | AssignmentStartPlan
    ) -> _AuthorityPlan:
        if isinstance(candidate, _AuthorityPlan):
            plan = candidate
        else:
            try:
                if isinstance(candidate, Mapping):
                    candidate = AssignmentStartPlan.from_mapping(candidate)
                plan = _AuthorityPlan(
                    plan=AssignmentStartPlan.from_mapping(candidate.as_dict()),
                    route=self.route,
                    deployment=self.deployment_name,
                    repo_root=self.repo_root,
                    expected_git_commit=self.expected_git_commit,
                )
            except Exception as exc:
                raise ProductionAuthorityError("writer_unavailable") from exc
        value = plan.plan
        try:
            if plan.route != self.route or plan.deployment != self.deployment_name:
                raise ValueError("route binding")
            if Path(value.repo_path).resolve(strict=False) != self.repo_root:
                raise ValueError("repository binding")
            if (
                value.mcp_route != self.route
                or value.graph_deployment != self.deployment_name
            ):
                raise ValueError("plan binding")
            worktree = Path(value.worktree_path).resolve(strict=False)
            if worktree == self.repo_root:
                raise ValueError("worktree binding")
            if worktree.is_relative_to(self.repo_root) or self.repo_root.is_relative_to(
                worktree
            ):
                raise ValueError("worktree containment")
        except (OSError, ValueError) as exc:
            raise ProductionAuthorityError("writer_unavailable") from exc
        return plan

    @staticmethod
    def is_bootstrap_assignment(assignment: Mapping[str, Any]) -> bool:
        """Identify the setup-created chief that has no target enrollment.

        Setup deliberately creates the first chief only after proving its
        pair; it does not create a target enrollment for that logical role.
        The native read model may therefore ask the server observer to prove
        this one already-bound pair without a target-route row.
        """

        attrs = _assignment_attrs(assignment)
        return (
            str(assignment.get("assignment_kind") or "").strip().lower() == "chief"
            and not attrs.get("target_route")
            and attrs.get("target_route_generation") in (None, "")
            and attrs.get("target_route_digest") in (None, "")
        )

    def bootstrap_plan_from_assignment(
        self, assignment: Mapping[str, Any]
    ) -> _AuthorityPlan:
        """Re-adopt the setup-created chief from its persisted pair halves.

        No enrollment is created or inferred here.  The chief row contributes
        only its already-authenticated assignment identity and the physical
        worktree/branch identifiers written during setup; Git and Chronos
        supply the immutable base, graph lineage, route, grant, and attempt
        metadata from their own live state.
        """

        if not self.is_bootstrap_assignment(assignment):
            raise ProductionAuthorityError("target_observer_unavailable")
        store: Any | None = None
        try:
            assignment_id = _token(
                assignment.get("assignment_id"), field="assignment_id"
            )
            generation = assignment.get("assignment_generation")
            if (
                isinstance(generation, bool)
                or not isinstance(generation, int)
                or generation < 1
            ):
                raise ValueError("assignment generation")
            host = _text(assignment.get("authenticated_host"), field="host")
            worktree = Path(_text(assignment.get("git_worktree_id"), field="worktree"))
            branch = _token(assignment.get("okg_branch_id"), field="branch")
            if not worktree.is_absolute() or any(
                part in {".", ".."} for part in worktree.parts
            ):
                raise ValueError("worktree path")
            store = self.graph_store_factory(self.dsn)
            graph_adapter = ProductionGraphWorkspaceAdapter.from_store(store)
            git_adapter = self.git_adapter_factory()
            graph = graph_adapter.inspect(self.deployment_name, branch)
            if not isinstance(graph, GraphWorkspaceState):
                raise ValueError("bootstrap graph state has an invalid type")
            repo = str(self.repo_root)
            canonical_worktree = str(worktree.resolve(strict=False))
            if (
                graph.deployment != self.deployment_name
                or graph.branch != branch
                or graph.assignment_id != assignment_id
                or graph.assignment_generation != generation
                or graph.authenticated_host != host
                or graph.lifecycle_status != "active"
                or not graph.current_ref
            ):
                raise ValueError("bootstrap pair identity changed")
            metadata = graph.metadata
            static_metadata = {
                "kind": "okg_paired_workspace",
                "assignment_id": assignment_id,
                "assignment_generation": generation,
                "authenticated_host": host,
                "repo_path": repo,
                "worktree_path": canonical_worktree,
                "git_branch": branch,
                "deployment": self.deployment_name,
                "branch": branch,
                "base_branch": graph.base_branch,
                "base_generation": graph.base_generation,
                "lifecycle_status": "active",
                "config_path": metadata.get("config_path"),
                "mcp_server_name": metadata.get("mcp_server_name"),
                "mcp_route": self.route,
                "mcp_command": "okg",
                "grant_id": metadata.get("grant_id"),
            }
            for key, expected in static_metadata.items():
                if metadata.get(key) != expected:
                    raise ValueError("bootstrap pair metadata changed")
            base = _text(metadata.get("git_base_commit"), field="base commit").lower()
            if not _COMMIT.fullmatch(base):
                raise ValueError("bootstrap base commit")
            register_base = getattr(git_adapter, "register_receipt_base", None)
            if callable(register_base):
                register_base(canonical_worktree, repo, base)
            git = git_adapter.inspect(worktree)
            if not isinstance(git, GitWorkspaceState):
                raise ValueError("bootstrap Git state has an invalid type")
            if (
                git.repo_path != repo
                or git.worktree_path != canonical_worktree
                or git.branch != branch
                or git.base_commit.lower() != base
            ):
                raise ValueError("bootstrap Git identity changed")
            attempt = _token(
                metadata.get("provision_attempt_id"), field="provision attempt"
            )
            grant = _token(metadata.get("grant_id"), field="grant")
            config = _text(metadata.get("config_path"), field="config path")
            config_path = Path(config)
            if not config_path.is_absolute():
                raise ValueError("bootstrap config path")
            plan = AssignmentStartPlan.from_mapping(
                {
                    "assignment_id": assignment_id,
                    "assignment_generation": generation,
                    "authenticated_host": host,
                    "repo_path": repo,
                    "worktree_path": canonical_worktree,
                    "git_branch": branch,
                    "git_base_commit": base,
                    "graph_deployment": self.deployment_name,
                    "graph_base_branch": graph.base_branch,
                    "graph_base_generation": graph.base_generation,
                    "graph_base_checkpoint": metadata.get("base_checkpoint"),
                    "graph_branch": branch,
                    "mcp_route": self.route,
                    "grant_id": grant,
                    "config_path": config,
                    "provision_attempt_id": attempt,
                }
            )
            authority_plan = self.validate_plan(plan)
            # Recheck the graph base metadata against the plan parser's
            # canonical values; this catches a state object that disagrees
            # with its own persisted metadata before it is observed.
            if (
                graph.base_branch != plan.graph_base_branch
                or graph.base_generation != plan.graph_base_generation
                or metadata.get("base_checkpoint") != plan.graph_base_checkpoint
                or metadata.get("git_base_commit") != plan.git_base_commit
            ):
                raise ValueError("bootstrap base metadata changed")
            return authority_plan
        except ProductionAuthorityError as exc:
            if exc.reason_code == "target_observer_unavailable":
                raise
            raise ProductionAuthorityError("target_observer_unavailable") from exc
        except Exception as exc:
            raise ProductionAuthorityError("target_observer_unavailable") from exc
        finally:
            if store is not None:
                close = getattr(store, "close", None)
                if callable(close):
                    try:
                        close()
                    except Exception:
                        pass

    def plan_from_assignment(self, assignment: Mapping[str, Any]) -> _AuthorityPlan:
        attrs = _assignment_attrs(assignment)
        raw = attrs.get("start_plan")
        if raw is not None:
            try:
                plan = AssignmentStartPlan.from_mapping(raw)
            except Exception as exc:
                raise ProductionAuthorityError("target_observer_unavailable") from exc
            return self.validate_plan(plan)

        # Finalized native rows retain the exact pair proof.  Older rows did
        # not duplicate the start plan in attrs, so reconstruct the closed
        # shape solely from that proof plus locked assignment columns.  No
        # caller or status cache is used for this fallback.
        raw_proof = attrs.get("pair_proof")
        if not isinstance(raw_proof, Mapping):
            raise ProductionAuthorityError("target_observer_unavailable")
        try:
            assignment_id = _token(
                assignment.get("assignment_id"), field="assignment_id"
            )
            generation = int(assignment.get("assignment_generation"))
            worktree = _text(raw_proof.get("git_worktree_id"), field="worktree")
            branch = _token(raw_proof.get("git_branch"), field="branch")
            base = _text(raw_proof.get("git_base_commit"), field="commit")
            plan = AssignmentStartPlan.from_mapping(
                {
                    "assignment_id": assignment_id,
                    "assignment_generation": generation,
                    "authenticated_host": _text(
                        assignment.get("authenticated_host"), field="host"
                    ),
                    "repo_path": str(self.repo_root),
                    "worktree_path": worktree,
                    "git_branch": branch,
                    "git_base_commit": base,
                    "graph_deployment": _text(
                        raw_proof.get("graph_deployment"), field="deployment"
                    ),
                    "graph_base_branch": _text(
                        raw_proof.get("graph_base_branch"), field="base branch"
                    ),
                    "graph_base_generation": raw_proof.get("graph_base_generation"),
                    "graph_base_checkpoint": raw_proof.get("graph_base_checkpoint"),
                    "graph_branch": _text(
                        raw_proof.get("okg_branch_id"), field="graph branch"
                    ),
                    "mcp_route": _text(raw_proof.get("mcp_route"), field="route"),
                    "grant_id": _text(raw_proof.get("grant_id"), field="grant"),
                    "config_path": str(
                        Path(worktree) / ".okg" / "coordination" / "mcp.json"
                    ),
                    "provision_attempt_id": _text(
                        raw_proof.get("provision_attempt_id"), field="attempt"
                    ),
                }
            )
        except Exception as exc:
            raise ProductionAuthorityError("target_observer_unavailable") from exc
        return self.validate_plan(plan)

    def validate_assignment_binding(
        self,
        assignment: Mapping[str, Any],
        plan: _AuthorityPlan,
    ) -> None:
        value = plan.plan
        try:
            if _text(assignment.get("assignment_id")) != value.assignment_id:
                raise ValueError("assignment")
            generation = assignment.get("assignment_generation")
            if (
                isinstance(generation, bool)
                or not isinstance(generation, int)
                or generation != value.assignment_generation
            ):
                raise ValueError("generation")
            if _text(assignment.get("deployment_name")) != self.deployment_name:
                raise ValueError("deployment")
            attrs = _assignment_attrs(assignment)
            if _text(attrs.get("target_route")) != self.route:
                raise ValueError("route")
            route_generation = attrs.get("target_route_generation")
            if (
                isinstance(route_generation, bool)
                or not isinstance(route_generation, int)
                or route_generation < 1
            ):
                raise ValueError("route generation")
            _text(attrs.get("target_route_digest"))
        except (TypeError, ValueError, ProductionAuthorityError) as exc:
            raise ProductionAuthorityError("target_observer_unavailable") from exc

    def observe_pair(
        self, plan: _AuthorityPlan, assignment: Mapping[str, Any]
    ) -> PairedWorkspaceReceipt:
        """Read the exact pair without requiring Git cleanliness.

        ``PairedWorkspaceLifecycle.adopt_for_close`` intentionally rejects a
        dirty worktree.  Status observers must still report that live dirty
        state, so this narrow read path performs the same identity checks and
        leaves the lifecycle untouched.
        """

        if not self.is_bootstrap_assignment(assignment):
            self.validate_assignment_binding(assignment, plan)
        managed = self.lifecycle_for(plan)
        try:
            request = plan.request()
            register_base = getattr(managed.git, "register_receipt_base", None)
            if callable(register_base):
                register_base(
                    request.worktree_path, request.repo_path, request.git_base_commit
                )
            git = managed.git.inspect(request.worktree_path)
            graph = managed.graph.inspect(
                request.graph_deployment, request.resolved_graph_branch
            )
            if not isinstance(git, GitWorkspaceState) or not isinstance(
                graph, GraphWorkspaceState
            ):
                raise ValueError("pair observation has an invalid state")
            if (
                git.repo_path != str(self.repo_root)
                or git.worktree_path != str(Path(request.worktree_path).resolve())
                or git.branch != request.git_branch
                or git.base_commit.lower() != request.git_base_commit.lower()
            ):
                raise ValueError("Git pair identity changed")
            if (
                graph.deployment != self.deployment_name
                or graph.branch != request.resolved_graph_branch
                or not graph.current_ref
                or graph.base_branch != request.graph_base_branch
                or graph.base_generation != request.graph_base_generation
                or graph.assignment_id != request.assignment_id
                or graph.assignment_generation != request.assignment_generation
                or graph.authenticated_host != request.authenticated_host
                or graph.lifecycle_status
                not in {"active", "checkpointed", "handed_off"}
            ):
                raise ValueError("graph pair identity changed")
            metadata = graph.metadata
            expected_metadata = {
                "assignment_id": request.assignment_id,
                "assignment_generation": request.assignment_generation,
                "authenticated_host": request.authenticated_host,
                "repo_path": str(self.repo_root),
                "worktree_path": str(Path(request.worktree_path).resolve()),
                "git_branch": request.git_branch,
                "deployment": self.deployment_name,
                "branch": request.resolved_graph_branch,
                "mcp_route": self.route,
                "grant_id": request.grant_id,
                "provision_attempt_id": request.provision_attempt_id,
            }
            for key, expected in expected_metadata.items():
                if metadata.get(key) != expected:
                    raise ValueError("graph pair metadata changed")
            if metadata.get("base_checkpoint") != request.graph_base_checkpoint:
                raise ValueError("graph checkpoint metadata changed")
            if graph.lifecycle_status in {"checkpointed", "handed_off"}:
                if (
                    not graph.checkpoint
                    or not graph.checkpoint_ref
                    or graph.checkpoint not in graph.checkpoint_ids
                ):
                    raise ValueError("graph checkpoint binding is incomplete")
                matches = getattr(managed.graph, "checkpoint_matches_current", None)
                if callable(matches) and not matches(
                    graph.deployment,
                    graph.branch,
                    graph.checkpoint,
                    graph.checkpoint_ref,
                    graph.current_ref,
                ):
                    raise ValueError("graph checkpoint is stale")
            config_path = managed._lifecycle._config_path(
                str(Path(request.worktree_path).resolve()),
                request.config_path
                or Path(request.worktree_path) / ".okg" / "coordination" / "mcp.json",
            )
            managed._lifecycle._validate_config_filesystem(
                str(Path(request.worktree_path).resolve()), config_path
            )
            receipt = PairedWorkspaceReceipt(
                assignment_id=request.assignment_id,
                assignment_generation=request.assignment_generation,
                authenticated_host=request.authenticated_host,
                repo_path=str(self.repo_root),
                worktree_path=str(Path(request.worktree_path).resolve()),
                git=git,
                graph=graph,
                lifecycle_status=graph.lifecycle_status,
                predecessor_digest=graph.predecessor_digest,
                cleanup_disposition=graph.cleanup_disposition,
                config_path=str(config_path),
                mcp_server_name=request.mcp_server_name,
                mcp_route=self.route,
                mcp_command=request.mcp_command,
                grant_id=request.grant_id or "",
            )
            # Use the same persisted binding as close adoption while skipping
            # only dynamic Git head/tree/dirty fields.  This keeps status
            # truthful for a dirty worktree without turning it into a close
            # attestation.
            _validate_managed_config(receipt)
            _metadata_identity_matches(graph.metadata, receipt, allow_state_move=True)
            # ``allow_state_move`` omits immutable Git base metadata because
            # the helper also serves checkpoint movement.  Keep that fence
            # explicit for this target-local observer.
            if graph.metadata.get("git_base_commit") != request.git_base_commit:
                raise ValueError("Git base metadata changed")
            return receipt
        except ProductionAuthorityError:
            raise
        except Exception as exc:
            raise ProductionAuthorityError("target_observer_unavailable") from exc
        finally:
            managed.close()

    def _run_with_lifecycle(
        self, operation: Callable[[_ManagedLifecycle], Any], plan: _AuthorityPlan
    ) -> Any:
        lifecycle = self.lifecycle_for(plan)
        try:
            return operation(lifecycle)
        finally:
            lifecycle.close()

    def _pair_for_start(
        self,
        *,
        assignment_id: str,
        assignment_generation: int,
        start_attempt_id: str,
        pair_digest: str,
        pair_proof_value: Mapping[str, Any],
        start_plan: Mapping[str, Any],
    ) -> str:
        try:
            plan = AssignmentStartPlan.from_mapping(start_plan)
            authority_plan = self.validate_plan(plan)
            if (
                plan.assignment_id != assignment_id
                or plan.assignment_generation != assignment_generation
                or plan.provision_attempt_id != start_attempt_id
            ):
                raise ValueError("assignment binding")
            if _digest(pair_digest) != pair_proof_digest(pair_proof_value):
                raise ValueError("pair digest")
            result = self._run_with_lifecycle(
                lambda lifecycle: lifecycle.adopt(plan.paired_request()), authority_plan
            )
            if result.lifecycle_status != "active":
                raise ValueError("pair status")
            live_proof = pair_proof(result)
            if dict(live_proof) != dict(pair_proof_value):
                raise ValueError("pair proof")
            live_digest = pair_proof_digest(live_proof)
            if live_digest != pair_digest:
                raise ValueError("pair digest")
            return live_digest
        except ProductionAuthorityError:
            raise
        except Exception as exc:
            raise ProductionAuthorityError("pair_attestation_invalid") from exc

    def pair_state_verifier(self, **kwargs: Any) -> str:
        return self._pair_for_start(
            assignment_id=_text(kwargs.get("assignment_id")),
            assignment_generation=int(kwargs.get("assignment_generation")),
            start_attempt_id=_text(kwargs.get("start_attempt_id")),
            pair_digest=_text(kwargs.get("pair_digest")),
            pair_proof_value=_mapping(kwargs.get("pair_proof")),
            start_plan=_mapping(kwargs.get("start_plan")),
        )

    def observe_pact(
        self,
        assignment: Mapping[str, Any],
        plan: _AuthorityPlan,
    ) -> dict[str, Any]:
        assignment_kind = str(assignment.get("assignment_kind") or "").strip().lower()
        if assignment_kind != "pact":
            # Chief and sprint rows coordinate PACT children; they do not own
            # a change/task file.  Keep their projection finite and let the
            # native no-active-children, Git, checkpoint, and external gates
            # decide close readiness.
            return {
                "change_id": None,
                "task_id": None,
                "state": "not_applicable",
                "status": "not_applicable",
                "task_done": True,
                "done": True,
                "proof_attached": True,
                "merge_ready": False,
                "applicable": False,
                "authority": "live",
            }
        change_id = assignment.get("change_id")
        task_id = assignment.get("task_id")
        if not isinstance(change_id, str) or not _TOKEN.fullmatch(change_id):
            raise ProductionAuthorityError("pact_incomplete")
        if not isinstance(task_id, str) or not _TOKEN.fullmatch(task_id):
            raise ProductionAuthorityError("pact_incomplete")
        pact_path = (
            Path(plan.plan.worktree_path) / "pact" / "changes" / change_id / "pact.yaml"
        )
        try:
            current = pact_path
            worktree = Path(plan.plan.worktree_path)
            while current != worktree and current != current.parent:
                if current.is_symlink():
                    raise ValueError("PACT path traverses a symlink")
                current = current.parent
            if not pact_path.is_file() or pact_path.stat().st_size > _PACT_MAX_BYTES:
                raise ValueError("PACT file missing")
            from okg.substrate.pact.evidence_kinds import load_registry
            from okg.substrate.pact.store import load_manifest_file
            from okg.substrate.pact.validation import validate_manifest

            manifest = load_manifest_file(pact_path)
            if manifest.change.id != change_id:
                raise ValueError("PACT change identity changed")
            report = validate_manifest(manifest, repo_root=worktree)
            registry = load_registry(worktree)
        except Exception as exc:  # noqa: BLE001 - PACT is a typed blocker
            raise ProductionAuthorityError("pact_incomplete") from exc
        task = next((item for item in manifest.tasks if item.id == task_id), None)
        if task is None:
            raise ProductionAuthorityError("pact_incomplete")
        status = str(task.status or "").strip().lower()
        alignment = report.alignment.get("tasks", {}).get(task_id, {})
        aligned = bool(
            report.ok
            and status == "done"
            and alignment.get("status") == "complete"
            and alignment.get("satisfaction_status") == "satisfied"
            and not alignment.get("missing")
            and not alignment.get("evidence_issues")
        )
        try:
            events = self._load_pact_proof_events(change_id)
        except Exception as exc:  # noqa: BLE001 - ledger proof is mandatory
            raise ProductionAuthorityError("pact_incomplete") from exc
        proof_attached = aligned and any(
            evidence.status == "pass"
            and task_id in evidence.refs
            and registry.canonicalize(evidence.kind) == "executed_test"
            and self._pact_evidence_has_recorded_event(
                evidence_id=evidence.id,
                evidence_kind=evidence.kind,
                evidence_artifact=evidence.artifact,
                task_id=task_id,
                events=events,
                registry=registry,
            )
            for evidence in manifest.evidence
        )
        return {
            "change_id": change_id,
            "task_id": task_id,
            "state": status or "unknown",
            "status": status or "unknown",
            "task_done": aligned,
            "done": aligned,
            "proof_attached": proof_attached,
            "merge_ready": aligned,
            "authority": "live",
        }

    def _load_pact_proof_events(self, change_id: str) -> list[Mapping[str, Any]]:
        loader = self.pact_event_loader
        if loader is not None:
            rows = loader(change_id)
        else:
            from okg.substrate.db.connection import connect as db_connect

            with db_connect(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT event_type, payload
                          FROM okg.pact_coordination_events
                         WHERE change_id = %s
                           AND event_type = 'proof'
                         ORDER BY id DESC
                         LIMIT 256
                        """,
                        (change_id,),
                    )
                    rows = [
                        {"event_type": row[0], "payload": row[1]}
                        for row in cur.fetchall()
                    ]
        if not isinstance(rows, list) or len(rows) > 256:
            raise ProductionAuthorityError("pact_incomplete")
        if any(not isinstance(row, Mapping) for row in rows):
            raise ProductionAuthorityError("pact_incomplete")
        return rows

    @staticmethod
    def _pact_evidence_has_recorded_event(
        *,
        evidence_id: str,
        evidence_kind: str,
        evidence_artifact: str | None,
        task_id: str,
        events: list[Mapping[str, Any]],
        registry: Any,
    ) -> bool:
        for event in events:
            if event.get("event_type") != "proof":
                continue
            payload = event.get("payload")
            if not isinstance(payload, Mapping):
                continue
            command = payload.get("command")
            if command == "evidence attach":
                try:
                    event_kind = registry.canonicalize(str(payload.get("kind") or ""))
                except Exception:
                    continue
                if (
                    payload.get("evidence_id") == evidence_id
                    and payload.get("target") == task_id
                    and payload.get("status") == "pass"
                    and payload.get("ref") == evidence_artifact
                    and event_kind == registry.canonicalize(evidence_kind)
                ):
                    return True
            elif command == "evidence ingest pytest":
                evidence_ids = payload.get("evidence_ids")
                tasks = payload.get("tasks")
                statuses = payload.get("statuses")
                if (
                    isinstance(evidence_ids, list)
                    and evidence_id in evidence_ids
                    and isinstance(tasks, list)
                    and task_id in tasks
                    and isinstance(statuses, list)
                    and "pass" in statuses
                    and payload.get("report_path") == evidence_artifact
                ):
                    return True
        return False

    def close_readiness_verifier(self, **kwargs: Any) -> dict[str, Any]:
        blockers: list[str] = []
        assignment = _mapping(kwargs.get("assignment"))
        assignment_kind = str(assignment.get("assignment_kind") or "").strip().lower()
        if kwargs.get("phase") == "finalize":
            # Finalization runs after the exact pair has been retired and
            # cleaned, so the target-local pair cannot be read again.  Native
            # persists the bounded server snapshot produced during prepare;
            # revalidate its immutable Git/Pact binding and rerun live external
            # reconciliation before capacity can be released.
            try:
                attrs = _assignment_attrs(assignment)
                if attrs.get("close_prepared") is not True:
                    raise ValueError("close is not prepared")
                prepared = AssignmentClosePlan.from_mapping(attrs.get("close_plan"))
                if (
                    prepared.close_attempt_id != kwargs.get("close_attempt_id")
                    or prepared.start_plan.as_dict()
                    != AssignmentStartPlan.from_mapping(
                        kwargs.get("start_plan")
                    ).as_dict()
                ):
                    raise ValueError("prepared close binding changed")
                snapshot = attrs.get("close_readiness_snapshot")
                if (
                    not isinstance(snapshot, Mapping)
                    or set(snapshot) != {"pair_state", "pact"}
                    or not isinstance(snapshot.get("pair_state"), Mapping)
                    or not isinstance(snapshot.get("pact"), Mapping)
                ):
                    raise ValueError("prepared reconciliation snapshot missing")
                pair_state = dict(snapshot["pair_state"])
                prepared_pact = dict(snapshot["pact"])
                if close_pair_proof_digest(pair_state) != prepared.pair_digest:
                    raise ValueError("prepared pair snapshot changed")
                authority_plan = self.validate_plan(prepared.start_plan)
                self.validate_assignment_binding(assignment, authority_plan)

                # Resolve the immutable prepared Git head while the target
                # pair is still represented by its server-owned snapshot.
                git_factory = getattr(self, "git_adapter_factory", None)
                if not callable(git_factory):
                    raise ValueError("Git authority is unavailable")
                git = git_factory()
                resolve_commit = getattr(git, "resolve_commit", None)
                if not callable(resolve_commit):
                    raise ValueError("Git commit authority is unavailable")
                head_commit = _text(pair_state.get("git_head_commit"), field="commit")
                if (
                    str(resolve_commit(self.repo_root, head_commit)).lower()
                    != head_commit.lower()
                ):
                    raise ValueError("prepared Git head is unavailable")

                if assignment_kind == "pact":
                    # The Pact file lives in the target worktree and is
                    # intentionally removed before native finalization.  Its
                    # server-derived completion summary is therefore checked
                    # as a bound prepared snapshot rather than re-reading a
                    # path which must no longer exist.
                    current_pact = prepared_pact
                    if (
                        current_pact.get("change_id") != assignment.get("change_id")
                        or current_pact.get("task_id") != assignment.get("task_id")
                        or current_pact.get("done") is not True
                        or current_pact.get("proof_attached") is not True
                    ):
                        blockers.append("pact_incomplete")
                else:
                    current_pact = self.observe_pact(assignment, authority_plan)
                    if current_pact != prepared_pact:
                        blockers.append("pact_incomplete")

                reconciler = self.external_github_reconciler
                if reconciler is None:
                    blockers.append("github_incomplete")
                else:
                    result = reconciler(
                        assignment=dict(assignment),
                        start_plan=prepared.start_plan.as_dict(),
                        pair_state=pair_state,
                        pact=current_pact,
                    )
                    if result is not True and not (
                        isinstance(result, Mapping) and result.get("ok") is True
                    ):
                        blockers.append("github_incomplete")

                normalized = list(dict.fromkeys(blockers))
                readiness = _canonical_digest(
                    {
                        "assignment_id": assignment.get("assignment_id"),
                        "assignment_generation": assignment.get(
                            "assignment_generation"
                        ),
                        "route": self.route,
                        "deployment": self.deployment_name,
                        "pair_digest": prepared.pair_digest,
                        "pair_state": pair_state,
                        "pact": current_pact,
                        "blockers": normalized,
                    }
                )
                return {
                    "readiness_digest": readiness,
                    "pair_digest": prepared.pair_digest,
                    "blockers": normalized,
                }
            except Exception:
                return {
                    "readiness_digest": _canonical_digest(
                        {
                            "assignment": assignment.get("assignment_id"),
                            "route": self.route,
                            "phase": "finalize",
                            "invalid": True,
                        }
                    ),
                    "pair_digest": _canonical_digest(
                        {
                            "assignment": assignment.get("assignment_id"),
                            "route": self.route,
                            "pair": "invalid",
                        }
                    ),
                    "blockers": ["checkpoint_stale"],
                }
        try:
            plan = self.validate_plan(
                AssignmentStartPlan.from_mapping(kwargs.get("start_plan"))
            )
            self.validate_assignment_binding(assignment, plan)
            expected_pair = _digest(kwargs.get("pair_digest"))
        except Exception:
            plan = None
            expected_pair = None
            blockers.append("checkpoint_stale")
        pact: Mapping[str, Any] = {}
        pair_state: Mapping[str, Any] = {}
        if plan is not None:
            try:
                receipt = self.observe_pair(plan, assignment)
                if receipt.git.dirty:
                    blockers.append("git_dirty")
                else:
                    pair_state = close_pair_proof(receipt)
                    # Active close must still describe the exact pair that
                    # native start finalized. Checkpointed/handoff pairs use
                    # the complete close proof above.
                    stored_proof = _assignment_attrs(assignment).get("pair_proof")
                    if (
                        isinstance(stored_proof, Mapping)
                        and receipt.lifecycle_status == "active"
                    ):
                        current_start_proof = pair_proof(receipt)
                        if dict(current_start_proof) != dict(stored_proof):
                            blockers.append("checkpoint_stale")
                        elif expected_pair != pair_proof_digest(current_start_proof):
                            blockers.append("checkpoint_stale")
            except Exception:
                blockers.append("checkpoint_stale")
            try:
                pact = self.observe_pact(assignment, plan)
                if assignment_kind == "pact" and (
                    pact.get("done") is not True
                    or pact.get("proof_attached") is not True
                ):
                    blockers.append("pact_incomplete")
            except ProductionAuthorityError as exc:
                blockers.append(
                    exc.reason_code
                    if exc.reason_code == "pact_incomplete"
                    else "pact_incomplete"
                )
            except Exception:
                blockers.append("pact_incomplete")
        # The cutover child supplies this seam.  Absence is an explicit block,
        # even when every local state is clean; it must never be inferred.
        reconciler = self.external_github_reconciler
        if reconciler is None:
            blockers.append("github_incomplete")
        else:
            try:
                result = reconciler(
                    assignment=dict(assignment),
                    start_plan=plan.plan.as_dict() if plan is not None else {},
                    pair_state=dict(pair_state),
                    pact=dict(pact),
                )
                if result is not True and not (
                    isinstance(result, Mapping) and result.get("ok") is True
                ):
                    blockers.append("github_incomplete")
            except Exception:
                blockers.append("github_incomplete")
        normalized = list(
            dict.fromkeys(item for item in blockers if item in _CLOSE_BLOCKERS)
        )
        if expected_pair is None:
            expected_pair = _canonical_digest(
                {"assignment": assignment.get("assignment_id"), "route": self.route}
            )
        if pair_state:
            try:
                expected_pair = close_pair_proof_digest(pair_state)
            except Exception:
                blockers.append("checkpoint_stale")
        normalized = list(
            dict.fromkeys(item for item in blockers if item in _CLOSE_BLOCKERS)
        )
        readiness = _canonical_digest(
            {
                "assignment_id": assignment.get("assignment_id"),
                "assignment_generation": assignment.get("assignment_generation"),
                "route": self.route,
                "deployment": self.deployment_name,
                "pair_digest": expected_pair,
                "pair_state": dict(pair_state),
                "pact": dict(pact),
                "blockers": normalized,
            }
        )
        return {
            "readiness_digest": readiness,
            "pair_digest": expected_pair,
            "blockers": normalized,
            "authority_snapshot": {
                "pair_state": dict(pair_state),
                "pact": dict(pact),
            },
        }

    def cleanup_state_verifier(self, **kwargs: Any) -> str:
        assignment = _mapping(kwargs.get("assignment"))
        try:
            plan = AssignmentClosePlan.from_mapping(kwargs.get("close_plan"))
            authority_plan = self.validate_plan(plan.start_plan)
            self.validate_assignment_binding(assignment, authority_plan)
            proof = CleanupProof.from_mapping(kwargs.get("cleanup_proof"))
            proof.validate_complete()
            provided = _digest(kwargs.get("cleanup_digest"))
            computed = cleanup_proof_digest(proof)
            if (
                provided != computed
                or proof.assignment_id != plan.assignment_id
                or proof.assignment_generation != plan.assignment_generation
                or proof.close_attempt_id != plan.close_attempt_id
                or proof.readiness_digest != plan.readiness_digest
            ):
                raise ValueError("cleanup proof binding")
            worktree = Path(plan.start_plan.worktree_path)
            config = Path(
                plan.start_plan.config_path
                or worktree / ".okg" / "coordination" / "mcp.json"
            )
            if os.path.lexists(worktree) or os.path.lexists(config):
                raise ValueError("cleanup target remains")
            git = self.git_adapter_factory()
            if git.branch_exists(self.repo_root, plan.start_plan.git_branch):
                raise ValueError("Git branch remains")
            store = self.graph_store_factory(self.dsn)
            try:
                graph = ProductionGraphWorkspaceAdapter.from_store(store)
                if graph.branch_exists(
                    self.deployment_name, plan.start_plan.graph_branch
                ):
                    raise ValueError("graph branch remains")
            finally:
                close = getattr(store, "close", None)
                if callable(close):
                    close()
            return computed
        except ProductionAuthorityError:
            raise
        except Exception as exc:
            raise ProductionAuthorityError("cleanup_attestation_invalid") from exc

    def _raw_start_prepare(self, evidence: Any, **kwargs: Any) -> Any:
        return self.writer.prepare_assignment_start_from_peer(evidence, **kwargs)

    def _raw_start_finalize(self, evidence: Any, **kwargs: Any) -> Any:
        return self.writer.finalize_assignment_start_from_peer(evidence, **kwargs)

    def _raw_close_prepare(self, evidence: Any, **kwargs: Any) -> Any:
        return self.writer.prepare_assignment_close_from_peer(evidence, **kwargs)

    def _raw_close_finalize(self, evidence: Any, **kwargs: Any) -> Any:
        return self.writer.finalize_assignment_close_from_peer(evidence, **kwargs)

    @staticmethod
    def _result_mapping(value: Any) -> Mapping[str, Any]:
        if hasattr(value, "as_dict"):
            value = value.as_dict()
        if not isinstance(value, Mapping):
            raise ProductionAuthorityError("writer_unavailable")
        return value

    def _run_start(self, evidence: Any, **kwargs: Any) -> Any:
        orchestrator = AssignmentStartOrchestrator(
            prepare=lambda **values: self._result_mapping(
                self._raw_start_prepare(evidence, **values)
            ),
            finalize=lambda **values: self._result_mapping(
                self._raw_start_finalize(evidence, **values)
            ),
            lifecycle_factory=lambda plan: self.lifecycle_for(self.validate_plan(plan)),
        )
        lifecycle = None
        try:
            # The orchestrator's factory owns the resource; capture it through
            # the wrapper's explicit list so failures close it as well.
            original_factory = orchestrator.lifecycle_factory
            opened: list[_ManagedLifecycle] = []
            orchestrator.lifecycle_factory = lambda plan: (
                opened.append(original_factory(plan)) or opened[-1]
            )
            try:
                return orchestrator.start(**kwargs)
            finally:
                for lifecycle in opened:
                    lifecycle.close()
        except AssignmentHandshakeError as exc:
            code = (
                "pair_attestation_invalid"
                if "pair" in str(exc).lower()
                else "writer_unavailable"
            )
            return {"ok": False, "status": "rejected", "rejection_code": code}
        except ProductionAuthorityError as exc:
            return {
                "ok": False,
                "status": "rejected",
                "rejection_code": exc.reason_code,
            }
        except PairedWorkspaceError:
            return {
                "ok": False,
                "status": "rejected",
                "rejection_code": "pair_attestation_invalid",
            }

    def _run_close(self, evidence: Any, **kwargs: Any) -> Any:
        orchestrator = AssignmentCloseOrchestrator(
            prepare=lambda **values: self._result_mapping(
                self._raw_close_prepare(evidence, **values)
            ),
            finalize=lambda **values: self._result_mapping(
                self._raw_close_finalize(evidence, **values)
            ),
            lifecycle_factory=lambda plan: self.lifecycle_for(self.validate_plan(plan)),
        )
        try:
            opened: list[_ManagedLifecycle] = []
            original_factory = orchestrator.lifecycle_factory
            orchestrator.lifecycle_factory = lambda plan: (
                opened.append(original_factory(plan)) or opened[-1]
            )
            try:
                return orchestrator.close(**kwargs)
            finally:
                for lifecycle in opened:
                    lifecycle.close()
        except AssignmentCloseError as exc:
            code = (
                "checkpoint_stale"
                if "adopt" in str(exc).lower()
                else "writer_unavailable"
            )
            return {"ok": False, "status": "rejected", "rejection_code": code}
        except ProductionAuthorityError as exc:
            return {
                "ok": False,
                "status": "rejected",
                "rejection_code": exc.reason_code,
            }
        except PairedWorkspaceError:
            return {
                "ok": False,
                "status": "rejected",
                "rejection_code": "checkpoint_stale",
            }

    # Public production methods execute target lifecycle behind the existing
    # prepare wire operation.  Raw finalize methods remain exact retry seams.
    def prepare_assignment_start_from_peer(self, evidence: Any, **kwargs: Any) -> Any:
        return self._run_start(evidence, **kwargs)

    def finalize_assignment_start_from_peer(self, evidence: Any, **kwargs: Any) -> Any:
        return self._raw_start_finalize(evidence, **kwargs)

    def prepare_assignment_close_from_peer(self, evidence: Any, **kwargs: Any) -> Any:
        return self._run_close(evidence, **kwargs)

    def finalize_assignment_close_from_peer(self, evidence: Any, **kwargs: Any) -> Any:
        return self._raw_close_finalize(evidence, **kwargs)

    # Explicit raw aliases aid recovery code without changing the wire shape.
    raw_prepare_assignment_start_from_peer = _raw_start_prepare
    raw_finalize_assignment_start_from_peer = _raw_start_finalize
    raw_prepare_assignment_close_from_peer = _raw_close_prepare
    raw_finalize_assignment_close_from_peer = _raw_close_finalize

    def __getattr__(self, name: str) -> Any:
        return getattr(self.writer, name)


ProductionTargetAuthority = ProductionAuthority
ProductionAuthorityBuilder = ProductionAuthority


def build_production_authority(
    route: Any, writer_cls: type[Any], **kwargs: Any
) -> ProductionAuthority:
    return ProductionAuthority.from_route(route, writer_cls, **kwargs)


build_server_owned_authority = build_production_authority


def build_production_writer(route: Any, writer_cls: type[Any], **kwargs: Any) -> Any:
    # The authority itself implements the native writer surface and wraps the
    # prepare operations with target-local lifecycle orchestration.  Returning
    # only ``.writer`` would silently bypass that composition.
    return build_production_authority(route, writer_cls, **kwargs)


__all__ = [
    "ProductionAuthority",
    "ProductionAuthorityBuilder",
    "ProductionAuthorityError",
    "ProductionAuthorityUnavailable",
    "ProductionPactObserver",
    "ProductionTargetAuthority",
    "ProductionTargetObserver",
    "ProductionWorkspaceObserver",
    "build_production_authority",
    "build_production_writer",
    "build_server_owned_authority",
]
