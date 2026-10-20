"""Small setup/start orchestration seams for native coordination.

The native coordination ledger, PACT policy, Git, and Chronos own their
respective state.  This module only composes read probes and injected writers;
it deliberately has no database connection, host client, or identity source.
That makes a plan safe to inspect and keeps the production boundary explicit:
an authenticated operator-control client is required for setup, and an
authenticated host-local executor is required for start.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
from types import MappingProxyType
from typing import Any, Callable, Mapping, Protocol, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from okg.substrate.errors import SubstrateError
from okg.substrate.pact.project_settings import (
    LOCAL_CLIENT_ROUTE_NAMES,
    REMOTE_CLIENT_ROUTE_COUNT,
    CoordinationPolicy,
    coordination_policy,
)


_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_REFERENCE_RE = re.compile(r"^(?:\$\{[A-Z][A-Z0-9_]*\}|credential_ref:[A-Za-z0-9_.:-]+)$")
_VIZ_PLAN_PATH = "/viz/api/coordination/setup/plan"
_VIZ_APPLY_PATH = "/viz/api/coordination/control/bootstrap"
_VIZ_OPERATOR_TOKEN_HEADER = "x-okg-operator-token"
_SAFE_APPLY_RECEIPT_FIELDS = frozenset(
    {
        "ok", "action", "deployment_name", "control_mode", "control_generation",
        "assignment_id", "plan_digest", "policy_digest", "project_digest",
        "assignment_generation",
        "audit_event_id", "status", "complete", "recoverable", "applied_operations",
        "compensation", "next_action",
    }
)

def _digest(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _safe_token(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    token = value.strip()
    if any(char.isspace() or char == "\x00" for char in token):
        raise ValueError(f"{field} must be one token")
    return token


@dataclass(frozen=True, slots=True)
class ClientShape:
    """One logical client shape; route and host values remain opaque."""

    name: str
    agent: str
    host_ref: str
    remote: bool
    route_ref: str

    def __post_init__(self) -> None:
        _safe_token(self.name, field="client shape name")
        if self.agent not in {"claude-code", "codex"}:
            raise ValueError("client shape agent must be claude-code or codex")
        _safe_token(self.host_ref, field="client shape host_ref")
        if not self.host_ref.startswith("logical:"):
            raise ValueError("client shape host_ref must be a logical reference")
        _safe_token(self.route_ref, field="client shape route_ref")
        if not self.route_ref.startswith("opaque:"):
            raise ValueError("client shape route_ref must be opaque")

    def to_json(self) -> dict[str, Any]:
        return {
            "name": self.name,
            # These are runtime-routing metadata, not caller identity.  Keep
            # the wire names distinct from the operator/assignment identity
            # vocabulary rejected by the control request validator.
            "runtime": self.agent,
            "location_ref": self.host_ref,
            "remote": self.remote,
            "route_ref": self.route_ref,
        }


LOCAL_CLIENT_SHAPES: tuple[ClientShape, ...] = (
    ClientShape("local-claude", "claude-code", "logical:local", False, "opaque:local-claude"),
    ClientShape("local-codex", "codex", "logical:local", False, "opaque:local-codex"),
)
"""The two local shapes.  Remote shapes come only from tracked policy."""

assert tuple(shape.name for shape in LOCAL_CLIENT_SHAPES) == LOCAL_CLIENT_ROUTE_NAMES


def _remote_shape(name: str, route_ref: str) -> ClientShape:
    return ClientShape(name, "codex", f"logical:{name}", True, route_ref)


def client_shapes_for_routes(routes: Mapping[str, str]) -> tuple[ClientShape, ...]:
    """Build the closed four-shape list from configured client routes.

    The local shapes are fixed; the remote route names are whatever the
    deployment's ``coordination.client_routes`` names, in sorted order.  There
    is no built-in remote name: routes that do not name exactly the local
    routes plus the configured number of remote routes raise ``ValueError``.
    """
    if not isinstance(routes, Mapping):
        raise ValueError("client routes must be a mapping")
    missing = [name for name in LOCAL_CLIENT_ROUTE_NAMES if name not in routes]
    remote = sorted(str(name) for name in routes if name not in LOCAL_CLIENT_ROUTE_NAMES)
    if missing or len(remote) != REMOTE_CLIENT_ROUTE_COUNT:
        raise ValueError(
            "client routes must name the local routes and exactly "
            f"{REMOTE_CLIENT_ROUTE_COUNT} configured remote routes"
        )
    shapes: list[ClientShape] = []
    for shape in LOCAL_CLIENT_SHAPES:
        shapes.append(
            ClientShape(shape.name, shape.agent, shape.host_ref, shape.remote, routes[shape.name])
        )
    for name in remote:
        if name.startswith("local-"):
            raise ValueError("a remote client route may not use the local- prefix")
        shapes.append(_remote_shape(name, routes[name]))
    return tuple(shapes)


def _policy_client_shapes(policy: CoordinationPolicy | None) -> tuple[tuple[ClientShape, ...], bool]:
    """Resolve the closed shape list from tracked opaque route references.

    Without a usable tracked policy the plan lists only the local shapes and
    reports ``client_routes`` unavailable; it never invents a remote route.
    """
    if policy is None:
        return LOCAL_CLIENT_SHAPES, False
    try:
        return client_shapes_for_routes(getattr(policy, "client_routes", None)), True
    except (TypeError, ValueError):
        return LOCAL_CLIENT_SHAPES, False


@dataclass(frozen=True, slots=True)
class SetupPlan:
    """A deterministic, read-only setup snapshot and its exact writes."""

    repo_root: Path = field(repr=False, compare=False)
    deployment: str
    policy_digest: str
    human_policy_digest: str
    project_digest: str
    capacity_digest: str
    bindings: Mapping[str, Any]
    clients: tuple[ClientShape, ...]
    checks: Mapping[str, Any]
    mutations: tuple[Mapping[str, Any], ...]
    read_ok: bool
    ready: bool
    decision_required: bool
    blocked_by: tuple[str, ...]
    next_action: str
    plan_digest: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "checks", MappingProxyType(dict(self.checks)))
        object.__setattr__(self, "bindings", MappingProxyType(dict(self.bindings)))
        object.__setattr__(
            self,
            "mutations",
            tuple(MappingProxyType(dict(row)) for row in self.mutations),
        )
        object.__setattr__(self, "clients", tuple(self.clients))
        object.__setattr__(self, "blocked_by", tuple(self.blocked_by))
        _safe_token(self.deployment, field="deployment")
        if self.plan_digest != _plan_digest(self._unsigned_payload()):
            raise ValueError("setup plan digest does not match its contents")

    def _unsigned_payload(self) -> dict[str, Any]:
        return {
            "schema": "okg.coordination.setup-plan.v1",
            "deployment": self.deployment,
            "policy_digest": self.policy_digest,
            "human_policy_digest": self.human_policy_digest,
            "project_digest": self.project_digest,
            "capacity_digest": self.capacity_digest,
            "bindings": dict(self.bindings),
            "clients": [client.to_json() for client in self.clients],
            "checks": dict(self.checks),
            "mutations": [dict(row) for row in self.mutations],
            "read_ok": self.read_ok,
            "ready": self.ready,
            "decision_required": self.decision_required,
            "blocked_by": list(self.blocked_by),
            "next_action": self.next_action,
        }

    def to_json(self) -> dict[str, Any]:
        payload = dict(self._unsigned_payload())
        payload["plan_digest"] = self.plan_digest
        return payload

    as_dict = to_json

    @classmethod
    def from_json(cls, payload: Mapping[str, Any], *, repo_root: Path) -> "SetupPlan":
        """Parse one exact plan document and recompute its digest.

        Saved plans are untrusted transport data.  The wire shape is closed,
        including nested client, binding, check, and mutation objects, so a
        caller cannot add a field that is silently ignored by the apply path.
        """
        if not isinstance(payload, Mapping):
            raise ValueError("setup plan must be a JSON object")

        def exact(value: Any, keys: set[str], field_name: str) -> dict[str, Any]:
            if not isinstance(value, Mapping) or not all(isinstance(k, str) for k in value):
                raise ValueError(f"{field_name} must be an object")
            if set(value) != keys:
                raise ValueError(f"{field_name} has unexpected or missing fields")
            return dict(value)

        top = exact(
            payload,
            {
                "schema", "deployment", "policy_digest", "human_policy_digest",
                "project_digest", "capacity_digest", "bindings", "clients",
                "checks", "mutations", "read_ok", "ready", "decision_required",
                "blocked_by", "next_action", "plan_digest",
            },
            "setup plan",
        )
        if top["schema"] != "okg.coordination.setup-plan.v1":
            raise ValueError("unsupported setup plan schema")
        deployment = _safe_token(top["deployment"], field="deployment")
        digest_fields = ("policy_digest", "human_policy_digest", "project_digest", "capacity_digest", "plan_digest")
        for name in digest_fields:
            if not isinstance(top[name], str) or not _DIGEST_RE.fullmatch(top[name]):
                raise ValueError(f"{name} must be a sha256 digest")

        raw_clients = top["clients"]
        expected_count = len(LOCAL_CLIENT_SHAPES) + REMOTE_CLIENT_ROUTE_COUNT
        if not isinstance(raw_clients, list) or len(raw_clients) != expected_count:
            raise ValueError("setup plan clients must contain the four closed shapes")
        clients: list[ClientShape] = []
        for index, raw_client in enumerate(raw_clients):
            value = exact(
                raw_client,
                {"name", "runtime", "location_ref", "remote", "route_ref"},
                f"clients[{index}]",
            )
            if not isinstance(value["remote"], bool):
                raise ValueError(f"clients[{index}].remote must be boolean")
            try:
                clients.append(
                    ClientShape(
                        name=value["name"],
                        agent=value["runtime"],
                        host_ref=value["location_ref"],
                        remote=value["remote"],
                        route_ref=value["route_ref"],
                    )
                )
            except (TypeError, ValueError) as exc:
                raise ValueError(f"clients[{index}] is malformed") from exc
        if len({client.name for client in clients}) != len(clients):
            raise ValueError("setup plan clients contain ambiguous names")
        # The remote names are the plan's own; the policy digest the apply
        # path re-checks binds them to the deployment's tracked routes.
        try:
            expected_clients = client_shapes_for_routes(
                {client.name: client.route_ref for client in clients}
            )
        except ValueError as exc:
            raise ValueError("setup plan clients are not the closed shapes") from exc
        if tuple(clients) != expected_clients:
            raise ValueError("setup plan clients are not in the closed order")

        raw_bindings = exact(top["bindings"], {"git", "pact", "graph", "runtime"}, "bindings")
        binding_fields = {
            "git": {"branch", "base_commit", "head_commit", "worktree_digest"},
            "pact": {"change_id", "task_id", "intent_digest"},
            "graph": {"generation", "branch"},
            "runtime": {"route_ref", "config_digest"},
        }
        bindings: dict[str, dict[str, str]] = {}
        for name, keys in binding_fields.items():
            value = exact(raw_bindings[name], keys, f"bindings.{name}")
            if any(not isinstance(item, str) or not item.strip() for item in value.values()):
                raise ValueError(f"bindings.{name} contains a malformed value")
            if name in {"pact", "runtime"}:
                digest_name = "intent_digest" if name == "pact" else "config_digest"
                if not _DIGEST_RE.fullmatch(value[digest_name]):
                    raise ValueError(f"bindings.{name}.{digest_name} must be a sha256 digest")
            if name == "runtime" and not value["route_ref"].startswith("opaque:"):
                raise ValueError("bindings.runtime.route_ref must be opaque")
            if name == "git" and not _DIGEST_RE.fullmatch(value["worktree_digest"]):
                raise ValueError("bindings.git.worktree_digest must be a sha256 digest")
            bindings[name] = value

        expected_checks = {
            "deployment", "ledger_empty", "operator_control", "identity", "hosts",
            "git", "okg", "capacity", "configuration_collision_free",
            "credential_references", "tracked_policy", "exact_bindings", "probe_complete",
            "client_routes",
        }
        checks = exact(top["checks"], expected_checks, "checks")
        if any(not isinstance(value, bool) for value in checks.values()):
            raise ValueError("checks values must be boolean")

        raw_mutations = top["mutations"]
        if not isinstance(raw_mutations, list) or len(raw_mutations) != 2:
            raise ValueError("setup plan mutations must contain exactly two operations")
        mutations: list[dict[str, Any]] = []
        for index, raw_mutation in enumerate(raw_mutations):
            value = dict(raw_mutation) if isinstance(raw_mutation, Mapping) else None
            if value is None or not all(isinstance(k, str) for k in value):
                raise ValueError(f"mutations[{index}] must be an object")
            if index == 0:
                keys = {"authority", "operation", "assignment_kind", "bindings"}
            else:
                keys = {"authority", "operation", "assignment_kind"}
            value = exact(value, keys, f"mutations[{index}]")
            if value.get("assignment_kind") != "chief":
                raise ValueError(f"mutations[{index}] must target the chief")
            if index == 0:
                if (
                    value["authority"] != "server_owned_workspace_lifecycle"
                    or value["operation"] != "stage_activate_workspace_pair"
                    or value["bindings"] != bindings
                ):
                    raise ValueError("workspace mutation is not bound to the plan")
            else:
                if (
                    value["authority"] != "authenticated_operator_control"
                    or value["operation"] != "bootstrap_native_coordination"
                ):
                    raise ValueError("native bootstrap mutation is malformed")
            mutations.append(value)

        if any(not isinstance(value, bool) for value in (top["read_ok"], top["ready"], top["decision_required"])):
            raise ValueError("setup plan status fields must be boolean")
        if not isinstance(top["blocked_by"], list) or any(not isinstance(item, str) or not item.strip() for item in top["blocked_by"]):
            raise ValueError("blocked_by must be a list of non-empty strings")
        if len(set(top["blocked_by"])) != len(top["blocked_by"]):
            raise ValueError("blocked_by contains duplicate reasons")
        if top["ready"] != (not top["blocked_by"]):
            raise ValueError("setup plan ready state is inconsistent with blockers")
        if top["ready"] != all(checks.values()):
            raise ValueError("setup plan ready state is inconsistent with checks")
        if top["ready"] and not top["read_ok"]:
            raise ValueError("ready setup plan must have successful reads")
        if not isinstance(top["next_action"], str) or not top["next_action"].strip():
            raise ValueError("next_action must be a non-empty string")
        next_action = top["next_action"].strip()
        unsigned = {
            "schema": "okg.coordination.setup-plan.v1",
            "deployment": deployment,
            "policy_digest": top["policy_digest"],
            "human_policy_digest": top["human_policy_digest"],
            "project_digest": top["project_digest"],
            "capacity_digest": top["capacity_digest"],
            "bindings": bindings,
            "clients": [client.to_json() for client in clients],
            "checks": checks,
            "mutations": mutations,
            "read_ok": top["read_ok"],
            "ready": top["ready"],
            "decision_required": top["decision_required"],
            "blocked_by": list(top["blocked_by"]),
            "next_action": next_action,
        }
        if _plan_digest(unsigned) != top["plan_digest"]:
            raise ValueError("setup plan digest does not match its contents")
        return cls(
            repo_root=Path(repo_root).resolve(),
            deployment=deployment,
            policy_digest=top["policy_digest"],
            human_policy_digest=top["human_policy_digest"],
            project_digest=top["project_digest"],
            capacity_digest=top["capacity_digest"],
            bindings=bindings,
            clients=tuple(clients),
            checks=checks,
            mutations=tuple(mutations),
            read_ok=top["read_ok"],
            ready=top["ready"],
            decision_required=top["decision_required"],
            blocked_by=tuple(top["blocked_by"]),
            next_action=next_action,
            plan_digest=top["plan_digest"],
        )

    @property
    def digest(self) -> str:
        return self.plan_digest

    @property
    def client_shapes(self) -> tuple[ClientShape, ...]:
        """Compatibility spelling used by setup callers and receipts."""
        return self.clients


def _plan_digest(unsigned: Mapping[str, Any]) -> str:
    return _digest(unsigned)


class SetupProbe(Protocol):
    def read_setup_state(self, *, repo_root: Path, deployment: str) -> Mapping[str, Any]: ...


class VizCoordinationError(SubstrateError):
    """Stable, redacted error for the constrained coordination transport."""


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        raise VizCoordinationError(
            "coordination service redirects are refused",
            data={"cause": "viz_redirect_refused"},
        )


def _operator_url_transport_is_safe(scheme: str, hostname: str | None) -> bool:
    """Permit bearer-token HTTP only on the local loopback interface."""
    if scheme == "https":
        return True
    host = str(hostname or "").strip().lower().rstrip(".")
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


class VizCoordinationClient:
    """Small environment-bound client for the two setup service paths.

    The service, not this client, owns current-state revalidation and operator
    authentication.  No constructor argument can supply a URL or token; tests
    may inject an environment mapping and a transport opener without exposing
    either value in a receipt or error.
    """

    def __init__(
        self,
        *,
        environment: Mapping[str, str] | None = None,
        opener: Any | None = None,
        timeout: float = 10.0,
    ) -> None:
        env = os.environ if environment is None else environment
        base_url = env.get("OKG_VIZ_URL", "") if isinstance(env, Mapping) else ""
        token = env.get("OKG_VIZ_OPERATOR_TOKEN", "") if isinstance(env, Mapping) else ""
        if not isinstance(base_url, str) or not base_url.strip():
            raise VizCoordinationError(
                "coordination service URL is unavailable",
                data={"cause": "viz_url_unavailable"},
            )
        if not isinstance(token, str) or not token.strip():
            raise VizCoordinationError(
                "coordination operator authentication is unavailable",
                data={"cause": "viz_operator_auth_unavailable"},
            )
        parsed = urlsplit(base_url.strip())
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.netloc
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or not _operator_url_transport_is_safe(parsed.scheme, parsed.hostname)
        ):
            raise VizCoordinationError(
                "coordination service URL is invalid",
                data={"cause": "viz_url_invalid"},
            )
        self._base_url = base_url.rstrip("/")
        self._token = token.strip()
        self._timeout = timeout
        self._opener = opener if opener is not None else build_opener(_NoRedirect())

    @classmethod
    def from_environment(
        cls,
        environment: Mapping[str, str] | None = None,
        *,
        opener: Any | None = None,
    ) -> "VizCoordinationClient":
        return cls(environment=environment, opener=opener)

    def _request(self, path: str, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        if path not in {_VIZ_PLAN_PATH, _VIZ_APPLY_PATH}:
            raise VizCoordinationError(
                "coordination service path is not allowed",
                data={"cause": "viz_path_refused"},
            )
        request = Request(
            self._base_url + path,
            data=json.dumps(dict(payload), ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8"),
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                _VIZ_OPERATOR_TOKEN_HEADER: self._token,
            },
            method="POST",
        )
        try:
            response = self._opener.open(request, timeout=self._timeout)
            status = response.getcode() if hasattr(response, "getcode") else 200
            if 300 <= status < 400:
                raise VizCoordinationError(
                    "coordination service redirects are refused",
                    data={"cause": "viz_redirect_refused"},
                )
            raw = response.read()
        except VizCoordinationError:
            raise
        except HTTPError as exc:
            if 300 <= exc.code < 400:
                raise VizCoordinationError(
                    "coordination service redirects are refused",
                    data={"cause": "viz_redirect_refused"},
                ) from exc
            raise VizCoordinationError(
                "coordination service request failed",
                data={"cause": "viz_service_unavailable"},
            ) from exc
        except (OSError, URLError, TimeoutError) as exc:
            raise VizCoordinationError(
                "coordination service request failed",
                data={"cause": "viz_service_unavailable"},
            ) from exc
        try:
            decoded = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
        except (UnicodeDecodeError, TypeError, json.JSONDecodeError) as exc:
            raise VizCoordinationError(
                "coordination service returned invalid JSON",
                data={"cause": "viz_response_invalid"},
            ) from exc
        if not isinstance(decoded, Mapping):
            raise VizCoordinationError(
                "coordination service returned an invalid response",
                data={"cause": "viz_response_invalid"},
            )
        return dict(decoded)

    def setup_plan(self, deployment: str) -> Mapping[str, Any]:
        deployment = _safe_token(deployment, field="deployment")
        response = self._request(_VIZ_PLAN_PATH, {"deployment": deployment})
        # The planner response is the canonical SetupPlan document itself.
        # Accepting a legacy ``{"setup": ...}`` wrapper would create a
        # second wire protocol that cannot be fed directly to
        # ``SetupPlan.from_json``.
        if not isinstance(response, Mapping) or "schema" not in response:
            raise VizCoordinationError(
                "coordination service returned an invalid setup plan",
                data={"cause": "viz_plan_invalid"},
            )
        return dict(response)

    def apply_setup(self, plan: SetupPlan) -> Mapping[str, Any]:
        if not isinstance(plan, SetupPlan):
            raise VizCoordinationError(
                "setup apply requires a typed setup plan",
                data={"cause": "plan_type_invalid"},
            )
        if plan.plan_digest != _plan_digest(plan._unsigned_payload()):
            raise VizCoordinationError(
                "setup plan digest is stale",
                data={"cause": "stale_setup_plan"},
            )
        # These are optimistic bindings, not a revalidation bypass.  The
        # bootstrap service must lock and re-read all current state before it
        # mutates anything.
        payload = {
            "operation": "bootstrap",
            # Native control canonicalizes the unsigned document.  Its digest
            # must therefore be exactly SetupPlan.plan_digest; including the
            # digest field in the submitted document would hash a different
            # object and permit a split plan protocol.
            "structured_plan": plan._unsigned_payload(),
            "claimed_plan_digest": plan.plan_digest,
            "expected_policy_digest": plan.policy_digest,
            "expected_project_digest": plan.project_digest,
            "idempotency_key": f"coord-setup:{plan.plan_digest}",
        }
        response = self._request(_VIZ_APPLY_PATH, payload)
        if response.get("plan_digest") != plan.plan_digest:
            raise VizCoordinationError(
                "coordination service returned an unbound setup receipt",
                data={"cause": "stale_setup_plan"},
            )
        return {
            key: value
            for key, value in response.items()
            if key in _SAFE_APPLY_RECEIPT_FIELDS
        }


def _read_probe(
    probe: SetupProbe | Mapping[str, Any] | Callable[..., Mapping[str, Any]] | None,
    *,
    repo_root: Path,
    deployment: str,
) -> tuple[dict[str, Any], bool]:
    if probe is None:
        return {}, False
    try:
        if isinstance(probe, Mapping):
            value = probe
        else:
            reader = getattr(probe, "read_setup_state", None)
            if callable(reader):
                value = reader(repo_root=repo_root, deployment=deployment)
            elif callable(probe):
                value = probe(repo_root=repo_root, deployment=deployment)
            else:
                return {}, False
    except Exception:  # noqa: BLE001 - probes are fail-closed seams
        return {}, False
    return (dict(value), True) if isinstance(value, Mapping) else ({}, False)


def _bool_check(state: Mapping[str, Any], key: str, missing: list[str]) -> bool:
    if key not in state or not isinstance(state[key], bool):
        missing.append(key)
        return False
    return state[key]


def _host_check(state: Mapping[str, Any], clients: Sequence[ClientShape], missing: list[str]) -> bool:
    value = state.get("host_reachability")
    if not isinstance(value, Mapping):
        missing.append("host_reachability")
        return False
    return all(
        isinstance(value.get(shape.name), bool) and value.get(shape.name) is True
        for shape in clients
    )


def _capacity_check(state: Mapping[str, Any], missing: list[str]) -> bool:
    value = state.get("capacity")
    if isinstance(value, bool):
        return value
    if isinstance(value, Mapping) and isinstance(value.get("ok"), bool):
        return value["ok"]
    missing.append("capacity")
    return False


def _collision_check(state: Mapping[str, Any], missing: list[str]) -> bool:
    value = state.get("configuration_collisions")
    if isinstance(value, list):
        return not value
    missing.append("configuration_collisions")
    return False


def _credential_check(
    state: Mapping[str, Any], clients: Sequence[ClientShape], missing: list[str]
) -> bool:
    value = state.get("credential_references")
    if not isinstance(value, Mapping):
        missing.append("credential_references")
        return False
    valid = True
    for shape in clients:
        reference = value.get(shape.name)
        if not isinstance(reference, str) or not _REFERENCE_RE.fullmatch(reference):
            valid = False
    return valid


def _binding_check(state: Mapping[str, Any], missing: list[str]) -> dict[str, Any] | None:
    """Validate and redact the exact setup identities supplied by a probe."""
    required: dict[str, tuple[str, ...]] = {
        "git": ("branch", "base_commit", "head_commit", "worktree"),
        "pact": ("change_id", "task_id", "intent_digest"),
        "graph": ("generation", "branch"),
        "runtime": ("route_ref", "config_digest"),
    }
    raw_bindings: dict[str, Any] = {}
    for name, fields in required.items():
        value = state.get(f"{name}_binding")
        if not isinstance(value, Mapping):
            missing.append(f"{name}_binding")
            continue
        if any(
            field_name not in value
            or not isinstance(value[field_name], str)
            or not value[field_name].strip()
            for field_name in fields
        ):
            missing.append(f"{name}_binding")
            continue
        raw_bindings[name] = dict(value)
    if len(raw_bindings) != len(required):
        return None
    runtime_route = raw_bindings["runtime"]["route_ref"]
    if not runtime_route.startswith("opaque:"):
        missing.append("runtime_route_ref")
    for name, field_name in (("pact", "intent_digest"), ("runtime", "config_digest")):
        if not _DIGEST_RE.fullmatch(raw_bindings[name][field_name]):
            missing.append(f"{name}_{field_name}")
    if missing:
        return None
    # Worktree paths are exact inputs to the digest but not output authority.
    # Commit/generation/action identifiers are already non-secret immutable
    # references and remain readable for an operator reviewing the plan.
    return {
        "git": {
            "branch": raw_bindings["git"]["branch"],
            "base_commit": raw_bindings["git"]["base_commit"],
            "head_commit": raw_bindings["git"]["head_commit"],
            "worktree_digest": _digest(raw_bindings["git"]["worktree"]),
        },
        "pact": {
            "change_id": raw_bindings["pact"]["change_id"],
            "task_id": raw_bindings["pact"]["task_id"],
            "intent_digest": raw_bindings["pact"]["intent_digest"],
        },
        "graph": {
            "generation": raw_bindings["graph"]["generation"],
            "branch": raw_bindings["graph"]["branch"],
        },
        "runtime": {
            "route_ref": runtime_route,
            "config_digest": raw_bindings["runtime"]["config_digest"],
        },
    }


def _safe_next_action(deployment: str, blocked: Sequence[str]) -> str:
    if "deployment_missing" in blocked:
        return f"uv run okg provision --deployment {deployment} --json"
    if blocked:
        return "repair the listed setup blockers, then rerun `okg pact coord setup plan`"
    return "review the exact plan, then use the authenticated operator-control apply"


def build_setup_plan(
    repo_root: Path,
    deployment: str,
    *,
    probe: SetupProbe | Mapping[str, Any] | Callable[..., Mapping[str, Any]] | None = None,
) -> SetupPlan:
    """Read current setup prerequisites and return a deterministic plan.

    A missing probe is intentionally not treated as an empty deployment.  It
    produces an incomplete, refused plan rather than guessing at readiness.
    """
    repo_root = Path(repo_root).resolve()
    deployment = _safe_token(deployment, field="deployment")

    policy: CoordinationPolicy | None = None
    try:
        policy = coordination_policy(repo_root)
    except (OSError, SubstrateError, TypeError, ValueError):
        pass
    selected, client_routes_ready = _policy_client_shapes(policy)

    state, probe_ok = _read_probe(probe, repo_root=repo_root, deployment=deployment)
    missing: list[str] = []
    deployment_exists = _bool_check(state, "deployment_exists", missing)
    deployment_ready = _bool_check(state, "deployment_ready", missing)
    ledger_empty = _bool_check(state, "ledger_empty", missing)
    authority = _bool_check(state, "operator_control_available", missing)
    git_ready = _bool_check(state, "git_ready", missing)
    okg_ready = _bool_check(state, "okg_ready", missing)
    identity_ready = _bool_check(state, "identity_ready", missing)
    hosts_ready = _host_check(state, selected, missing)
    capacity_ready = _capacity_check(state, missing)
    collisions_clear = _collision_check(state, missing)
    credentials_safe = _credential_check(state, selected, missing)
    bindings = _binding_check(state, missing)
    policy_ready = policy is not None

    blocked: list[str] = []
    if not probe_ok or missing:
        blocked.append("setup_readiness_unavailable")
    if not deployment_exists:
        blocked.append("deployment_missing")
    elif not deployment_ready:
        blocked.append("deployment_not_ready")
    if not ledger_empty:
        blocked.append("coordination_ledger_not_empty")
    if not authority:
        blocked.append("operator_control_unavailable")
    if not identity_ready:
        blocked.append("identity_unavailable")
    if not hosts_ready:
        blocked.append("host_unavailable")
    if not git_ready:
        blocked.append("git_unready")
    if not okg_ready:
        blocked.append("okg_unready")
    if not capacity_ready:
        blocked.append("capacity_insufficient")
    if not collisions_clear:
        blocked.append("configuration_collision")
    if not credentials_safe:
        blocked.append("credential_reference_invalid")
    if not policy_ready:
        blocked.append("tracked_policy_unavailable")
    if not client_routes_ready:
        blocked.append("client_routes_unavailable")
    if bindings is None:
        blocked.append("exact_bindings_unavailable")
    # Preserve order while preventing a malformed probe from duplicating causes.
    blocked = list(dict.fromkeys(blocked))

    policy_digest = policy.policy_digest if policy else ""
    human_policy_digest = policy.human_policy_digest if policy else ""
    project_digest = policy.project_digest if policy else ""
    # Bind both the tracked capacity policy and the live authority snapshot.
    # A later admission/capacity change must invalidate this plan even when
    # the coarse ``capacity.ok`` result remains true.
    capacity_digest = _digest(
        {
            "policy_digest": policy.capacity_digest if policy else "",
            "observed": state.get("capacity"),
        }
    )
    checks: dict[str, Any] = {
        "deployment": deployment_exists and deployment_ready,
        "ledger_empty": ledger_empty,
        "operator_control": authority,
        "identity": identity_ready,
        "hosts": hosts_ready,
        "git": git_ready,
        "okg": okg_ready,
        "capacity": capacity_ready,
        "configuration_collision_free": collisions_clear,
        "credential_references": credentials_safe,
        "tracked_policy": policy_ready,
        "client_routes": client_routes_ready,
        "exact_bindings": bindings is not None,
        "probe_complete": probe_ok and not missing,
    }
    # Routes and policy are tracked/read authorities.  They are not setup
    # writes.  Keep the plan honest by listing only the two actual mutations:
    # the server-owned pair lifecycle and the native chief/control bootstrap.
    mutations: list[Mapping[str, Any]] = [
        {
            "authority": "server_owned_workspace_lifecycle",
            "operation": "stage_activate_workspace_pair",
            "assignment_kind": "chief",
            "bindings": bindings or {},
        },
        {
            "authority": "authenticated_operator_control",
            "operation": "bootstrap_native_coordination",
            "assignment_kind": "chief",
        },
    ]
    unsigned = {
        "schema": "okg.coordination.setup-plan.v1",
        "deployment": deployment,
        "policy_digest": policy_digest,
        "human_policy_digest": human_policy_digest,
        "project_digest": project_digest,
        "capacity_digest": capacity_digest,
        "bindings": bindings or {},
        "clients": [shape.to_json() for shape in selected],
        "checks": checks,
        "mutations": [dict(row) for row in mutations],
        "read_ok": probe_ok and policy_ready and not missing,
        "ready": not blocked,
        "decision_required": not blocked and bool(state.get("decision_required", True)),
        "blocked_by": blocked,
        "next_action": _safe_next_action(deployment, blocked),
    }
    return SetupPlan(
        repo_root=repo_root,
        deployment=deployment,
        policy_digest=policy_digest,
        human_policy_digest=human_policy_digest,
        project_digest=project_digest,
        capacity_digest=capacity_digest,
        bindings=bindings or {},
        clients=selected,
        checks=checks,
        mutations=tuple(mutations),
        read_ok=bool(unsigned["read_ok"]),
        ready=bool(unsigned["ready"]),
        decision_required=bool(unsigned["decision_required"]),
        blocked_by=tuple(blocked),
        next_action=str(unsigned["next_action"]),
        plan_digest=_plan_digest(unsigned),
    )


# Descriptive alias keeps callers from mistaking this planner for an authority
# store while leaving one implementation and one digest contract. Setup apply
# and assignment start intentionally have no local/injected seam: they must run
# through authenticated production services.
build_coordination_setup_plan = build_setup_plan


__all__ = [
    "LOCAL_CLIENT_SHAPES",
    "client_shapes_for_routes",
    "ClientShape",
    "SetupPlan",
    "SetupProbe",
    "VizCoordinationClient",
    "VizCoordinationError",
    "build_setup_plan",
    "build_coordination_setup_plan",
]
