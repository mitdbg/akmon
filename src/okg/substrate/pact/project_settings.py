"""Repository-level PACT settings.

`pact/project.yaml` is tracked semantic policy. Worktree-local files under
`.okg/` are runtime context only; this module reports them but never lets them
drive PACT artifact semantics.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
from types import MappingProxyType
from typing import Any, Mapping

import yaml

from okg.substrate import fastyaml
from okg.substrate.errors import SubstrateError
from okg.substrate.pact.evaluation_context import PactEvaluationContext
from okg.substrate.pact.decision_policy import (
    DecisionPolicyError,
    HumanInvolvementPolicy,
    parse_human_involvement_policy,
)
from okg.substrate.pact.store import load_yaml_mapping


PROJECT_SETTINGS_RELATIVE_PATH = "pact/project.yaml"

#: okg agent-runtime policy, moved out of the PACT contract file by
#: `pact-v5-corpus-cutover`. PACT v5 has no coordination writer and no graph
#: projection, so these blocks are okg's, not the workflow's.
RUNTIME_SETTINGS_RELATIVE_PATH = ".okg/coordination.yaml"
DEFAULT_LOCAL_SETTING_PATHS = (
    ".okg/local.env",
    ".okg/worktree.yaml",
    ".okg/pact-local.yaml",
)


@dataclass(frozen=True)
class PactVersionDefaults:
    artifact_format_version: int = 0
    target_pact_version: int | None = None
    source: str = PROJECT_SETTINGS_RELATIVE_PATH
    compatibility_mode: bool = True

    def to_json(self) -> dict[str, Any]:
        return {
            "artifact_format_version": self.artifact_format_version,
            "target_pact_version": self.target_pact_version,
            "source": self.source,
            "compatibility_mode": self.compatibility_mode,
        }


@dataclass(frozen=True, slots=True)
class CoordinationLeasePolicy:
    """The four lease windows used by the guarded coordination writer.

    The values are policy, not caller input.  Keeping them in one immutable
    value object prevents a writer from accidentally mixing values read from
    different settings sources during one transaction.
    """

    chief: int
    sprint: int
    pact: int
    delegation: int

    def to_json(self) -> dict[str, int]:
        return {
            "chief": self.chief,
            "sprint": self.sprint,
            "pact": self.pact,
            "delegation": self.delegation,
        }

    def __getitem__(self, kind: str) -> int:
        """Provide mapping-style access for writer call sites."""
        try:
            return self.to_json()[kind]
        except KeyError as exc:
            raise KeyError(f"unknown coordination lease kind: {kind!r}") from exc


@dataclass(frozen=True, slots=True)
class CoordinationExecutionProfile:
    """One named execution reservation profile.

    ``lane`` describes the resource lane reserved by the profile.  It is not
    a priority or requirement-importance marker.  Resource quantities are
    copied into a read-only mapping so a loaded policy cannot be changed by a
    caller after validation.
    """

    lane: str
    resources: Mapping[str, int]

    def __post_init__(self) -> None:
        object.__setattr__(self, "resources", MappingProxyType(dict(self.resources)))

    def to_json(self) -> dict[str, Any]:
        return {
            "lane": self.lane,
            "resources": dict(self.resources),
        }


@dataclass(frozen=True, slots=True)
class CoordinationHelperBudget:
    """Shared admission ceiling for direct helpers of one parent role."""

    profile: str
    max_active: int

    def to_json(self) -> dict[str, Any]:
        return {
            "profile": self.profile,
            "max_active": self.max_active,
        }


@dataclass(frozen=True, slots=True)
class CoordinationExecutionPolicy:
    """Validated lane, named-resource, and profile admission capacity."""

    lane_capacity: Mapping[str, int]
    named_resource_capacity: Mapping[str, int]
    profiles: Mapping[str, CoordinationExecutionProfile]
    helper_budgets: Mapping[str, CoordinationHelperBudget]

    def __post_init__(self) -> None:
        object.__setattr__(self, "lane_capacity", MappingProxyType(dict(self.lane_capacity)))
        object.__setattr__(
            self,
            "named_resource_capacity",
            MappingProxyType(dict(self.named_resource_capacity)),
        )
        object.__setattr__(self, "profiles", MappingProxyType(dict(self.profiles)))
        object.__setattr__(
            self,
            "helper_budgets",
            MappingProxyType(dict(self.helper_budgets)),
        )

    def profile(self, name: str) -> CoordinationExecutionProfile:
        """Return one named profile, refusing unknown profile names."""
        try:
            return self.profiles[name]
        except KeyError as exc:
            raise SubstrateError(
                f"unknown coordination execution profile: {name!r}",
                data={"field": "coordination.execution.profiles", "value": name},
            ) from exc

    def to_json(self) -> dict[str, Any]:
        return {
            "lane_capacity": dict(self.lane_capacity),
            "named_resource_capacity": dict(self.named_resource_capacity),
            "profiles": {
                name: profile.to_json() for name, profile in self.profiles.items()
            },
            "helper_budgets": {
                name: budget.to_json() for name, budget in self.helper_budgets.items()
            },
        }


@dataclass(frozen=True, slots=True)
class CoordinationPolicy:
    """Validated, tracked policy consumed by the guarded coordination writer."""

    agent_transport: str
    operator_transport: str
    lease_ttl_seconds: CoordinationLeasePolicy
    execution: CoordinationExecutionPolicy
    client_routes: Mapping[str, str]
    policy_digest: str
    human_policy: HumanInvolvementPolicy
    human_policy_digest: str
    project_digest: str
    capacity_digest: str
    source: str = PROJECT_SETTINGS_RELATIVE_PATH

    @property
    def remote_client_route_names(self) -> tuple[str, ...]:
        """The configured remote route names, sorted."""
        return tuple(
            sorted(name for name in self.client_routes if name not in LOCAL_CLIENT_ROUTE_NAMES)
        )

    @property
    def client_route_names(self) -> tuple[str, ...]:
        """All route names in the closed order: local routes, then remote ones."""
        return LOCAL_CLIENT_ROUTE_NAMES + self.remote_client_route_names

    @property
    def leases(self) -> CoordinationLeasePolicy:
        """Compatibility spelling for callers that refer to lease policy."""
        return self.lease_ttl_seconds

    @property
    def execution_policy(self) -> CoordinationExecutionPolicy:
        """Compatibility spelling for callers that name the policy explicitly."""
        return self.execution

    def profile(self, name: str) -> CoordinationExecutionProfile:
        """Return a named execution profile from the tracked policy."""
        return self.execution.profile(name)

    def execution_profile(self, name: str) -> CoordinationExecutionProfile:
        """Explicit spelling for callers resolving an execution profile."""
        return self.execution.profile(name)

    @property
    def human_involvement_profiles(self) -> Mapping[str, Any]:
        """Expose the typed profile map without making it mutable."""
        return self.human_policy.profiles

    @property
    def action_registry(self) -> Mapping[str, Any]:
        return self.human_policy.actions

    def evaluate_action(self, action: str, context: Any, *, profile: str | None = None) -> Any:
        """Evaluate a mutation against the tracked human policy."""
        return self.human_policy.evaluate(action, context, profile=profile)

    def to_json(self) -> dict[str, Any]:
        return {
            "agent_transport": self.agent_transport,
            "operator_transport": self.operator_transport,
            "lease_ttl_seconds": self.lease_ttl_seconds.to_json(),
            "execution": self.execution.to_json(),
            "client_routes": dict(self.client_routes),
            "policy_digest": self.policy_digest,
            "human_policy": self.human_policy.to_json(),
            "human_policy_digest": self.human_policy_digest,
            "project_digest": self.project_digest,
            "capacity_digest": self.capacity_digest,
            "source": self.source,
        }


def project_settings_path(repo_root: Path) -> Path:
    return repo_root / PROJECT_SETTINGS_RELATIVE_PATH


def load_project_settings(repo_root: Path) -> dict[str, Any]:
    path = project_settings_path(repo_root)
    if not path.is_file():
        return {}
    return load_yaml_mapping(path)


def runtime_settings_path(repo_root: Path) -> Path:
    return repo_root / RUNTIME_SETTINGS_RELATIVE_PATH


def runtime_settings_source(repo_root: Path) -> Path:
    """The file a runtime block was actually read from.

    `load_runtime_settings` falls back to `pact/project.yaml` when
    `.okg/coordination.yaml` is absent, so an error that always attached the
    PACT settings path pointed the reader at a file that may not contain the
    block it is complaining about.
    """
    path = runtime_settings_path(repo_root)
    return path if path.is_file() else project_settings_path(repo_root)


def load_runtime_settings(repo_root: Path) -> dict[str, Any]:
    """okg runtime policy (`.okg/coordination.yaml`).

    A test repo that writes only `pact/project.yaml` keeps working: the blocks
    are read from there when the runtime file is absent, so moving the file
    did not invalidate every fixture that declares a coordination policy
    inline. A repo carrying BOTH has the runtime file win for the blocks it
    declares -- it is the one this repository ships.
    """
    merged = dict(load_project_settings(repo_root))
    path = runtime_settings_path(repo_root)
    if path.is_file():
        merged.update(load_yaml_mapping(path))
    return merged


_COORDINATION_FIELDS = frozenset(
    {
        "agent_transport",
        "operator_transport",
        "lease_ttl_seconds",
        "execution",
        "client_routes",
        "human_involvement_profiles",
        "action_registry",
        "escalation_route",
        "decision_suppression",
        "default_profile",
    }
)
_HUMAN_POLICY_FIELDS = frozenset(
    {
        "human_involvement_profiles",
        "action_registry",
        "escalation_route",
        "decision_suppression",
        "default_profile",
    }
)
_LEASE_KINDS = frozenset({"chief", "sprint", "pact", "delegation"})
_EXECUTION_FIELDS = frozenset(
    {"lane_capacity", "named_resource_capacity", "profiles", "helper_budgets"}
)
_LANE_CAPACITY_FIELDS = frozenset({"critical", "light"})
_EXECUTION_PROFILE_FIELDS = frozenset({"lane", "resources"})
_HELPER_BUDGET_FIELDS = frozenset({"profile", "max_active"})
_HELPER_PARENT_KINDS = frozenset({"chief", "sprint", "pact"})
_EXECUTION_LANES = frozenset({"critical", "light"})
_AGENT_TRANSPORT = "restricted_ssh_stdio"
_OPERATOR_TRANSPORT = "viz_operator_token"
LOCAL_CLIENT_ROUTE_NAMES: tuple[str, ...] = ("local-claude", "local-codex")
"""The two local client routes every deployment declares."""

REMOTE_CLIENT_ROUTE_COUNT = 2
"""How many remote codex routes the closed four-shape set carries."""

# Remote route names are deployment data, not source constants: each
# deployment names its own remote machines in `.okg/coordination.yaml`, so no
# real machine name lives in the source. A name is one lower-case token and
# may not claim the reserved ``local-`` prefix, which marks a local route.
_REMOTE_ROUTE_NAME = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,62}$")
_OPAQUE_ROUTE = re.compile(r"^opaque:[A-Za-z0-9_.:-]+$")


def coordination_policy(repo_root: Path) -> CoordinationPolicy:
    """Load and validate the tracked coordination writer policy.

    Only tracked policy is consulted (``.okg/coordination.yaml``, falling back
    to ``pact/project.yaml``).  In particular, this function
    does not inspect ``.okg`` overlays or environment variables: those are
    runtime context and cannot override semantic writer authority.
    """

    settings = load_runtime_settings(repo_root)
    raw = settings.get("coordination")
    if not isinstance(raw, dict):
        _coordination_error(
            ".okg/coordination.yaml coordination must be a mapping with the writer policy"
        )
    _require_string_keys(raw, "coordination")
    extra = sorted(set(raw) - _COORDINATION_FIELDS)
    if extra:
        _coordination_error(
            f"coordination has unknown field(s): {', '.join(extra)}",
            field="coordination",
        )
    present_human = set(raw).intersection(_HUMAN_POLICY_FIELDS)
    if present_human != _HUMAN_POLICY_FIELDS:
        missing = sorted(_HUMAN_POLICY_FIELDS - present_human)
        _coordination_error(
            "coordination is missing required human-policy field(s): "
            + ", ".join(missing),
            field="coordination",
        )
    missing = sorted(set(_COORDINATION_FIELDS) - set(raw))
    if missing:
        _coordination_error(
            "coordination is missing required field(s): " + ", ".join(missing),
            field="coordination",
        )

    agent_transport = _require_transport(
        raw["agent_transport"],
        field="coordination.agent_transport",
        expected=_AGENT_TRANSPORT,
    )
    operator_transport = _require_transport(
        raw["operator_transport"],
        field="coordination.operator_transport",
        expected=_OPERATOR_TRANSPORT,
    )

    lease_raw = raw["lease_ttl_seconds"]
    if not isinstance(lease_raw, dict):
        _coordination_error(
            ".okg/coordination.yaml coordination.lease_ttl_seconds must be a mapping"
        )
    _require_string_keys(lease_raw, "coordination.lease_ttl_seconds")
    _require_exact_keys(lease_raw, _LEASE_KINDS, "coordination.lease_ttl_seconds")
    lease_values = {
        kind: _require_positive_ttl(
            lease_raw[kind],
            field=f"coordination.lease_ttl_seconds.{kind}",
        )
        for kind in ("chief", "sprint", "pact", "delegation")
    }

    execution = _parse_execution_policy(raw["execution"])
    client_routes = raw["client_routes"]
    if not isinstance(client_routes, dict):
        _coordination_error(
            "coordination.client_routes must be a mapping",
            field="coordination.client_routes",
        )
    _require_string_keys(client_routes, "coordination.client_routes")
    _require_client_route_names(client_routes)
    if any(
        not isinstance(route, str) or not _OPAQUE_ROUTE.fullmatch(route)
        for route in client_routes.values()
    ):
        _coordination_error(
            "coordination.client_routes values must be opaque route references",
            field="coordination.client_routes",
        )
    client_routes = {name: client_routes[name] for name in sorted(client_routes)}
    try:
        human_policy = parse_human_involvement_policy(
            raw["action_registry"],
            raw["human_involvement_profiles"],
            raw["escalation_route"],
            raw["decision_suppression"],
            default_profile=raw["default_profile"],
        )
    except DecisionPolicyError as exc:
        _coordination_error(str(exc), field="coordination.human_policy")

    policy_payload = {
        "agent_transport": agent_transport,
        "operator_transport": operator_transport,
        "lease_ttl_seconds": lease_values,
        "client_routes": client_routes,
    }
    project_payload = dict(settings)
    coordination_payload = dict(raw)
    coordination_payload.pop("execution", None)
    for field_name in _HUMAN_POLICY_FIELDS:
        coordination_payload.pop(field_name, None)
    project_payload["coordination"] = coordination_payload
    return CoordinationPolicy(
        agent_transport=agent_transport,
        operator_transport=operator_transport,
        lease_ttl_seconds=CoordinationLeasePolicy(**lease_values),
        execution=execution,
        client_routes=MappingProxyType(client_routes),
        policy_digest=_canonical_digest(policy_payload),
        human_policy=human_policy,
        human_policy_digest=human_policy.digest,
        project_digest=_canonical_digest(project_payload),
        capacity_digest=_canonical_digest(execution.to_json()),
        source=PROJECT_SETTINGS_RELATIVE_PATH,
    )


def load_coordination_policy(repo_root: Path) -> CoordinationPolicy:
    """Alias for :func:`coordination_policy` used by loader-oriented callers."""

    return coordination_policy(repo_root)


def coordination_policy_digests(repo_root: Path) -> dict[str, str]:
    """Return the policy and full tracked-project digests for writer binding."""

    policy = coordination_policy(repo_root)
    return {
        "policy_digest": policy.policy_digest,
        "human_policy_digest": policy.human_policy_digest,
        "project_digest": policy.project_digest,
        "capacity_digest": policy.capacity_digest,
    }


def coordination_policy_digest(repo_root: Path) -> str:
    """Return the digest of the validated coordination policy."""

    return coordination_policy(repo_root).policy_digest


def coordination_human_policy_digest(repo_root: Path) -> str:
    """Return the digest of the tracked human-involvement policy."""

    return coordination_policy(repo_root).human_policy_digest


def project_settings_digest(repo_root: Path) -> str:
    """Return the digest of the validated tracked project settings."""

    return coordination_policy(repo_root).project_digest


def coordination_capacity_digest(repo_root: Path) -> str:
    """Return the digest of the validated execution-capacity policy."""

    return coordination_policy(repo_root).capacity_digest


def legacy_missing_tier_allowed(
    repo_root: Path,
    change_id: str,
    *,
    context: PactEvaluationContext | None = None,
) -> bool:
    """Whether one tierless v4 artifact belongs to the declared old cohort.

    A numeric default alone is deliberately insufficient: it would admit every
    newly authored tierless manifest forever. Repositories may name a small
    explicit id set or a tracked newline-delimited snapshot. New paths must
    persist ``change.tier``. The snapshot is deliberately available at HEAD
    so compatibility does not depend on checkout depth or remote history.
    """
    captured_files = context is not None and context.files is not None
    if captured_files:
        lookup = context.file_lookup(repo_root, PROJECT_SETTINGS_RELATIVE_PATH)
        if not lookup.exists or lookup.content is None:
            return False
        try:
            settings = fastyaml.safe_load(lookup.content.decode("utf-8"))
        except (UnicodeDecodeError, yaml.YAMLError) as exc:
            raise SubstrateError(
                "captured pact/project.yaml is invalid YAML",
                data={"path": PROJECT_SETTINGS_RELATIVE_PATH, "yaml_error": str(exc)},
            ) from exc
        if not isinstance(settings, dict):
            raise SubstrateError(
                "captured pact/project.yaml must be a mapping",
                data={"path": PROJECT_SETTINGS_RELATIVE_PATH},
            )
    else:
        settings = load_project_settings(repo_root)
    defaults = _mapping(settings.get("pact_defaults"))
    if defaults.get("legacy_missing_tier") != 1:
        return False
    explicit_ids = _string_list(defaults.get("legacy_missing_tier_ids"))
    if change_id in explicit_ids:
        return True
    snapshot = defaults.get("legacy_missing_tier_snapshot")
    if not isinstance(snapshot, str) or not snapshot.strip():
        return False
    snapshot_ref = snapshot.strip()
    if captured_files:
        assert context is not None
        root = context.resolve_repo_path(repo_root, ".")
        snapshot_path = context.resolve_repo_path(repo_root, snapshot_ref)
        try:
            relative = snapshot_path.relative_to(root)
        except ValueError:
            return False
        if relative.parts[:1] != ("pact",):
            return False
        lookup = context.file_lookup(repo_root, snapshot_ref)
        if not lookup.exists or lookup.content is None:
            return False
        try:
            snapshot_text = lookup.content.decode("utf-8")
        except UnicodeDecodeError:
            return False
    else:
        snapshot_path = Path(repo_root) / snapshot_ref
        try:
            relative = snapshot_path.resolve().relative_to(Path(repo_root).resolve())
        except (OSError, ValueError):
            return False
        if relative.parts[:1] != ("pact",) or not snapshot_path.is_file():
            return False
        try:
            snapshot_text = snapshot_path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            return False
    members = {
        line.strip()
        for line in snapshot_text.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }
    return change_id in members


def pr_base_policy(repo_root: Path, default: str = "dev") -> str:
    """The branch PRs must target, from pact/project.yaml `pr_policy.base`."""
    policy = _mapping(load_project_settings(repo_root).get("pr_policy"))
    base = policy.get("base")
    return base if isinstance(base, str) and base else default


def graph_projection_deployment(repo_root: Path) -> str | None:
    """Return the repository-owned PACT graph/ledger deployment.

    Missing policy is represented by ``None`` so authority callers can emit a
    purpose-built recovery message.  A present but malformed block is a loud
    configuration error, never permission to fall back to another repository's
    deployment.
    """
    settings = load_runtime_settings(repo_root)
    raw = settings.get("graph_projection")
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise SubstrateError(
            ".okg/coordination.yaml graph_projection must be a mapping",
            data={"path": str(runtime_settings_source(repo_root))},
        )
    deployment = raw.get("deployment")
    if not isinstance(deployment, str) or not deployment.strip():
        raise SubstrateError(
            ".okg/coordination.yaml graph_projection.deployment must be a non-empty string",
            data={"path": str(runtime_settings_source(repo_root))},
        )
    return deployment.strip()


PACT_GRAPH_DEPLOYMENT_ENV = "OKG_PACT_GRAPH_DEPLOYMENT"
PACT_GRAPH_DEPLOYMENT_FIX = (
    "set OKG_PACT_GRAPH_DEPLOYMENT=<deployment> or add "
    "graph_projection.deployment to .okg/coordination.yaml"
)


def resolve_pact_graph_deployment(repo_root: Path | None) -> str | None:
    """The deployment PACT's graph surfaces read, or ``None`` when unconfigured.

    ``OKG_PACT_GRAPH_DEPLOYMENT`` wins, then the repository's
    ``graph_projection.deployment``. There is no built-in default: a
    repository that configures neither gets ``None`` and each surface says so
    (with :data:`PACT_GRAPH_DEPLOYMENT_FIX`) instead of reporting some other
    repository's deployment as unavailable. A malformed block still raises.
    """
    import os

    env = (os.environ.get(PACT_GRAPH_DEPLOYMENT_ENV) or "").strip()
    if env:
        return env
    if repo_root is None:
        return None
    return graph_projection_deployment(repo_root)


def pact_version_defaults(repo_root: Path) -> PactVersionDefaults:
    # A repo WITHOUT pact/project.yaml — or with one that declares no
    # `pact_defaults` block (e.g. added only for pr_policy) — scaffolds at
    # the CURRENT language version (the validator's constant), never 0:
    # the first PACT in a fresh repo must be valid without cribbing a
    # sibling repo's shape (issue #643, polish-pact-first-run-dx
    # req.pactdx.version-stamp; adversarial-review M3 closed the
    # config-file-without-defaults gap). Only DECLARED defaults win.
    settings = load_project_settings(repo_root)
    defaults = _mapping(settings.get("pact_defaults"))
    if not defaults:
        from okg.substrate.pact.validation import CURRENT_ARTIFACT_FORMAT_VERSION

        return PactVersionDefaults(
            artifact_format_version=CURRENT_ARTIFACT_FORMAT_VERSION,
            target_pact_version=CURRENT_ARTIFACT_FORMAT_VERSION,
            source="builtin:current-language-version",
            compatibility_mode=False,
        )
    artifact = _non_negative_int(
        defaults.get("artifact_format_version"),
        field="pact_defaults.artifact_format_version",
        default=0,
    )
    target_raw = defaults.get("target_pact_version")
    target = (
        None
        if target_raw is None
        else _non_negative_int(
            target_raw,
            field="pact_defaults.target_pact_version",
        )
    )
    return PactVersionDefaults(
        artifact_format_version=artifact,
        target_pact_version=target,
        compatibility_mode=artifact == 0,
    )


def artifact_inventory(repo_root: Path) -> list[dict[str, Any]]:
    settings = load_project_settings(repo_root)
    inventory = settings.get("artifact_inventory")
    if inventory is None:
        return []
    if not isinstance(inventory, list) or not all(isinstance(row, dict) for row in inventory):
        raise SubstrateError(
            "pact/project.yaml artifact_inventory must be a list of mappings",
            data={"path": str(project_settings_path(repo_root))},
        )
    return list(inventory)


def worktree_local_settings_report(repo_root: Path) -> dict[str, Any]:
    settings = load_project_settings(repo_root)
    policy = _mapping(settings.get("worktree_local_settings"))
    paths = _string_list(policy.get("allowed_paths")) or list(DEFAULT_LOCAL_SETTING_PATHS)
    forbidden = set(_string_list(policy.get("forbidden_semantic_keys")))
    runtime_keys = set(_string_list(policy.get("runtime_authority_keys")))
    overlays: list[dict[str, Any]] = []
    warnings: list[str] = []
    for rel_path in paths:
        path = repo_root / rel_path
        if not path.exists():
            continue
        row: dict[str, Any] = {
            "path": rel_path,
            "policy": "runtime_only",
        }
        if path.is_dir():
            row["kind"] = "directory"
        elif path.suffix in {".yaml", ".yml"}:
            row["kind"] = "yaml"
            data = load_yaml_mapping(path)
            present_forbidden = sorted(forbidden.intersection(data))
            present_runtime = sorted(runtime_keys.intersection(data))
            row["runtime_authority_keys"] = present_runtime
            row["ignored_semantic_keys"] = present_forbidden
            for key in present_forbidden:
                warnings.append(
                    f"{rel_path}:{key} ignored; semantic PACT policy comes from {PROJECT_SETTINGS_RELATIVE_PATH}"
                )
        else:
            row["kind"] = "env"
            keys = _read_env_keys(path)
            row["runtime_authority_keys"] = sorted(runtime_keys.intersection(keys))
        overlays.append(row)
    return {
        "policy_source": PROJECT_SETTINGS_RELATIVE_PATH,
        "semantic_overrides_allowed": False,
        "overlays": overlays,
        "warnings": warnings,
    }


def audit_pact_versions(repo_root: Path) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    changes_root = repo_root / "pact" / "changes"
    for path in sorted(changes_root.glob("*/pact.yaml")):
        data = load_yaml_mapping(path)
        change = _mapping(data.get("change"))
        pact_version = data.get("pact_version", 0)
        target_version = data.get("target_pact_version")
        rows.append(
            {
                "change": path.parent.name,
                "path": str(path.relative_to(repo_root)),
                "status": change.get("status"),
                "pact_version": pact_version,
                "target_pact_version": target_version,
                "compatibility_mode": pact_version == 0 and target_version is not None,
            }
        )
    counts: dict[str, int] = {}
    for row in rows:
        key = (
            f"artifact={row['pact_version']} target="
            f"{row['target_pact_version'] if row['target_pact_version'] is not None else 'unset'}"
        )
        counts[key] = counts.get(key, 0) + 1
    return {
        "open_total": len(rows),
        "counts": counts,
        "rows": rows,
        "policy": (
            "This audit is read-only. Repo defaults affect new PACTs only; "
            "migrating existing manifests requires an explicit follow-up PACT."
        ),
    }


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        return []
    return list(value)


def _non_negative_int(value: Any, *, field: str, default: int | None = None) -> int:
    if value is None and default is not None:
        return default
    if not isinstance(value, int) or value < 0:
        raise SubstrateError(
            f"{field} must be a non-negative integer",
            data={"field": field, "value": value},
        )
    return value


def _read_env_keys(path: Path) -> set[str]:
    keys: set[str] = set()
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        keys.add(line.split("=", 1)[0].strip())
    return keys


def _require_client_route_names(client_routes: dict[str, Any]) -> None:
    """Require the two local routes plus exactly two configured remote routes.

    There is no default remote route: a deployment that does not name its
    remote routes gets this error instead of a built-in machine name.
    """
    missing_local = [name for name in LOCAL_CLIENT_ROUTE_NAMES if name not in client_routes]
    remote = sorted(set(client_routes) - set(LOCAL_CLIENT_ROUTE_NAMES))
    if missing_local or len(remote) != REMOTE_CLIENT_ROUTE_COUNT:
        _coordination_error(
            "coordination.client_routes must name the local routes "
            + ", ".join(LOCAL_CLIENT_ROUTE_NAMES)
            + f" and exactly {REMOTE_CLIENT_ROUTE_COUNT} remote routes"
            + f" (missing local: {', '.join(missing_local) or 'none'};"
            + f" remote routes named: {len(remote)})",
            field="coordination.client_routes",
        )
    bad = [
        name
        for name in remote
        if name.startswith("local-") or not _REMOTE_ROUTE_NAME.fullmatch(name)
    ]
    if bad:
        _coordination_error(
            "coordination.client_routes remote route names must be one lower-case"
            " token and must not start with the reserved local- prefix",
            field="coordination.client_routes",
            value=bad,
        )


def _coordination_error(message: str, *, field: str | None = None, value: Any = None) -> None:
    # The coordination block lives in the okg runtime file since
    # pact-v5-corpus-cutover. A repo that still declares it inline in
    # pact/project.yaml is read through the documented fallback; the path here
    # names where the block BELONGS, which is what a reader needs to fix it.
    data: dict[str, Any] = {"path": RUNTIME_SETTINGS_RELATIVE_PATH}
    if field is not None:
        data["field"] = field
    if value is not None:
        data["value"] = value
    raise SubstrateError(message, data=data)


def _require_string_keys(value: dict[Any, Any], field: str) -> None:
    if not all(isinstance(key, str) for key in value):
        _coordination_error(f"{field} keys must be strings", field=field)


def _require_exact_keys(
    value: dict[str, Any], expected: frozenset[str], field: str
) -> None:
    actual = set(value)
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    if missing:
        _coordination_error(
            f"{field} is missing required field(s): {', '.join(missing)}",
            field=field,
        )
    if extra:
        _coordination_error(
            f"{field} has unknown field(s): {', '.join(extra)}",
            field=field,
        )


def _require_transport(value: Any, *, field: str, expected: str) -> str:
    if not isinstance(value, str) or not value.strip():
        _coordination_error(
            f"{field} must be the selected transport {expected!r}",
            field=field,
            value=value,
        )
    if value != expected:
        _coordination_error(
            f"{field} has unknown transport {value!r}; expected {expected!r}",
            field=field,
            value=value,
        )
    return value


def _require_positive_ttl(value: Any, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        _coordination_error(
            f"{field} must be a positive integer number of seconds",
            field=field,
            value=value,
        )
    return value


def _parse_execution_policy(value: Any) -> CoordinationExecutionPolicy:
    field = "coordination.execution"
    if not isinstance(value, dict):
        _coordination_error(f"{field} must be a mapping", field=field, value=value)
    _require_string_keys(value, field)
    _require_exact_keys(value, _EXECUTION_FIELDS, field)

    lane_raw = value["lane_capacity"]
    if not isinstance(lane_raw, dict):
        _coordination_error(
            f"{field}.lane_capacity must be a mapping",
            field=f"{field}.lane_capacity",
            value=lane_raw,
        )
    _require_string_keys(lane_raw, f"{field}.lane_capacity")
    _require_exact_keys(lane_raw, _LANE_CAPACITY_FIELDS, f"{field}.lane_capacity")
    lane_capacity = {
        lane: _require_non_negative_capacity(
            lane_raw[lane], field=f"{field}.lane_capacity.{lane}"
        )
        for lane in ("critical", "light")
    }

    resource_raw = value["named_resource_capacity"]
    if not isinstance(resource_raw, dict):
        _coordination_error(
            f"{field}.named_resource_capacity must be a mapping",
            field=f"{field}.named_resource_capacity",
            value=resource_raw,
        )
    _require_string_keys(resource_raw, f"{field}.named_resource_capacity")
    resource_capacity: dict[str, int] = {}
    for name, raw_capacity in resource_raw.items():
        _require_non_blank_name(name, field=f"{field}.named_resource_capacity")
        resource_capacity[name] = _require_non_negative_capacity(
            raw_capacity,
            field=f"{field}.named_resource_capacity.{name}",
        )

    profiles_raw = value["profiles"]
    if not isinstance(profiles_raw, dict):
        _coordination_error(
            f"{field}.profiles must be a mapping",
            field=f"{field}.profiles",
            value=profiles_raw,
        )
    _require_string_keys(profiles_raw, f"{field}.profiles")
    profiles: dict[str, CoordinationExecutionProfile] = {}
    for name, profile_raw in profiles_raw.items():
        _require_non_blank_name(name, field=f"{field}.profiles")
        profile_field = f"{field}.profiles.{name}"
        if not isinstance(profile_raw, dict):
            _coordination_error(
                f"{profile_field} must be a mapping",
                field=profile_field,
                value=profile_raw,
            )
        _require_string_keys(profile_raw, profile_field)
        _require_exact_keys(profile_raw, _EXECUTION_PROFILE_FIELDS, profile_field)

        lane = profile_raw["lane"]
        if not isinstance(lane, str) or not lane.strip():
            _coordination_error(
                f"{profile_field}.lane must name a known execution lane",
                field=f"{profile_field}.lane",
                value=lane,
            )
        if lane not in _EXECUTION_LANES:
            _coordination_error(
                f"{profile_field}.lane has unknown execution lane {lane!r}",
                field=f"{profile_field}.lane",
                value=lane,
            )

        profile_resources_raw = profile_raw["resources"]
        if not isinstance(profile_resources_raw, dict):
            _coordination_error(
                f"{profile_field}.resources must be a mapping",
                field=f"{profile_field}.resources",
                value=profile_resources_raw,
            )
        _require_string_keys(profile_resources_raw, f"{profile_field}.resources")
        profile_resources: dict[str, int] = {}
        for resource_name, raw_demand in profile_resources_raw.items():
            _require_non_blank_name(
                resource_name, field=f"{profile_field}.resources"
            )
            if resource_name not in resource_capacity:
                _coordination_error(
                    f"{profile_field}.resources has unknown resource {resource_name!r}",
                    field=f"{profile_field}.resources.{resource_name}",
                    value=raw_demand,
                )
            profile_resources[resource_name] = _require_positive_demand(
                raw_demand,
                field=f"{profile_field}.resources.{resource_name}",
            )
        profiles[name] = CoordinationExecutionProfile(
            lane=lane,
            resources=profile_resources,
        )

    helper_budgets_raw = value["helper_budgets"]
    helper_budgets_field = f"{field}.helper_budgets"
    if not isinstance(helper_budgets_raw, dict):
        _coordination_error(
            f"{helper_budgets_field} must be a mapping",
            field=helper_budgets_field,
            value=helper_budgets_raw,
        )
    _require_string_keys(helper_budgets_raw, helper_budgets_field)
    _require_exact_keys(
        helper_budgets_raw,
        _HELPER_PARENT_KINDS,
        helper_budgets_field,
    )
    helper_budgets: dict[str, CoordinationHelperBudget] = {}
    for parent_kind in ("chief", "sprint", "pact"):
        budget_raw = helper_budgets_raw[parent_kind]
        budget_field = f"{helper_budgets_field}.{parent_kind}"
        if not isinstance(budget_raw, dict):
            _coordination_error(
                f"{budget_field} must be a mapping",
                field=budget_field,
                value=budget_raw,
            )
        _require_string_keys(budget_raw, budget_field)
        _require_exact_keys(budget_raw, _HELPER_BUDGET_FIELDS, budget_field)
        profile_name = _require_non_blank_name(
            budget_raw["profile"], field=f"{budget_field}.profile"
        )
        if profile_name not in profiles:
            _coordination_error(
                f"{budget_field}.profile has unknown execution profile {profile_name!r}",
                field=f"{budget_field}.profile",
                value=profile_name,
            )
        helper_budgets[parent_kind] = CoordinationHelperBudget(
            profile=profile_name,
            max_active=_require_non_negative_capacity(
                budget_raw["max_active"], field=f"{budget_field}.max_active"
            ),
        )

    return CoordinationExecutionPolicy(
        lane_capacity=lane_capacity,
        named_resource_capacity=resource_capacity,
        profiles=profiles,
        helper_budgets=helper_budgets,
    )


def _require_non_blank_name(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        _coordination_error(
            f"{field} names must be non-blank strings",
            field=field,
            value=value,
        )
    return value


def _require_non_negative_capacity(value: Any, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        _coordination_error(
            f"{field} must be a non-negative integer capacity",
            field=field,
            value=value,
        )
    return value


def _require_positive_demand(value: Any, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        _coordination_error(
            f"{field} must be a positive integer resource demand",
            field=field,
            value=value,
        )
    return value


def _canonical_digest(value: Any) -> str:
    try:
        payload = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        _coordination_error(
            "pact/project.yaml settings cannot be canonically encoded for digest",
            value=str(exc),
        )
    return "sha256:" + hashlib.sha256(payload).hexdigest()
