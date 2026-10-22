"""Strict deployment-definition and live-environment composition.

A deployment definition owns graph behavior. An environment descriptor owns
the live target and its mutable resource identities. This module composes one
of each through explicit field ownership; it never deep-merges YAML.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass, field as dataclass_field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping
from urllib.parse import unquote, urlsplit

from okg.substrate import fastyaml
from okg.substrate.db.wal_settings import resolve_wal_settings

from . import (
    DeploymentConfigError,
    DeploymentManifest,
    _substitute_manifest_placeholders,
)


ENVIRONMENT_API_VERSION = "okg.environment/v1"
DEFINITION_API_VERSION = "okg.deployment-definition/v1"
POSTGRES_CONTAINER_OWNER_API_VERSION = "okg.postgres-container-owner/v1"
POSTGRES_CAPACITY_API_VERSION = "okg.postgres-capacity/v1"
POSTGRES_MIN_BACKGROUND_WORKER_HEADROOM = 4
ENVIRONMENT_TIERS = frozenset({"development", "staging", "production"})
PRODUCTION_LIKE_TIERS = frozenset({"staging", "production"})

_IDENTIFIER_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$")
_DATABASE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_RESOURCE_SIZE_RE = re.compile(r"^[1-9][0-9]*(?:[kmgt]b?)?$", re.IGNORECASE)
_ENV_REF_RE = re.compile(r"^env:([A-Za-z_][A-Za-z0-9_]*)$")
_TEMPLATE_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")

_ENVIRONMENT_KEYS = frozenset(
    {
        "api_version",
        "id",
        "tier",
        "graph_deployment_name",
        "sync_root",
        "definition",
        "database",
        "release",
        "runtime",
        "ui",
        "webhook",
        "mcp",
        "readiness",
    }
)

_DEFINITION_KEYS = frozenset(
    {
        "api_version",
        "id",
        "description",
        "schema_dir",
        "source_registry",
        "enricher_registry",
        "taxonomy",
        "aliases",
        "mcp_instructions",
        "skills_dir",
        "audit_questions",
        "tool_description_overrides",
        "skill_triggers",
        "snapshot_path",
        "viz_path",
        "link_config",
        "refreshers",
        "modules",
        "modules_config",
        "module_providers",
        "record_prefixes",
        "session_start_context",
        "agent_tools",
        "mcp_pre_join_tools",
        "resolvers",
        "viz",
        "chat",
        "profiling",
        "embedding",
        "embeddings",
        "chronos",
        "consistency",
        "extractor",
        "enrichers",
        "nomos",
        "consistency_gates",
        "post_publish_consistency_checks",
        "ir_consistency_checks",
        "checkpoint_attestors",
        "checkpoint_attestor_root",
        "lenses",
        "derived_producers",
        "native_views",
        "maintained_derivations",
        "alias_resolver",
        "doctor_checks",
        "dashboards",
        "live_capabilities",
        # Producer ranking for shared edge types (item 92a): part of what the
        # definition publishes, like its enrichers and sources.
        "edge_precedence",
        # The declared agent surface (issue #2067). Validated by the same
        # `parse_agent_surface` the manifest path runs, because composition
        # builds the manifest through `DeploymentManifest._build_from_raw`.
        "agent_surface",
        # What the definition declares it did not ingest (PACT dataset-card):
        # a statement about the data, so it travels with the definition.
        "not_ingested",
        # The record attribute a reader cites (PACT read-names-list-3666): a
        # statement about the data's records, like `not_ingested`.
        "citation_attr",
    }
)

_ENVIRONMENT_OWNED_DEFINITION_KEYS = frozenset(
    {
        "name",
        "postgres",
        "postgres_dsn",
        "release",
        "contract",
        "runtime",
        "profile",
        "sync_root",
        "mcp_trace_path",
    }
)

_DEFINITION_IMPORT_KEYS = frozenset(
    {
        "base",
        "definition",
        "extends",
        "import",
        "imports",
        "include",
        "includes",
    }
)

_DEFINITION_PATH_KEYS = frozenset(
    {
        "schema_dir",
        "source_registry",
        "enricher_registry",
        "taxonomy",
        "aliases",
        "mcp_instructions",
        "skills_dir",
        "audit_questions",
        "tool_description_overrides",
        "skill_triggers",
        "snapshot_path",
        "viz_path",
        "mcp_pre_join_tools",
        "checkpoint_attestor_root",
    }
)

# `_DEFINITION_PATH_KEYS` covers keys whose value is ONE path string. A key
# whose value is a mapping of owner to directory cannot go in that set -- the
# loops there read a string -- so it is named here instead, and
# `_resolve_directory_mapping_key` turns its mapping into the directories it
# names. Both sets reach the definition digest and the substance comparison by
# the BYTES behind the paths; a path-shaped key in neither contributes only
# its literal YAML value, which is the defect #1967 fixed for
# `module_providers`. A new path-shaped key belongs in one of the two.
_DEFINITION_DIRECTORY_MAPPING_KEYS = frozenset({"module_providers"})

# Path-valued fields NESTED inside a definition block
# (`okg.substrate.agent_surface_paths`). Same rule as the two sets above --
# the digest and the substance comparison bind the bytes behind the path --
# for the fields `agent_surface` carries in place of the top-level
# `mcp_instructions` and `tool_description_overrides` (issue #2067). Without
# this a moved instructions file would drop out of the definition digest, and
# two tiers naming the same bytes at different paths (okg-ci names
# `mcp-instructions.md`, okg-ci-dev `../okg-ci/...`) would read as different
# foundations. The slot is the dotted path.

_VIZ_ENVIRONMENT_KEYS = frozenset(
    {
        "bind",
        "host",
        "listen",
        "port",
        "service",
        "url",
    }
)


@dataclass(frozen=True)
class DatabaseResource:
    dsn_secret_ref: str
    dsn: str
    name: str
    container: str
    volume: str
    host_port: int
    dbos_identity: str
    # None is reserved for ownerless v1 artifacts loaded only through the
    # release-pointer transition bridge. Every installable descriptor must
    # carry the sealed owner sidecar.
    container_uid: int | None = None
    container_gid: int | None = None
    shm_size: str | None = None
    memory_limit: str | None = None
    max_worker_processes: int | None = None
    max_parallel_workers: int | None = None
    timescaledb_max_background_workers: int | None = None
    timescaledb_job_worker_reserve: int | None = None
    # Checkpoint and WAL settings the rendered Postgres starts with: the
    # okg.substrate.db.wal_settings defaults overlaid by database.wal.
    wal_settings: Mapping[str, str] = dataclass_field(
        default_factory=lambda: resolve_wal_settings(None)
    )


@dataclass(frozen=True)
class ReleaseResource:
    mode: str
    artifact_store: Path
    active_pointer: Path
    previous_pointer: Path
    pin: Mapping[str, str] = dataclass_field(default_factory=dict)


@dataclass(frozen=True)
class RuntimeResource:
    worker_service: str
    mirror_service: str | None
    mirror_enabled: bool
    state_root: Path
    data_root: Path
    local_env_file: Path
    log_root: Path
    config: Mapping[str, Any]
    credential_refs: Mapping[str, tuple[str, ...]]


@dataclass(frozen=True)
class EndpointResource:
    service: str
    host: str
    port: int
    enabled: bool = True
    #: ``ui`` only (issue #3278): NAME of the environment variable holding the
    #: read-API token. The supervised viz gets ``--auth-token-env <name>``, and
    #: the readiness probe sends the same token as a Bearer credential.
    auth_token_env: str | None = None


@dataclass(frozen=True)
class McpResource:
    transport: str
    registration_id: str
    launcher_environment: str


@dataclass(frozen=True)
class ReadinessResource:
    policy: str
    evidence_store: str
    evidence_path: Path
    max_age_seconds: int
    prerequisite_pacts: tuple[str, ...]
    requires_clone_rebind: bool
    override_authority: str | None = None
    source_groups: Mapping[str, Mapping[str, tuple[str, ...]]] = dataclass_field(
        default_factory=dict
    )


@dataclass(frozen=True)
class ResourceIdentity:
    environment_id: str
    resource: str
    collision_domain: str
    value: str
    shareable: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "environment_id": self.environment_id,
            "resource": self.resource,
            "collision_domain": self.collision_domain,
            "value": self.value,
            "shareable": self.shareable,
        }


@dataclass(frozen=True)
class _DatabaseTarget:
    host: str
    port: int
    database: str

    @property
    def endpoint(self) -> str:
        return f"{self.host}:{self.port}"

    @property
    def identity(self) -> str:
        return f"{self.endpoint}/{self.database}"


@dataclass(frozen=True)
class DeploymentEnvironment:
    id: str
    tier: str
    graph_deployment_name: str
    descriptor_path: Path
    descriptor_digest: str
    postgres_capacity_digest: str | None
    definition_id: str
    definition_path: Path
    definition_digest: str
    # Tier-shared substance comparison keys (see definition_substance()).
    # Kept separate from definition_digest so persisted release identities
    # are untouched.
    definition_substance: Mapping[str, str]
    database: DatabaseResource
    release: ReleaseResource
    runtime: RuntimeResource
    ui: EndpointResource
    webhook: EndpointResource
    mcp: McpResource
    readiness: ReadinessResource
    path_environment: Mapping[str, str]
    manifest: DeploymentManifest

    def to_dict(self) -> dict[str, Any]:
        """Return a secret-redacted, stable planning/status representation."""
        return {
            "schema": ENVIRONMENT_API_VERSION,
            "id": self.id,
            "tier": self.tier,
            "graph_deployment_name": self.graph_deployment_name,
            "descriptor": {
                "path": str(self.descriptor_path),
                "digest": self.descriptor_digest,
            },
            "definition": {
                "id": self.definition_id,
                "path": str(self.definition_path),
                "digest": self.definition_digest,
                "sync_root": (
                    str(self.manifest.sync_root)
                    if self.manifest.sync_root is not None
                    else None
                ),
            },
            "database": {
                "dsn_secret_ref": self.database.dsn_secret_ref,
                "dsn": "<redacted>",
                "name": self.database.name,
                "container": self.database.container,
                "volume": self.database.volume,
                "host_port": self.database.host_port,
                "dbos_identity": self.database.dbos_identity,
                "container_uid": self.database.container_uid,
                "container_gid": self.database.container_gid,
                "shm_size": self.database.shm_size,
                "memory_limit": self.database.memory_limit,
                "wal_settings": dict(self.database.wal_settings),
                "max_worker_processes": self.database.max_worker_processes,
                "max_parallel_workers": self.database.max_parallel_workers,
                "timescaledb_max_background_workers": (
                    self.database.timescaledb_max_background_workers
                ),
                "timescaledb_job_worker_reserve": (
                    self.database.timescaledb_job_worker_reserve
                ),
                "postgres_capacity_digest": self.postgres_capacity_digest,
            },
            "release": {
                "mode": self.release.mode,
                "artifact_store": str(self.release.artifact_store),
                "active_pointer": str(self.release.active_pointer),
                "previous_pointer": str(self.release.previous_pointer),
            },
            "runtime": {
                "worker_service": self.runtime.worker_service,
                "mirror_service": self.runtime.mirror_service,
                "mirror_enabled": self.runtime.mirror_enabled,
                "state_root": str(self.runtime.state_root),
                "data_root": str(self.runtime.data_root),
                "local_env_file": str(self.runtime.local_env_file),
                "log_root": str(self.runtime.log_root),
                "path_environment": sorted(self.path_environment),
                "credential_refs": {
                    service: list(refs)
                    for service, refs in sorted(self.runtime.credential_refs.items())
                },
            },
            "ui": {
                "enabled": self.ui.enabled,
                "service": self.ui.service,
                "host": self.ui.host,
                "port": self.ui.port,
                **(
                    {"auth_token_env": self.ui.auth_token_env}
                    if self.ui.auth_token_env
                    else {}
                ),
            },
            "webhook": {
                "enabled": self.webhook.enabled,
                "service": self.webhook.service,
                "host": self.webhook.host,
                "port": self.webhook.port,
            },
            "mcp": {
                "transport": self.mcp.transport,
                "registration_id": self.mcp.registration_id,
                "launcher_environment": self.mcp.launcher_environment,
            },
            "readiness": {
                "policy": self.readiness.policy,
                "evidence_store": self.readiness.evidence_store,
                "evidence_path": str(self.readiness.evidence_path),
                "max_age_seconds": self.readiness.max_age_seconds,
                "prerequisite_pacts": list(
                    self.readiness.prerequisite_pacts
                ),
                "requires_clone_rebind": self.readiness.requires_clone_rebind,
                "override_authority": self.readiness.override_authority,
                "source_groups": {
                    profile: {
                        group: list(sources)
                        for group, sources in groups.items()
                    }
                    for profile, groups in self.readiness.source_groups.items()
                },
            },
        }


def default_environments_dir() -> Path:
    configured = os.environ.get("OKG_ENVIRONMENTS_DIR")
    if configured:
        return Path(configured).expanduser().resolve()
    # A wheel install has no ops/ beside the package; the runtime image names
    # okg's tree with OKG_FRAMEWORK_TREE (refused if it is not there).
    from okg.substrate.framework_tree import framework_tree_root

    tree = framework_tree_root()
    if tree is not None:
        return tree / "ops" / "environments"
    return Path(__file__).resolve().parents[4] / "ops" / "environments"


def find_environment_path(
    environment: str | Path,
    *,
    environments_dir: str | Path | None = None,
) -> Path:
    """Resolve an explicit path or an id under one configured environment root.

    This intentionally does not walk from cwd and does not inspect `.okg`.
    """
    candidate = Path(environment).expanduser()
    if candidate.name == "environment.yaml":
        return candidate.resolve()
    explicit_path = (
        isinstance(environment, Path)
        or candidate.is_absolute()
        or len(candidate.parts) > 1
    )
    if explicit_path and candidate.is_dir():
        return (candidate / "environment.yaml").resolve()
    if explicit_path:
        return candidate.resolve()
    root = (
        Path(environments_dir).expanduser().resolve()
        if environments_dir is not None
        else default_environments_dir()
    )
    return root / str(environment) / "environment.yaml"


def load_environment(
    environment: str | Path,
    *,
    environments_dir: str | Path | None = None,
    environ: Mapping[str, str] | None = None,
    _allow_legacy_container_owner: bool = False,
) -> DeploymentEnvironment:
    descriptor_path = find_environment_path(
        environment,
        environments_dir=environments_dir,
    )
    raw = _load_yaml_mapping(descriptor_path, label="environment descriptor")
    _reject_unknown(raw, _ENVIRONMENT_KEYS, path=descriptor_path, field="environment")

    api_version = _required_string(raw, "api_version", path=descriptor_path)
    if api_version != ENVIRONMENT_API_VERSION:
        raise DeploymentConfigError(
            f"{descriptor_path} api_version must be {ENVIRONMENT_API_VERSION!r}; "
            f"observed {api_version!r}"
        )

    environment_id = _identifier(raw, "id", path=descriptor_path)
    if descriptor_path.parent.name != environment_id:
        raise DeploymentConfigError(
            f"{descriptor_path} environment id {environment_id!r} must match "
            f"descriptor directory {descriptor_path.parent.name!r}"
        )
    tier = _required_string(raw, "tier", path=descriptor_path)
    if tier not in ENVIRONMENT_TIERS:
        raise DeploymentConfigError(
            f"{descriptor_path} tier must be one of {sorted(ENVIRONMENT_TIERS)}; "
            f"observed {tier!r}"
        )
    graph_deployment_name = _identifier(
        raw,
        "graph_deployment_name",
        path=descriptor_path,
    )

    values = dict(os.environ if environ is None else environ)
    sync_root = (
        _resolve_path(
            raw,
            "sync_root",
            descriptor_path=descriptor_path,
            environ=values,
        )
        if raw.get("sync_root") is not None
        else None
    )
    definition_block = _mapping(raw, "definition", path=descriptor_path)
    _reject_unknown(
        definition_block,
        frozenset({"path"}),
        path=descriptor_path,
        field="definition",
    )
    definition_path = _definition_path(
        _required_string(definition_block, "path", path=descriptor_path),
        descriptor_path=descriptor_path,
    )
    definition_raw = _load_yaml_mapping(
        definition_path,
        label="deployment definition",
    )
    definition_id = _validate_definition(
        definition_raw,
        definition_path=definition_path,
    )
    if "checkpoint_attestors" in definition_raw:
        from okg.substrate.publisher.checkpoint_attestations import (
            CheckpointAttestationConfigurationError,
            inspect_checkpoint_attestor_definition_declarations,
        )

        try:
            inspect_checkpoint_attestor_definition_declarations(
                definition_path,
                deployment_name=definition_id,
            )
        except CheckpointAttestationConfigurationError as exc:
            raise DeploymentConfigError(
                f"{definition_path} checkpoint_attestors are invalid: {exc}"
            ) from exc

    container_uid, container_gid = _load_postgres_container_owner(
        descriptor_path=descriptor_path,
        allow_legacy=_allow_legacy_container_owner,
    )
    postgres_capacity = _load_postgres_capacity(
        descriptor_path=descriptor_path,
        allow_legacy=_allow_legacy_container_owner,
    )
    postgres_capacity_digest = _postgres_capacity_digest(postgres_capacity)
    database = _parse_database(
        _mapping(raw, "database", path=descriptor_path),
        descriptor_path=descriptor_path,
        environ=values,
        container_uid=container_uid,
        container_gid=container_gid,
        postgres_capacity=postgres_capacity,
    )
    release = _parse_release(
        _mapping(raw, "release", path=descriptor_path),
        descriptor_path=descriptor_path,
        environ=values,
        tier=tier,
    )
    runtime = _parse_runtime(
        _mapping(raw, "runtime", path=descriptor_path),
        descriptor_path=descriptor_path,
        environ=values,
    )
    ui = _parse_endpoint(
        _mapping(raw, "ui", path=descriptor_path),
        descriptor_path=descriptor_path,
        field="ui",
    )
    webhook = _parse_endpoint(
        _mapping(raw, "webhook", path=descriptor_path),
        descriptor_path=descriptor_path,
        field="webhook",
    )
    # The token reaches the supervised UI only through the service env file,
    # which carries exactly the declared credential references. Refuse a name
    # nothing would supply, instead of starting a UI that cannot read it.
    if (
        ui.auth_token_env is not None
        and f"env:{ui.auth_token_env}"
        not in runtime.credential_refs.get(ui.service, ())
    ):
        raise DeploymentConfigError(
            f"{descriptor_path} ui.auth_token_env {ui.auth_token_env!r} must "
            f"also be declared as env:{ui.auth_token_env} under "
            f"runtime.credential_refs for service {ui.service!r}"
        )
    mcp = _parse_mcp(
        _mapping(raw, "mcp", path=descriptor_path),
        descriptor_path=descriptor_path,
        environment_id=environment_id,
    )
    readiness = _parse_readiness(
        _mapping(raw, "readiness", path=descriptor_path),
        descriptor_path=descriptor_path,
        environ=values,
        state_root=runtime.state_root,
    )
    path_environment = _descriptor_path_environment(
        raw,
        environ=values,
    )

    manifest = _compose_manifest(
        definition_raw=definition_raw,
        definition_path=definition_path,
        graph_deployment_name=graph_deployment_name,
        sync_root=sync_root,
        database=database,
        runtime=runtime,
        webhook=webhook,
        readiness=readiness,
        tier=tier,
    )

    return DeploymentEnvironment(
        id=environment_id,
        tier=tier,
        graph_deployment_name=graph_deployment_name,
        descriptor_path=descriptor_path,
        descriptor_digest=_mapping_digest(raw),
        postgres_capacity_digest=postgres_capacity_digest,
        definition_id=definition_id,
        definition_path=definition_path,
        definition_digest=_definition_digest(
            definition_raw,
            definition_path=definition_path,
        ),
        definition_substance=definition_substance(
            definition_raw,
            definition_path=definition_path,
        ),
        database=database,
        release=release,
        runtime=runtime,
        ui=ui,
        webhook=webhook,
        mcp=mcp,
        readiness=readiness,
        path_environment=path_environment,
        manifest=manifest,
    )


def load_environment_fleet(
    environments_dir: str | Path,
    *,
    environ: Mapping[str, str] | None = None,
) -> tuple[DeploymentEnvironment, ...]:
    root = Path(environments_dir).expanduser().resolve()
    if not root.is_dir():
        raise DeploymentConfigError(f"environment root not found: {root}")
    paths = sorted(root.glob("*/environment.yaml"))
    if not paths:
        raise DeploymentConfigError(f"no environment descriptors found under {root}")
    environments = tuple(load_environment(path, environ=environ) for path in paths)
    ids = [environment.id for environment in environments]
    duplicates = sorted({item for item in ids if ids.count(item) > 1})
    if duplicates:
        raise DeploymentConfigError(
            f"environment root {root} contains duplicate id(s): {duplicates}"
        )
    return environments


def resolve_postgres_capacity_authority(
    *,
    deployment_name: str,
    deployment_dir: str | Path,
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any] | None:
    """Resolve production Postgres capacity without reading database secrets.

    Deployment readiness runs outside the sealed launcher as well as inside it.
    It therefore cannot rely on a process-local descriptor variable.  The
    checked-in production environment descriptor remains the authority in both
    cases: an explicit sealed descriptor wins, otherwise the repository's
    environment fleet is searched for one production owner of the deployment.
    """
    values = dict(os.environ if environ is None else environ)
    explicit = values.get("OKG_ENVIRONMENT_DESCRIPTOR")
    descriptor_paths: list[Path]
    if explicit:
        descriptor_paths = [Path(explicit).expanduser().resolve()]
    else:
        declared_root = values.get("OKG_ENVIRONMENTS_DIR")
        if declared_root:
            environments_root = Path(declared_root).expanduser().resolve()
        else:
            resolved_deployment_dir = Path(deployment_dir).expanduser().resolve()
            environments_root = next(
                (
                    parent / "ops" / "environments"
                    for parent in (
                        resolved_deployment_dir,
                        *resolved_deployment_dir.parents,
                    )
                    if (parent / "ops" / "environments").is_dir()
                ),
                resolved_deployment_dir.parent / "ops" / "environments",
            )
        descriptor_paths = sorted(environments_root.glob("*/environment.yaml"))

    matches: list[tuple[Path, Mapping[str, Any]]] = []
    for descriptor_path in descriptor_paths:
        raw = _load_yaml_mapping(
            descriptor_path,
            label="environment descriptor",
        )
        if (
            str(raw.get("graph_deployment_name") or "") == deployment_name
            and str(raw.get("tier") or "") == "production"
        ):
            matches.append((descriptor_path, raw))
    if not matches and not explicit:
        return None
    if len(matches) != 1:
        raise DeploymentConfigError(
            "Postgres capacity authority requires exactly one production "
            f"environment for deployment {deployment_name!r}"
        )

    descriptor_path, raw = matches[0]
    capacity = _load_postgres_capacity(
        descriptor_path=descriptor_path,
        allow_legacy=False,
    )
    capacity_digest = _postgres_capacity_digest(capacity)
    assert capacity[0] is not None
    (
        max_worker_processes,
        max_parallel_workers,
        timescaledb_max_background_workers,
        timescaledb_job_worker_reserve,
        configured_headroom,
    ) = capacity
    return {
        "schema": "okg.postgres_capacity_authority/v1",
        "environment_id": _identifier(raw, "id", path=descriptor_path),
        "deployment": deployment_name,
        "authority_source": "sealed_postgres_capacity_sidecar",
        "sidecar_digest": capacity_digest,
        "max_worker_processes": max_worker_processes,
        "max_parallel_workers": max_parallel_workers,
        "timescaledb_max_background_workers": (
            timescaledb_max_background_workers
        ),
        "timescaledb_job_worker_reserve": timescaledb_job_worker_reserve,
        "configured_headroom": configured_headroom,
        "minimum_headroom": POSTGRES_MIN_BACKGROUND_WORKER_HEADROOM,
    }


def environment_resource_identities(
    environment: DeploymentEnvironment,
) -> tuple[ResourceIdentity, ...]:
    target = _database_target(
        environment.database.dsn,
        path=environment.descriptor_path,
    )
    environment_id = environment.id

    def identity(
        resource: str,
        collision_domain: str,
        value: str | Path,
        *,
        shareable: bool = False,
    ) -> ResourceIdentity:
        return ResourceIdentity(
            environment_id=environment_id,
            resource=resource,
            collision_domain=collision_domain,
            value=str(value),
            shareable=shareable,
        )

    return (
        identity(
            "graph_deployment_name",
            "graph_deployment_name",
            environment.graph_deployment_name,
        ),
        identity("database.target", "database_target", target.identity),
        identity("database.endpoint", "tcp_endpoint", target.endpoint),
        identity(
            "database.container",
            "container_name",
            environment.database.container,
        ),
        identity("database.volume", "volume_name", environment.database.volume),
        identity(
            "database.dbos_identity",
            "dbos_identity",
            environment.database.dbos_identity,
        ),
        identity(
            "release.artifact_store",
            "immutable_artifact_store",
            environment.release.artifact_store,
            shareable=True,
        ),
        identity(
            "release.active_pointer",
            "mutable_path",
            environment.release.active_pointer,
        ),
        identity(
            "release.previous_pointer",
            "mutable_path",
            environment.release.previous_pointer,
        ),
        identity(
            "runtime.worker_service",
            "service_id",
            environment.runtime.worker_service,
        ),
        identity(
            "runtime.state_root",
            "mutable_path",
            environment.runtime.state_root,
        ),
        identity(
            "runtime.data_root",
            "mutable_path",
            environment.runtime.data_root,
        ),
        identity(
            "runtime.local_env_file",
            "mutable_path",
            environment.runtime.local_env_file,
        ),
        identity("runtime.log_root", "mutable_path", environment.runtime.log_root),
        identity("ui.service", "service_id", environment.ui.service),
        identity("ui.endpoint", "tcp_endpoint", _endpoint_identity(environment.ui)),
        identity("webhook.service", "service_id", environment.webhook.service),
        identity(
            "webhook.endpoint",
            "tcp_endpoint",
            _endpoint_identity(environment.webhook),
        ),
        identity(
            "mcp.registration_id",
            "mcp_registration",
            environment.mcp.registration_id,
        ),
        identity(
            "readiness.evidence_path",
            "mutable_path",
            environment.readiness.evidence_path,
        ),
        identity(
            "definition.digest",
            "definition_digest",
            environment.definition_digest,
            shareable=True,
        ),
    )


def resource_isolation_report(
    environments: tuple[DeploymentEnvironment, ...] | list[DeploymentEnvironment],
) -> dict[str, Any]:
    rows = [
        identity
        for environment in environments
        for identity in environment_resource_identities(environment)
    ]
    grouped: dict[tuple[str, str], list[ResourceIdentity]] = {}
    for row in rows:
        grouped.setdefault((row.collision_domain, row.value), []).append(row)

    collisions: list[dict[str, Any]] = []
    for (domain, value), owners in sorted(grouped.items()):
        if len(owners) < 2 or all(owner.shareable for owner in owners):
            continue
        collisions.append(
            {
                "collision_domain": domain,
                "value": value,
                "owners": [owner.to_dict() for owner in owners],
            }
        )
    collisions.extend(_nested_mutable_path_collisions(rows))

    return {
        "schema": "okg.environment.resource_isolation.v1",
        "ok": not collisions,
        "environment_ids": sorted(environment.id for environment in environments),
        "resource_count": len(rows),
        "resources": [row.to_dict() for row in rows],
        "collision_count": len(collisions),
        "collisions": collisions,
    }


def require_resource_isolation(
    environments: tuple[DeploymentEnvironment, ...] | list[DeploymentEnvironment],
) -> dict[str, Any]:
    report = resource_isolation_report(environments)
    if report["ok"]:
        return report
    summary = [
        {
            "collision_domain": row["collision_domain"],
            "value": row["value"],
            "owners": [
                f"{owner['environment_id']}:{owner['resource']}"
                for owner in row["owners"]
            ],
        }
        for row in report["collisions"]
    ]
    raise DeploymentConfigError(f"environment resource isolation failed: {summary}")


def _load_yaml_mapping(path: Path, *, label: str) -> dict[str, Any]:
    if not path.is_file():
        raise DeploymentConfigError(f"{label} not found: {path}")
    try:
        raw = fastyaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise DeploymentConfigError(
            f"{label} YAML parse failed at {path}: {exc}"
        ) from exc
    if not isinstance(raw, dict):
        raise DeploymentConfigError(f"{label} must be a YAML mapping: {path}")
    return dict(raw)


def _reject_unknown(
    raw: Mapping[str, Any],
    allowed: frozenset[str],
    *,
    path: Path,
    field: str,
) -> None:
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise DeploymentConfigError(
            f"{path} {field} has unknown field(s): {unknown}; "
            "composition has no override or last-file-wins fallback"
        )


def _mapping(raw: Mapping[str, Any], key: str, *, path: Path) -> dict[str, Any]:
    value = raw.get(key)
    if not isinstance(value, dict):
        raise DeploymentConfigError(f"{path} field {key!r} must be a mapping")
    return dict(value)


def _required_string(raw: Mapping[str, Any], key: str, *, path: Path) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value.strip():
        raise DeploymentConfigError(f"{path} field {key!r} must be a non-empty string")
    return value.strip()


def _identifier(raw: Mapping[str, Any], key: str, *, path: Path) -> str:
    value = _required_string(raw, key, path=path)
    if not _IDENTIFIER_RE.fullmatch(value):
        raise DeploymentConfigError(
            f"{path} field {key!r} must match {_IDENTIFIER_RE.pattern!r}; "
            f"observed {value!r}"
        )
    return value


def _port(raw: Mapping[str, Any], key: str, *, path: Path) -> int:
    value = raw.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 65535:
        raise DeploymentConfigError(f"{path} field {key!r} must be a TCP port")
    return value


def _positive_int(raw: Mapping[str, Any], key: str, *, path: Path) -> int:
    value = raw.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise DeploymentConfigError(f"{path} field {key!r} must be a positive integer")
    return value


def _definition_path(value: str, *, descriptor_path: Path) -> Path:
    if _TEMPLATE_RE.search(value):
        raise DeploymentConfigError(
            f"{descriptor_path} definition.path cannot contain environment placeholders"
        )
    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        candidate = descriptor_path.parent / candidate
    return candidate.resolve()


def _validate_definition(raw: Mapping[str, Any], *, definition_path: Path) -> str:
    imported = sorted(set(raw) & _DEFINITION_IMPORT_KEYS)
    if imported:
        raise DeploymentConfigError(
            f"{definition_path} declares nested definition import field(s) {imported}; "
            "an environment composes exactly one definition"
        )
    environment_owned = sorted(set(raw) & _ENVIRONMENT_OWNED_DEFINITION_KEYS)
    if environment_owned:
        raise DeploymentConfigError(
            f"{definition_path} declares environment-owned field(s) {environment_owned}"
        )
    _reject_unknown(
        raw,
        _DEFINITION_KEYS,
        path=definition_path,
        field="definition",
    )
    api_version = _required_string(raw, "api_version", path=definition_path)
    if api_version != DEFINITION_API_VERSION:
        raise DeploymentConfigError(
            f"{definition_path} api_version must be {DEFINITION_API_VERSION!r}; "
            f"observed {api_version!r}"
        )
    definition_id = _identifier(raw, "id", path=definition_path)
    for required in ("schema_dir", "source_registry"):
        _required_string(raw, required, path=definition_path)
    has_attestors = "checkpoint_attestors" in raw
    has_attestor_root = "checkpoint_attestor_root" in raw
    if has_attestor_root:
        _required_string(raw, "checkpoint_attestor_root", path=definition_path)
        if not has_attestors:
            raise DeploymentConfigError(
                f"{definition_path} checkpoint_attestor_root requires "
                "checkpoint_attestors"
            )
    if has_attestors:
        if not has_attestor_root:
            raise DeploymentConfigError(
                f"{definition_path} checkpoint_attestors require an explicit "
                "checkpoint_attestor_root so executable attestor bytes are "
                "bound by the definition digest"
            )
        from okg.substrate.publisher.checkpoint_attestations import (
            CheckpointAttestationConfigurationError,
            inspect_checkpoint_attestor_declaration_block,
        )

        try:
            inspect_checkpoint_attestor_declaration_block(
                raw["checkpoint_attestors"],
                deployment_name=definition_id,
            )
        except CheckpointAttestationConfigurationError as exc:
            raise DeploymentConfigError(
                f"{definition_path} checkpoint_attestors are invalid: {exc}"
            ) from exc
    viz = raw.get("viz")
    if viz is not None:
        if not isinstance(viz, dict):
            raise DeploymentConfigError(
                f"{definition_path} field 'viz' must be a mapping"
            )
        misplaced = sorted(set(viz) & _VIZ_ENVIRONMENT_KEYS)
        if misplaced:
            raise DeploymentConfigError(
                f"{definition_path} viz declares environment-owned endpoint field(s) "
                f"{misplaced}"
            )
    return definition_id


def _parse_database(
    raw: Mapping[str, Any],
    *,
    descriptor_path: Path,
    environ: Mapping[str, str],
    container_uid: int | None,
    container_gid: int | None,
    postgres_capacity: tuple[
        int | None,
        int | None,
        int | None,
        int | None,
        int | None,
    ],
) -> DatabaseResource:
    allowed = frozenset(
        {
            "dsn_secret_ref",
            "name",
            "container",
            "volume",
            "host_port",
            "dbos_identity",
            "shm_size",
            "memory_limit",
            "wal",
        }
    )
    _reject_unknown(raw, allowed, path=descriptor_path, field="database")
    try:
        wal_settings = resolve_wal_settings(raw.get("wal"))
    except ValueError as exc:
        raise DeploymentConfigError(f"{descriptor_path} database.wal: {exc}") from exc
    secret_ref = _required_string(raw, "dsn_secret_ref", path=descriptor_path)
    database_name = _required_string(raw, "name", path=descriptor_path)
    if not _DATABASE_RE.fullmatch(database_name):
        raise DeploymentConfigError(
            f"{descriptor_path} database.name is invalid: {database_name!r}"
        )
    dsn = _resolve_secret_ref(secret_ref, path=descriptor_path, environ=environ)
    host_port = _port(raw, "host_port", path=descriptor_path)
    target = _database_target(dsn, path=descriptor_path)
    if target.database != database_name:
        raise DeploymentConfigError(
            f"{descriptor_path} database.name {database_name!r} does not match "
            f"resolved DSN database {target.database!r}"
        )
    if target.port != host_port:
        raise DeploymentConfigError(
            f"{descriptor_path} database.host_port {host_port} does not match "
            f"resolved DSN port {target.port}"
        )
    (
        max_worker_processes,
        max_parallel_workers,
        timescaledb_max_background_workers,
        timescaledb_job_worker_reserve,
        _configured_headroom,
    ) = postgres_capacity
    return DatabaseResource(
        dsn_secret_ref=secret_ref,
        dsn=dsn,
        name=database_name,
        container=_identifier(raw, "container", path=descriptor_path),
        volume=_identifier(raw, "volume", path=descriptor_path),
        host_port=host_port,
        dbos_identity=_required_string(raw, "dbos_identity", path=descriptor_path),
        container_uid=container_uid,
        container_gid=container_gid,
        shm_size=_optional_resource_size(
            raw,
            "shm_size",
            path=descriptor_path,
        ),
        memory_limit=_optional_resource_size(
            raw,
            "memory_limit",
            path=descriptor_path,
        ),
        max_worker_processes=max_worker_processes,
        max_parallel_workers=max_parallel_workers,
        timescaledb_max_background_workers=(
            timescaledb_max_background_workers
        ),
        timescaledb_job_worker_reserve=timescaledb_job_worker_reserve,
        wal_settings=wal_settings,
    )


def _postgres_capacity_values(
    raw: Mapping[str, Any],
    *,
    path: Path,
) -> tuple[int, int, int, int, int]:
    max_worker_processes = _positive_int(
        raw,
        "max_worker_processes",
        path=path,
    )
    max_parallel_workers = _positive_int(
        raw,
        "max_parallel_workers",
        path=path,
    )
    timescaledb_max_background_workers = _positive_int(
        raw,
        "timescaledb_max_background_workers",
        path=path,
    )
    timescaledb_job_worker_reserve = _positive_int(
        raw,
        "timescaledb_job_worker_reserve",
        path=path,
    )
    configured_headroom = (
        max_worker_processes
        - max_parallel_workers
        - timescaledb_max_background_workers
    )
    if configured_headroom < POSTGRES_MIN_BACKGROUND_WORKER_HEADROOM:
        raise DeploymentConfigError(
            f"{path} database worker capacity leaves "
            f"{configured_headroom} process(es) of headroom; at least "
            f"{POSTGRES_MIN_BACKGROUND_WORKER_HEADROOM} are required after "
            "max_parallel_workers and timescaledb_max_background_workers"
        )
    return (
        max_worker_processes,
        max_parallel_workers,
        timescaledb_max_background_workers,
        timescaledb_job_worker_reserve,
        configured_headroom,
    )


def _load_postgres_capacity(
    *,
    descriptor_path: Path,
    allow_legacy: bool,
) -> tuple[
    int | None,
    int | None,
    int | None,
    int | None,
    int | None,
]:
    """Load capacity through a sidecar accepted by closed v1 controllers."""
    capacity_path = descriptor_path.parent / "postgres-capacity.yaml"
    if not capacity_path.exists():
        if allow_legacy:
            return (None, None, None, None, None)
        raise DeploymentConfigError(
            f"{descriptor_path} requires sealed Postgres capacity declaration"
        )
    raw = _load_yaml_mapping(capacity_path, label="Postgres capacity")
    _reject_unknown(
        raw,
        frozenset({
            "api_version",
            "max_worker_processes",
            "max_parallel_workers",
            "timescaledb_max_background_workers",
            "timescaledb_job_worker_reserve",
        }),
        path=capacity_path,
        field="Postgres capacity",
    )
    api_version = _required_string(raw, "api_version", path=capacity_path)
    if api_version != POSTGRES_CAPACITY_API_VERSION:
        raise DeploymentConfigError(
            f"{capacity_path} api_version must be "
            f"{POSTGRES_CAPACITY_API_VERSION!r}; observed {api_version!r}"
        )
    values = _postgres_capacity_values(raw, path=capacity_path)
    if values[2] <= values[3]:
        raise DeploymentConfigError(
            "Postgres capacity timescaledb_max_background_workers must exceed "
            "timescaledb_job_worker_reserve so at least one database scheduler "
            "can run"
        )
    return values


def _postgres_capacity_digest(
    values: tuple[
        int | None,
        int | None,
        int | None,
        int | None,
        int | None,
    ],
) -> str | None:
    """Digest the validated semantic capacity contract, not YAML formatting."""
    if values[0] is None:
        return None
    (
        max_worker_processes,
        max_parallel_workers,
        timescaledb_max_background_workers,
        timescaledb_job_worker_reserve,
        _configured_headroom,
    ) = values
    return _mapping_digest(
        {
            "api_version": POSTGRES_CAPACITY_API_VERSION,
            "max_worker_processes": max_worker_processes,
            "max_parallel_workers": max_parallel_workers,
            "timescaledb_max_background_workers": (
                timescaledb_max_background_workers
            ),
            "timescaledb_job_worker_reserve": timescaledb_job_worker_reserve,
        }
    )


def _load_postgres_container_owner(
    *,
    descriptor_path: Path,
    allow_legacy: bool,
) -> tuple[int | None, int | None]:
    """Load the sealed, v1-controller-compatible Postgres owner contract.

    The environment/v1 schema is closed and deployed controllers reject new
    descriptor fields. A sibling sidecar crosses that trust transition safely:
    v1 includes it in the sealed artifact tree without interpreting it, while
    current code validates it before rendering or installing services.
    """
    owner_path = descriptor_path.parent / "postgres-container-owner.yaml"
    if not owner_path.exists():
        if allow_legacy:
            return None, None
        raise DeploymentConfigError(
            f"{descriptor_path} requires sealed Postgres container owner "
            f"declaration {owner_path}"
        )
    raw = _load_yaml_mapping(owner_path, label="Postgres container owner")
    _reject_unknown(
        raw,
        frozenset({"api_version", "uid", "gid"}),
        path=owner_path,
        field="Postgres container owner",
    )
    api_version = _required_string(raw, "api_version", path=owner_path)
    if api_version != POSTGRES_CONTAINER_OWNER_API_VERSION:
        raise DeploymentConfigError(
            f"{owner_path} api_version must be "
            f"{POSTGRES_CONTAINER_OWNER_API_VERSION!r}; observed {api_version!r}"
        )
    return (
        _positive_int(raw, "uid", path=owner_path),
        _positive_int(raw, "gid", path=owner_path),
    )


def _optional_resource_size(
    raw: Mapping[str, Any],
    key: str,
    *,
    path: Path,
) -> str | None:
    value = raw.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not _RESOURCE_SIZE_RE.fullmatch(value):
        raise DeploymentConfigError(
            f"{path} database.{key} must be a positive Compose size "
            "such as '2gb'"
        )
    return value.lower()


def _parse_release(
    raw: Mapping[str, Any],
    *,
    descriptor_path: Path,
    environ: Mapping[str, str],
    tier: str,
) -> ReleaseResource:
    allowed = frozenset(
        {
            "mode",
            "artifact_store",
            "active_pointer",
            "previous_pointer",
            "pin",
        }
    )
    _reject_unknown(raw, allowed, path=descriptor_path, field="release")
    mode = _required_string(raw, "mode", path=descriptor_path)
    if mode not in {"sealed", "tracking"}:
        raise DeploymentConfigError(
            f"{descriptor_path} release.mode must be 'sealed' or 'tracking'"
        )
    if tier in PRODUCTION_LIKE_TIERS and mode != "sealed":
        raise DeploymentConfigError(
            f"{descriptor_path} {tier} environments require release.mode 'sealed'"
        )
    pin_raw = raw.get("pin") or {}
    if not isinstance(pin_raw, Mapping):
        raise DeploymentConfigError(f"{descriptor_path} release.pin must be a mapping")
    _reject_unknown(
        pin_raw,
        frozenset({"code_repository", "code_revision", "deployment_revision"}),
        path=descriptor_path,
        field="release.pin",
    )
    pin = {
        str(key): _required_string(pin_raw, str(key), path=descriptor_path)
        for key in pin_raw
    }
    return ReleaseResource(
        mode=mode,
        artifact_store=_resolve_path(
            raw,
            "artifact_store",
            descriptor_path=descriptor_path,
            environ=environ,
        ),
        active_pointer=_resolve_path(
            raw,
            "active_pointer",
            descriptor_path=descriptor_path,
            environ=environ,
        ),
        previous_pointer=_resolve_path(
            raw,
            "previous_pointer",
            descriptor_path=descriptor_path,
            environ=environ,
        ),
        pin=MappingProxyType(pin),
    )


def _parse_runtime(
    raw: Mapping[str, Any],
    *,
    descriptor_path: Path,
    environ: Mapping[str, str],
) -> RuntimeResource:
    allowed = frozenset(
        {
            "worker_service",
            "mirror_service",
            "mirror_enabled",
            "state_root",
            "data_root",
            "local_env_file",
            "log_root",
            "config",
            "credential_refs",
        }
    )
    _reject_unknown(raw, allowed, path=descriptor_path, field="runtime")
    config = raw.get("config") or {}
    if not isinstance(config, dict):
        raise DeploymentConfigError(
            f"{descriptor_path} runtime.config must be a mapping"
        )
    credential_refs_raw = raw.get("credential_refs") or {}
    if not isinstance(credential_refs_raw, dict):
        raise DeploymentConfigError(
            f"{descriptor_path} runtime.credential_refs must be a mapping"
        )
    credential_refs: dict[str, tuple[str, ...]] = {}
    for service, refs in credential_refs_raw.items():
        if not isinstance(service, str) or not service.strip() or not isinstance(refs, list):
            raise DeploymentConfigError(
                f"{descriptor_path} runtime.credential_refs entries are invalid"
            )
        parsed: list[str] = []
        for ref in refs:
            if not isinstance(ref, str) or _ENV_REF_RE.fullmatch(ref) is None:
                raise DeploymentConfigError(
                    f"{descriptor_path} credential reference must use env:VARIABLE syntax"
                )
            parsed.append(ref)
        if len(set(parsed)) != len(parsed):
            raise DeploymentConfigError(
                f"{descriptor_path} credential references must be unique"
            )
        credential_refs[service] = tuple(parsed)
    mirror_enabled = raw.get("mirror_enabled", False)
    if not isinstance(mirror_enabled, bool):
        raise DeploymentConfigError(
            f"{descriptor_path} runtime.mirror_enabled must be boolean"
        )
    mirror_service = raw.get("mirror_service")
    if mirror_service is not None and (
        not isinstance(mirror_service, str) or not mirror_service.strip()
    ):
        raise DeploymentConfigError(
            f"{descriptor_path} runtime.mirror_service must be a service id"
        )
    if mirror_enabled and mirror_service is None:
        raise DeploymentConfigError(
            f"{descriptor_path} enabled mirror requires runtime.mirror_service"
        )
    state_root = _resolve_path(
        raw, "state_root", descriptor_path=descriptor_path, environ=environ
    )
    data_root = (
        _resolve_path(raw, "data_root", descriptor_path=descriptor_path, environ=environ)
        if "data_root" in raw
        else state_root / "data"
    )
    return RuntimeResource(
        worker_service=_required_string(raw, "worker_service", path=descriptor_path),
        mirror_service=mirror_service,
        mirror_enabled=mirror_enabled,
        state_root=state_root,
        data_root=data_root,
        local_env_file=_resolve_path(
            raw,
            "local_env_file",
            descriptor_path=descriptor_path,
            environ=environ,
        ),
        log_root=_resolve_path(
            raw,
            "log_root",
            descriptor_path=descriptor_path,
            environ=environ,
        ),
        config=dict(config),
        credential_refs=credential_refs,
    )


def _parse_endpoint(
    raw: Mapping[str, Any],
    *,
    descriptor_path: Path,
    field: str,
) -> EndpointResource:
    keys = {"enabled", "service", "host", "port"}
    if field == "ui":
        keys.add("auth_token_env")
    _reject_unknown(
        raw,
        frozenset(keys),
        path=descriptor_path,
        field=field,
    )
    enabled = raw.get("enabled", True)
    if not isinstance(enabled, bool):
        raise DeploymentConfigError(f"{descriptor_path} {field}.enabled must be boolean")
    auth_token_env: str | None = None
    if "auth_token_env" in raw:
        auth_token_env = _required_string(
            raw, "auth_token_env", path=descriptor_path,
        )
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", auth_token_env) is None:
            raise DeploymentConfigError(
                f"{descriptor_path} {field}.auth_token_env must be an "
                "environment variable NAME, never the token value"
            )
    return EndpointResource(
        service=_required_string(raw, "service", path=descriptor_path),
        host=_required_string(raw, "host", path=descriptor_path),
        port=_port(raw, "port", path=descriptor_path),
        enabled=enabled,
        auth_token_env=auth_token_env,
    )


def _parse_mcp(
    raw: Mapping[str, Any],
    *,
    descriptor_path: Path,
    environment_id: str,
) -> McpResource:
    allowed = frozenset({"transport", "registration_id", "launcher_environment"})
    _reject_unknown(raw, allowed, path=descriptor_path, field="mcp")
    transport = _required_string(raw, "transport", path=descriptor_path)
    if transport != "stdio":
        raise DeploymentConfigError(
            f"{descriptor_path} mcp.transport must be 'stdio'; HTTP MCP requires "
            "an explicit service/network contract"
        )
    launcher_environment = _required_string(
        raw,
        "launcher_environment",
        path=descriptor_path,
    )
    if launcher_environment != environment_id:
        raise DeploymentConfigError(
            f"{descriptor_path} mcp.launcher_environment must equal environment id "
            f"{environment_id!r}"
        )
    return McpResource(
        transport=transport,
        registration_id=_identifier(raw, "registration_id", path=descriptor_path),
        launcher_environment=launcher_environment,
    )


def _parse_readiness(
    raw: Mapping[str, Any],
    *,
    descriptor_path: Path,
    environ: Mapping[str, str],
    state_root: Path,
) -> ReadinessResource:
    _reject_unknown(
        raw,
        frozenset(
            {
                "policy",
                "evidence_ref",
                "prerequisite_pacts",
                "requires_clone_rebind",
                "override_authority",
                "source_groups",
            }
        ),
        path=descriptor_path,
        field="readiness",
    )
    evidence = _mapping(raw, "evidence_ref", path=descriptor_path)
    _reject_unknown(
        evidence,
        frozenset({"store", "path", "max_age_seconds"}),
        path=descriptor_path,
        field="readiness.evidence_ref",
    )
    store = _required_string(evidence, "store", path=descriptor_path)
    if store != "runtime_state":
        raise DeploymentConfigError(
            f"{descriptor_path} readiness.evidence_ref.store must be 'runtime_state'"
        )
    evidence_value = _required_string(evidence, "path", path=descriptor_path)
    expanded = _expand_template(
        evidence_value,
        field="readiness.evidence_ref.path",
        path=descriptor_path,
        environ=environ,
    )
    evidence_path = Path(expanded).expanduser()
    if not evidence_path.is_absolute():
        evidence_path = state_root / evidence_path
    prerequisites = raw.get("prerequisite_pacts") or []
    override_authority = raw.get("override_authority")
    if override_authority is not None and override_authority != "staging_fail_closed":
        raise DeploymentConfigError(
            f"{descriptor_path} readiness.override_authority must be "
            "'staging_fail_closed' when set"
        )
    raw_source_groups = raw.get("source_groups") or {}
    if not isinstance(raw_source_groups, Mapping):
        raise DeploymentConfigError(
            f"{descriptor_path} readiness.source_groups must be a mapping"
        )
    source_groups: dict[str, dict[str, tuple[str, ...]]] = {}
    for profile, raw_groups in raw_source_groups.items():
        if (
            not isinstance(profile, str)
            or not profile.strip()
            or not isinstance(raw_groups, Mapping)
            or not raw_groups
        ):
            raise DeploymentConfigError(
                f"{descriptor_path} readiness.source_groups profiles must map "
                "non-empty names to non-empty group mappings"
            )
        groups: dict[str, tuple[str, ...]] = {}
        for group, raw_sources in raw_groups.items():
            if (
                not isinstance(group, str)
                or not group.strip()
                or not isinstance(raw_sources, list)
                or not raw_sources
                or any(
                    not isinstance(source, str) or not source.strip()
                    for source in raw_sources
                )
            ):
                raise DeploymentConfigError(
                    f"{descriptor_path} readiness.source_groups.{profile} entries "
                    "must be non-empty source-name lists"
                )
            groups[group] = tuple(raw_sources)
        source_groups[profile] = groups
    requires_clone_rebind = raw.get("requires_clone_rebind", False)
    if not isinstance(requires_clone_rebind, bool):
        raise DeploymentConfigError(
            f"{descriptor_path} readiness.requires_clone_rebind must be boolean"
        )
    if (
        not isinstance(prerequisites, list)
        or any(
            not isinstance(value, str) or not value.strip()
            for value in prerequisites
        )
        or len(set(prerequisites)) != len(prerequisites)
    ):
        raise DeploymentConfigError(
            f"{descriptor_path} readiness.prerequisite_pacts must be a "
            "unique list of non-empty PACT ids"
        )
    return ReadinessResource(
        policy=_required_string(raw, "policy", path=descriptor_path),
        evidence_store=store,
        evidence_path=evidence_path.resolve(),
        max_age_seconds=_positive_int(
            evidence,
            "max_age_seconds",
            path=descriptor_path,
        ),
        prerequisite_pacts=tuple(str(value) for value in prerequisites),
        requires_clone_rebind=requires_clone_rebind,
        override_authority=override_authority,
        source_groups=source_groups,
    )


def _resolve_secret_ref(
    secret_ref: str,
    *,
    path: Path,
    environ: Mapping[str, str],
) -> str:
    match = _ENV_REF_RE.fullmatch(secret_ref)
    if match is None:
        raise DeploymentConfigError(
            f"{path} secret reference must use env:VARIABLE syntax; "
            f"observed {secret_ref!r}"
        )
    variable = match.group(1)
    value = environ.get(variable)
    if not isinstance(value, str) or not value.strip():
        raise DeploymentConfigError(
            f"{path} required secret reference env:{variable} is unavailable; "
            "no cwd or .okg/local.env fallback is permitted"
        )
    return value


def _database_target(dsn: str, *, path: Path) -> _DatabaseTarget:
    try:
        parsed = urlsplit(dsn)
        port = parsed.port or 5432
    except ValueError as exc:
        raise DeploymentConfigError(f"{path} database DSN is invalid: {exc}") from exc
    if parsed.scheme not in {"postgres", "postgresql"}:
        raise DeploymentConfigError(
            f"{path} database DSN must use postgres:// or postgresql://"
        )
    if not parsed.hostname:
        raise DeploymentConfigError(f"{path} database DSN must name a host")
    database = unquote(parsed.path.lstrip("/"))
    if not database or "/" in database:
        raise DeploymentConfigError(
            f"{path} database DSN must name exactly one database"
        )
    host = parsed.hostname.lower()
    if host in {"127.0.0.1", "::1", "localhost"}:
        host = "loopback"
    return _DatabaseTarget(host=host, port=port, database=database)


def _endpoint_identity(endpoint: EndpointResource) -> str:
    host = endpoint.host.lower()
    if host in {"127.0.0.1", "::1", "localhost"}:
        host = "loopback"
    return f"{host}:{endpoint.port}"


def _nested_mutable_path_collisions(
    rows: list[ResourceIdentity],
) -> list[dict[str, Any]]:
    mutable = [row for row in rows if row.collision_domain == "mutable_path"]
    collisions: list[dict[str, Any]] = []
    for index, left in enumerate(mutable):
        left_path = Path(left.value)
        for right in mutable[index + 1 :]:
            if left.environment_id == right.environment_id or left.value == right.value:
                continue
            right_path = Path(right.value)
            if right_path.is_relative_to(left_path):
                ancestor, descendant = left, right
            elif left_path.is_relative_to(right_path):
                ancestor, descendant = right, left
            else:
                continue
            collisions.append(
                {
                    "collision_domain": "mutable_path",
                    "relationship": "ancestor_descendant",
                    "value": f"{ancestor.value} contains {descendant.value}",
                    "paths": [ancestor.value, descendant.value],
                    "owners": [ancestor.to_dict(), descendant.to_dict()],
                }
            )
    return collisions


def _resolve_path(
    raw: Mapping[str, Any],
    key: str,
    *,
    descriptor_path: Path,
    environ: Mapping[str, str],
) -> Path:
    value = _required_string(raw, key, path=descriptor_path)
    expanded = _expand_template(
        value,
        field=key,
        path=descriptor_path,
        environ=environ,
    )
    candidate = Path(expanded).expanduser()
    if not candidate.is_absolute():
        candidate = descriptor_path.parent / candidate
    return candidate.resolve()


def _expand_template(
    value: str,
    *,
    field: str,
    path: Path,
    environ: Mapping[str, str],
) -> str:
    variables = {match.group(1) for match in _TEMPLATE_RE.finditer(value)}
    secret_like = sorted(
        variable
        for variable in variables
        if any(
            marker in variable
            for marker in ("_CREDENTIAL", "_DSN", "_PASSWORD", "_SECRET", "_TOKEN")
        )
        or variable == "API_KEY"
        or variable.endswith("_API_KEY")
    )
    if secret_like:
        raise DeploymentConfigError(
            f"{path} field {field!r} cannot interpolate secret-like environment "
            f"variable(s) {secret_like}; resolved paths are reported by status"
        )
    missing = sorted({variable for variable in variables if not environ.get(variable)})
    if missing:
        raise DeploymentConfigError(
            f"{path} field {field!r} has unresolved environment variable(s) "
            f"{missing}; no cwd fallback is permitted"
        )
    return _TEMPLATE_RE.sub(lambda match: str(environ[match.group(1)]), value)


def _descriptor_path_environment(
    raw: Mapping[str, Any],
    *,
    environ: Mapping[str, str],
) -> dict[str, str]:
    fields = (
        ("sync_root",),
        ("release", "artifact_store"),
        ("release", "active_pointer"),
        ("release", "previous_pointer"),
        ("runtime", "state_root"),
        ("runtime", "local_env_file"),
        ("runtime", "log_root"),
        ("readiness", "evidence_ref", "path"),
    )
    variables: set[str] = set()
    for field_path in fields:
        value: Any = raw
        for key in field_path:
            if not isinstance(value, Mapping):
                value = None
                break
            value = value.get(key)
        if isinstance(value, str):
            variables.update(_TEMPLATE_RE.findall(value))
    return {name: str(environ[name]) for name in sorted(variables)}


def _compose_manifest(
    *,
    definition_raw: Mapping[str, Any],
    definition_path: Path,
    graph_deployment_name: str,
    sync_root: Path | None,
    database: DatabaseResource,
    runtime: RuntimeResource,
    webhook: EndpointResource,
    readiness: ReadinessResource,
    tier: str,
) -> DeploymentManifest:
    graph_raw = {
        key: value
        for key, value in definition_raw.items()
        if key not in {"api_version", "id"}
    }
    graph_raw["name"] = graph_deployment_name
    if sync_root is not None:
        graph_raw["sync_root"] = str(sync_root)
    graph_raw["postgres"] = {
        "dsn": database.dsn,
        "database": database.name,
    }
    runtime_config = dict(runtime.config)
    # Arm the destructive-verb guard from the environment tier. The
    # `okg runtime reset` guard refuses any `runtime.environment` other than
    # "development" (see runtime/dev.py); mapping the tier marker here means
    # staging + production environments self-arm without a per-manifest edit,
    # while development environments remain resettable. An explicit
    # `runtime.config.environment` in the descriptor still wins.
    runtime_config.setdefault("environment", tier)
    source_sync = dict(runtime_config.get("source_sync") or {})
    declared_webhook = dict(source_sync.get("webhook") or {})
    for field, expected in (
        ("enabled", webhook.enabled),
        ("host", webhook.host),
        ("port", webhook.port),
    ):
        observed = declared_webhook.get(field)
        if observed is not None and observed != expected:
            raise DeploymentConfigError(
                f"{definition_path} runtime.config.source_sync.webhook.{field} "
                f"conflicts with environment-owned webhook {expected!r}"
            )
    source_sync["webhook"] = {
        **declared_webhook,
        "enabled": webhook.enabled,
        "host": webhook.host,
        "port": webhook.port,
    }
    runtime_config["source_sync"] = source_sync
    graph_raw["runtime"] = runtime_config
    if readiness.override_authority is not None or readiness.source_groups:
        graph_raw["readiness"] = {
            **(
                {"override_authority": readiness.override_authority}
                if readiness.override_authority is not None
                else {}
            ),
            **(
                {
                    "source_groups": {
                        profile: {
                            group: list(sources)
                            for group, sources in groups.items()
                        }
                        for profile, groups in readiness.source_groups.items()
                    }
                }
                if readiness.source_groups
                else {}
            ),
        }
    substituted = _substitute_manifest_placeholders(
        graph_raw,
        definition_path.parent,
    )
    return DeploymentManifest._build_from_raw(
        substituted,
        definition_path.parent,
        definition_path,
        strict=True,
    )


def _nested_definition_paths(
    raw: Mapping[str, Any],
    *,
    definition_path: Path,
) -> tuple[tuple[str, Path], ...]:
    """`(<dotted field>, resolved path)` for each declared nested path field."""
    from okg.substrate.agent_surface_paths import agent_surface_path_values

    found: list[tuple[str, Path]] = []
    for field_path, value in agent_surface_path_values(raw):
        if not isinstance(value, str) or not value.strip():
            continue
        substituted = _substitute_manifest_placeholders(
            value,
            definition_path.parent,
        )
        candidate = Path(substituted).expanduser()
        if not candidate.is_absolute():
            candidate = definition_path.parent / candidate
        found.append((".".join(field_path), candidate.resolve()))
    return tuple(found)


def _without_nested_definition_paths(raw: Mapping[str, Any]) -> dict[str, Any]:
    """A copy of `raw` minus the nested path strings, for the substance
    mapping: their string values may differ between tiers while the bytes
    behind them, compared as asset slots, must not."""
    import copy

    from okg.substrate.agent_surface_paths import AGENT_SURFACE_PATH_FIELDS

    stripped = copy.deepcopy(dict(raw))
    for field_path in AGENT_SURFACE_PATH_FIELDS:
        parent: Any = stripped
        for part in field_path[:-1]:
            parent = parent.get(part) if isinstance(parent, dict) else None
        if isinstance(parent, dict):
            parent.pop(field_path[-1], None)
    return stripped


def _mapping_digest(raw: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        raw,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


# Keys that legitimately differ between the staging and production
# definitions of one foundation: identity/prose plus per-tier nomos policy
# (operationalize-okg-ci-deployment-pipeline req.production-faithful-staging
# hardens staging's posture, superseding the earlier equal-whole-digest
# phrasing in make-okg-ci-deployment-lifecycle-coherent). Everything else --
# modules, enrichers, embedding, consistency, and the bytes behind every
# path-referenced asset -- is shared substance and must match, so a NEW
# definition key defaults to must-match (fail closed).
_DEFINITION_TIER_LOCAL_KEYS = frozenset({"id", "description", "nomos"})

_SUBSTANCE_MISSING = "missing"
_SUBSTANCE_MAPPING_KEY = "mapping"


def definition_substance(
    raw: Mapping[str, Any],
    *,
    definition_path: Path,
) -> dict[str, str]:
    """Comparison keys for the tier-shared substance of one definition.

    Returns ``{"mapping": <digest>, <asset slot>: <asset digest or
    "missing">}`` where ``mapping`` digests the raw definition minus
    tier-local keys and minus the string path keys (whose string values
    legitimately differ between tiers while their referenced bytes must not).
    An asset slot is a path key, or ``<key>:<owner>`` for a directory named by
    a mapping-valued key such as ``module_providers``. Those owner-keyed
    directory names stay in ``mapping`` as well, so the two tiers must agree
    on which owners exist and where they read from, and -- through the asset
    slots -- on the bytes underneath.
    """
    substance: dict[str, str] = {}
    normalized = {
        key: value
        for key, value in _without_nested_definition_paths(raw).items()
        if key not in _DEFINITION_TIER_LOCAL_KEYS
        and key not in _DEFINITION_PATH_KEYS
    }
    substance[_SUBSTANCE_MAPPING_KEY] = _mapping_digest(normalized)
    for key in sorted(_DEFINITION_PATH_KEYS):
        value = raw.get(key)
        if not isinstance(value, str) or not value.strip():
            continue
        substituted = _substitute_manifest_placeholders(
            value,
            definition_path.parent,
        )
        candidate = Path(substituted).expanduser()
        if not candidate.is_absolute():
            candidate = definition_path.parent / candidate
        substance[key] = _path_content_digest(key=key, path=candidate.resolve())
    for slot, directory in _directory_mapping_assets(
        raw,
        definition_path=definition_path,
    ):
        substance[slot] = _path_content_digest(key=slot, path=directory)
    for slot, path in _nested_definition_paths(
        raw,
        definition_path=definition_path,
    ):
        substance[slot] = _path_content_digest(key=slot, path=path)
    default_pre_join = definition_path.parent / "mcp_pre_join_tools.yaml"
    if "mcp_pre_join_tools" not in substance and default_pre_join.is_file():
        substance["mcp_pre_join_tools"] = _path_content_digest(
            key="mcp_pre_join_tools",
            path=default_pre_join,
        )
    return substance


def _resolve_directory_mapping_key(
    key: str,
    raw: Mapping[str, Any],
    *,
    definition_path: Path,
) -> dict[str, Path]:
    """Directories named by a mapping-valued definition key.

    Resolution goes through the same reader deployment composition uses, so
    the digest binds exactly the bytes that get read -- including that
    reader's refusal of an absolute path, a `.`/`..` segment, a symlinked
    component, and a target outside the definition directory. `strict=False`
    keeps a declared-but-absent directory a digestable fact (it lands as
    `missing`, which never compares equal) rather than a load failure.
    """
    if key == "module_providers":
        # Imported here: the package `__init__` imports this module.
        from okg.substrate.deployments import resolve_declared_module_providers

        return resolve_declared_module_providers(
            raw,
            definition_path.parent,
            manifest_path=definition_path,
            strict=False,
        )
    raise DeploymentConfigError(
        f"no directory reader for definition key {key!r} "
        f"(Definition: {definition_path})"
    )


def _directory_mapping_assets(
    raw: Mapping[str, Any],
    *,
    definition_path: Path,
) -> tuple[tuple[str, Path], ...]:
    """`(<key>:<owner>, directory)` for every mapping-valued path key.

    Sorted by slot name so the walk is deterministic across filesystems.
    """
    assets: list[tuple[str, Path]] = []
    for key in sorted(_DEFINITION_DIRECTORY_MAPPING_KEYS):
        if key not in raw:
            continue
        resolved = _resolve_directory_mapping_key(
            key,
            raw,
            definition_path=definition_path,
        )
        for owner, directory in resolved.items():
            assets.append((f"{key}:{owner}", directory.resolve()))
    return tuple(sorted(assets, key=lambda item: item[0]))


def _path_content_digest(*, key: str, path: Path) -> str:
    if not path.exists():
        return _SUBSTANCE_MISSING
    digest = hashlib.sha256()
    _update_path_digest(digest, key=key, path=path)
    return f"sha256:{digest.hexdigest()}"


def shared_definition_substance_problems(
    a: Mapping[str, str],
    b: Mapping[str, str],
    *,
    a_label: str = "staging",
    b_label: str = "production",
) -> tuple[str, ...]:
    """Why two definition substances are NOT the same foundation.

    Empty result means shared substance. A declared asset that resolves to a
    missing target is a problem on its own -- missing may never compare equal
    to missing -- and a comparison with no path-backed assets at all is
    refused rather than passed, so an empty foundation cannot slip through as
    agreement.
    """
    problems: list[str] = []
    asset_keys = (set(a) | set(b)) - {_SUBSTANCE_MAPPING_KEY}
    if not asset_keys:
        problems.append(
            "no path-backed definition assets to compare; refusing an empty"
            " foundation comparison"
        )
    for key in sorted({_SUBSTANCE_MAPPING_KEY, *asset_keys}):
        left = a.get(key)
        right = b.get(key)
        if left == _SUBSTANCE_MISSING or right == _SUBSTANCE_MISSING:
            sides = " and ".join(
                label
                for label, value in ((a_label, left), (b_label, right))
                if value == _SUBSTANCE_MISSING
            )
            problems.append(f"{key}: declared asset target is missing in {sides}")
            continue
        if left != right:
            if left is None or right is None:
                declared, undeclared = (
                    (b_label, a_label) if left is None else (a_label, b_label)
                )
                problems.append(
                    f"{key}: declared in {declared} but not in {undeclared}"
                )
            else:
                problems.append(f"{key}: content differs between {a_label} and {b_label}")
    return tuple(problems)


def _definition_digest(
    raw: Mapping[str, Any],
    *,
    definition_path: Path,
) -> str:
    digest = hashlib.sha256()
    digest.update(_mapping_digest(raw).encode("ascii"))
    for key in sorted(_DEFINITION_PATH_KEYS):
        value = raw.get(key)
        if not isinstance(value, str) or not value.strip():
            continue
        substituted = _substitute_manifest_placeholders(
            value,
            definition_path.parent,
        )
        candidate = Path(substituted).expanduser()
        if not candidate.is_absolute():
            candidate = definition_path.parent / candidate
        _update_path_digest(digest, key=key, path=candidate.resolve())
    for slot, directory in _directory_mapping_assets(
        raw,
        definition_path=definition_path,
    ):
        _update_path_digest(digest, key=slot, path=directory)
    for slot, path in _nested_definition_paths(
        raw,
        definition_path=definition_path,
    ):
        _update_path_digest(digest, key=slot, path=path)
    default_pre_join = definition_path.parent / "mcp_pre_join_tools.yaml"
    if "mcp_pre_join_tools" not in raw and default_pre_join.is_file():
        _update_path_digest(
            digest,
            key="mcp_pre_join_tools(default)",
            path=default_pre_join,
        )
    return f"sha256:{digest.hexdigest()}"


def _update_path_digest(digest: Any, *, key: str, path: Path) -> None:
    # Absolute checkout paths are observations, not executable identity. The
    # raw definition already contributes the declared logical path; hash only
    # the asset slot and bytes here so copied/sealed roots remain identical.
    digest.update(f"\0asset:{key}".encode("utf-8"))
    if not path.exists():
        digest.update(b"\0missing")
        return
    if path.is_file():
        digest.update(b"\0file\0")
        digest.update(path.read_bytes())
        return
    digest.update(b"\0directory")
    for child in sorted(item for item in path.rglob("*") if item.is_file()):
        if child.name == ".DS_Store" or "__pycache__" in child.parts:
            continue
        digest.update(f"\0{child.relative_to(path).as_posix()}\0".encode("utf-8"))
        digest.update(child.read_bytes())
