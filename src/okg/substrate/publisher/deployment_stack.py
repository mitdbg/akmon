"""Deployment-scoped Publisher wiring.

Direct CLI publishes, ingest inter-layer publishes, and queued publish
jobs must construct ``Publisher`` with the same deployment-owned
runtime: enrichers, projection linker config, alias resolver, native
view materializers, consistency checks, and lens edit policies.
"""
from __future__ import annotations

import importlib
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg
from okg.substrate import fastyaml
from okg.substrate.deployments import PublisherConfig


@dataclass(frozen=True)
class DeploymentPublisherStack:
    """Runtime components loaded from one deployment directory."""

    deployment_name: str
    deployment_dir: Path | None
    publisher_config: PublisherConfig
    enrichers: list[Any]
    derived_producer_policies: dict[str, dict[str, Any]]
    resolver_factory: Any | None
    extractor: Any | None
    extractor_source_subtypes: list[str] | None
    routes: list[Any] | None
    materializers: list[Any]
    maintained_derivations: Any | None
    consistency_gates: list[Any]
    post_publish_consistency_checks: list[Any]
    lenses: list[Any]
    checkpoint_attestors: list[Any]
    enqueue_embeddings: bool = False
    native_view_min_refresh_interval_ms: int = 0
    native_view_materialization_delay_ms: int = 0
    attribute_effective_view_empty_overlay_fast_path: bool = False
    history_retention_policy: dict[str, Any] | None = None
    # Opt-in flag for the resolution-aware reuse scheduler (PACT
    # consistency-resolution-aware-reuse, task.reuse.scheduler). Read from
    # consistency.resolution_aware_reuse; NOT a Publisher kwarg (the reuse pass
    # runs as a post-cycle DBOS step, not inside Publisher.cycle in v1), so it is
    # excluded from publisher_kwargs and no other deployment's cycle changes.
    resolution_aware_reuse_enabled: bool = False

    def publisher_kwargs(self) -> dict[str, Any]:
        """Keyword arguments accepted by ``Publisher``."""

        return {
            "enrichers": self.enrichers,
            "derived_producer_policies": self.derived_producer_policies,
            "resolver_factory": self.resolver_factory,
            "extractor": self.extractor,
            "extractor_source_subtypes": self.extractor_source_subtypes,
            "routes": self.routes,
            "materializers": self.materializers,
            "maintained_derivations": self.maintained_derivations,
            "consistency_gates": self.consistency_gates,
            "post_publish_consistency_checks": (
                self.post_publish_consistency_checks
            ),
            "lenses": self.lenses,
            "checkpoint_attestors": self.checkpoint_attestors,
            "enqueue_embeddings": self.enqueue_embeddings,
            "native_view_min_refresh_interval_ms": (
                self.native_view_min_refresh_interval_ms
            ),
            "native_view_materialization_delay_ms": (
                self.native_view_materialization_delay_ms
            ),
            "attribute_effective_view_empty_overlay_fast_path": (
                self.attribute_effective_view_empty_overlay_fast_path
            ),
            "history_retention_policy": self.history_retention_policy,
            "hard_statement_timeout_ms": (
                self.publisher_config.hard_statement_timeout_ms
            ),
            "hard_statement_timeout_source": (
                self.publisher_config.hard_statement_timeout_source
            ),
            "lock_hold_headroom_factor": (
                self.publisher_config.lock_hold_headroom_factor
            ),
            "lock_hold_headroom_factor_source": (
                self.publisher_config.lock_hold_headroom_factor_source
            ),
            "allow_raised_statement_timeout": (
                self.publisher_config.allow_raised_statement_timeout
            ),
            "allow_raised_statement_timeout_source": (
                self.publisher_config.allow_raised_statement_timeout_source
            ),
            "first_publish_warning_budget": (
                self.publisher_config.first_publish_warning_budget.as_dict()
                if self.publisher_config.first_publish_warning_budget is not None
                else None
            ),
            "first_publish_warning_budget_source": (
                self.publisher_config.first_publish_warning_budget_source
            ),
        }


def build_runtime_catalog_identity(
    *,
    dsn: str,
    deployment_name: str,
    deployment_dir: Path | None,
    command: str,
) -> Any:
    """Construct the ``CatalogIdentity`` the runtime publish must carry.

    Closes the #709 hole (ruling R5a): the DBOS worker path historically
    built ``Publisher`` with no ``catalog_identity`` (``publisher_kwargs()``
    carried none), so ``resolve_catalog_for_publish`` fell through to
    ``max(applied)`` and a manifest flip published silently. Both the CLI
    (`okg run`) and the DBOS worker now build the identity here so the
    manifest content-hash is compared at publish. A verified sealed runtime
    supplies named environment authority; tracking mode supplies explicit
    deployment-root authority. Missing or partial authority refuses.
    """
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.catalog.ownership import (
        WRITER_SCOPE_DEPLOYMENT_ROOT,
        WRITER_SCOPE_ENVIRONMENT,
        _writer_scope_error,
        catalog_identity_for,
    )
    from okg.substrate.errors import SealedReleaseInvalid
    from okg.substrate.runtime.release_authority import (
        verify_worker_release_authority as verify_worker_authority,
    )

    sealed_authority = verify_worker_authority(
        deployment=deployment_name,
        dsn=dsn,
    )
    if sealed_authority is not None:
        authority_dir = Path(str(sealed_authority["definition_root"])).resolve()
        if (
            deployment_dir is not None
            and Path(deployment_dir).resolve() != authority_dir
        ):
            raise SealedReleaseInvalid(
                "publisher deployment root differs from sealed runtime authority",
                data={
                    "expected": str(authority_dir),
                    "observed": str(Path(deployment_dir).resolve()),
                },
            )
        writer_scope = WRITER_SCOPE_ENVIRONMENT
        environment_id = str(sealed_authority["environment_id"])
        dep_dir = authority_dir
    else:
        if deployment_dir is None:
            raise _writer_scope_error(
                "publisher has no resolved catalog writer authority",
                reason_code="catalog_writer_scope_required",
                data={"deployment_name": deployment_name},
                fix_hint=(
                    "resolve a deployment directory, or launch from a verified "
                    "sealed environment"
                ),
            )
        writer_scope = WRITER_SCOPE_DEPLOYMENT_ROOT
        environment_id = None
        dep_dir = Path(deployment_dir).resolve()

    composed = compose_catalog(dep_dir)
    return catalog_identity_for(
        composed=composed,
        deployment_dir=dep_dir,
        command=command,
        writer_scope=writer_scope,
        environment_id=environment_id,
    )


def publisher_kwargs_from_env() -> dict[str, Any]:
    """Operator env overrides shared by publisher-backed commands."""

    publisher_kwargs: dict[str, Any] = {}
    gate_timeout_env = os.environ.get("OKG_GATE_TIMEOUT_MS")
    if gate_timeout_env:
        try:
            publisher_kwargs["gate_total_timeout_ms"] = int(
                gate_timeout_env
            )
        except ValueError:
            # Invalid override values fall back to the publisher default.
            pass
    min_refresh_env = os.environ.get("OKG_NATIVE_VIEW_MIN_REFRESH_MS")
    if min_refresh_env:
        try:
            publisher_kwargs["native_view_min_refresh_interval_ms"] = int(
                min_refresh_env
            )
        except ValueError:
            # Invalid override values fall back to the publisher default.
            pass
    delay_env = os.environ.get("OKG_NATIVE_VIEW_MATERIALIZATION_DELAY_MS")
    if delay_env:
        try:
            publisher_kwargs["native_view_materialization_delay_ms"] = int(
                delay_env
            )
        except ValueError:
            # Invalid override values fall back to the publisher default.
            pass
    attr_view_empty_env = os.environ.get(
        "OKG_ATTRIBUTE_EFFECTIVE_VIEW_EMPTY_OVERLAY_FAST_PATH",
    )
    if attr_view_empty_env:
        publisher_kwargs["attribute_effective_view_empty_overlay_fast_path"] = (
            attr_view_empty_env in {"1", "true", "TRUE", "yes", "YES"}
        )
    return publisher_kwargs


def build_deployment_publisher_stack(
    *,
    dsn: str,
    deployment_name: str,
    deployment_dir: Path | None = None,
) -> DeploymentPublisherStack:
    """Load the deployment-owned pieces used by Publisher.

    Returns an empty stack when the deployment directory is absent.
    That preserves the historical one-DB default deployment path while
    making named deployments fail loudly through their loaders when a
    configured component is malformed.

    Cached: parses 7+ YAML files per call (lens definitions, enrichers,
    consistency gates, post-publish consistency checks, projection
    extractor, materializers/discovery, embedding config). Measured 63
    ms per call inside `_should_skip_layer_publish` during ingest;
    keyed by (dsn, deployment_name, str(deployment_dir), deployment
    stack token) so the same deployment in one process pays once while
    deployment-owned config edits invalidate the stack automatically.
    """
    dep_dir = Path(deployment_dir) if deployment_dir is not None else None
    if dep_dir is None:
        from okg.substrate.deployments import find_deployment_dir
        dep_dir = find_deployment_dir(deployment_name)
    cache_key = (
        dsn,
        deployment_name,
        str(dep_dir) if dep_dir else None,
        _deployment_stack_cache_token(dep_dir),
    )
    cached = _stack_cache.get(cache_key)
    if cached is not None:
        return cached
    stack = _build_deployment_publisher_stack_uncached(
        dsn=dsn, deployment_name=deployment_name,
        deployment_dir=dep_dir,
    )
    _stack_cache[cache_key] = stack
    return stack


# Module-level cache. Keyed by (dsn, deployment_name, deployment_dir_str,
# deployment_stack_token) so multi-deployment workers (rare) get correct
# isolation and live deployment-owned config changes invalidate warmed stacks.
# Cleared by `_clear_deployment_stack_cache` in tests; otherwise lives for the
# process's lifetime.
_StackCacheToken = tuple[tuple[str, int, int], ...]
_stack_cache: dict[
    tuple[str, str, str | None, _StackCacheToken],
    "DeploymentPublisherStack",
] = {}

_STACK_CACHE_FILE_SUFFIXES = frozenset({
    ".json",
    ".py",
    ".sql",
    ".yaml",
    ".yml",
})
_STACK_CACHE_EXCLUDED_DIRS = frozenset({
    ".mypy_cache",
    ".pytest_cache",
    "__pycache__",
})


def _deployment_stack_cache_token(
    deployment_dir: Path | None,
) -> _StackCacheToken:
    """Fingerprint deployment-owned files that define Publisher wiring."""

    if deployment_dir is None:
        return ()
    dep_dir = Path(deployment_dir)
    if not dep_dir.exists():
        return ()
    token: list[tuple[str, int, int]] = []
    for path in sorted(dep_dir.rglob("*")):
        if any(part in _STACK_CACHE_EXCLUDED_DIRS for part in path.parts):
            continue
        if not path.is_file() or path.suffix not in _STACK_CACHE_FILE_SUFFIXES:
            continue
        try:
            stat = path.stat()
        except OSError:
            continue
        token.append((
            path.relative_to(dep_dir).as_posix(),
            stat.st_mtime_ns,
            stat.st_size,
        ))
    return tuple(token)


def _clear_deployment_stack_cache() -> None:
    """Test-only: drop the cache so test fixtures with fresh tmpdirs
    don't see stale stacks from prior test cases. Production callers
    never need this — worker restarts implicitly clear it."""
    _stack_cache.clear()


def _validated_enrichers(enrichers: list) -> list:
    """Enrichers loaded for publishing, with their static contracts checked."""
    from okg.substrate.publisher.enricher_loader import (
        validate_enricher_support_policies,
    )

    validate_enricher_support_policies(enrichers)
    return enrichers


def _build_deployment_publisher_stack_uncached(
    *,
    dsn: str,
    deployment_name: str,
    deployment_dir: Path | None = None,
) -> DeploymentPublisherStack:
    """Real builder — see `build_deployment_publisher_stack` docstring."""

    from okg.substrate.alias.loader import make_alias_resolver_factory
    from okg.substrate.consistency import (
        StoredLiveNodesGate,
        TransformConsistencyGate,
        load_consistency_gates,
        load_ir_consistency_checks,
        load_post_publish_consistency_checks,
    )
    from okg.substrate.consistency.ir_executor import IrPostPublishCheck
    from okg.substrate.deployments import (
        find_deployment_dir,
        inject_deployment_path,
    )
    from okg.substrate.embeddings import (
        EmbeddingConfig,
        build_embedding_execution_plan,
    )
    from okg.substrate.publisher.enricher_loader import (
        active_enricher_profiles_from_env,
        load_enrichers,
    )

    dep_dir = deployment_dir or find_deployment_dir(deployment_name)
    if dep_dir is None:
        return DeploymentPublisherStack(
            deployment_name=deployment_name,
            deployment_dir=None,
            publisher_config=PublisherConfig(),
            enrichers=[],
            derived_producer_policies={},
            resolver_factory=None,
            extractor=None,
            extractor_source_subtypes=None,
            routes=None,
            materializers=[],
            maintained_derivations=None,
            consistency_gates=[],
            post_publish_consistency_checks=[],
            lenses=[],
            checkpoint_attestors=[],
            enqueue_embeddings=False,
            native_view_min_refresh_interval_ms=0,
            native_view_materialization_delay_ms=0,
            attribute_effective_view_empty_overlay_fast_path=False,
            history_retention_policy=None,
            resolution_aware_reuse_enabled=False,
        )

    dep_dir = Path(dep_dir)
    inject_deployment_path(dep_dir)
    native_view_policy = load_native_view_refresh_policy(dep_dir)
    deployment_manifest = _lenient_manifest(dep_dir)
    embedding_plan = build_embedding_execution_plan(
        deployment_manifest.embedding_config
        if deployment_manifest is not None
        else EmbeddingConfig(),
        # A publish never embeds with a paid backend (alias resolution skips
        # it), so building this stack makes no paid probe call.
        paid_backend="absent",
    )
    chronos = (
        deployment_manifest.chronos
        if deployment_manifest is not None
        and isinstance(deployment_manifest.chronos, dict)
        else {}
    )
    history_retention = chronos.get("history_retention")
    return DeploymentPublisherStack(
        deployment_name=deployment_name,
        deployment_dir=dep_dir,
        publisher_config=(
            deployment_manifest.publisher
            if deployment_manifest is not None
            else PublisherConfig()
        ),
        # Issue #1256: refuse a node-writing enricher with no declared
        # derived-node support policy while the stack is being built, not
        # once per candidate inside the cycle this stack serves.
        enrichers=_validated_enrichers(load_enrichers(
            dep_dir,
            active_profiles=active_enricher_profiles_from_env(),
            runtime_dsn=dsn,
            deployment=deployment_name,
        )),
        derived_producer_policies=load_derived_producer_policies(dep_dir),
        resolver_factory=make_alias_resolver_factory(
            dep_dir,
            embedding_plan=embedding_plan,
        ),
        extractor=load_projection_extractor(dep_dir),
        extractor_source_subtypes=load_projection_extractor_source_subtypes(
            dep_dir,
        ),
        routes=load_projection_routes(dep_dir),
        materializers=discover_materializers(
            dsn=dsn,
            deployment_name=deployment_name,
            deployment_root=dep_dir,
        ),
        maintained_derivations=load_maintained_derivations(dep_dir),
        consistency_gates=[
            # chronos-native: the transform consistency gate (no_dangling + narrowing
            # over the Chronos graph_nodes/graph_edges) IS the Phase B integrity gate,
            # always on. The legacy edges_live narrowing trigger went away with the
            # projector path, so this is the sole narrowing/dangling enforcement at publish.
            # `dangling_policy` comes from deployment.yaml's consistency.dangling_edges
            # (default "block"); deployments that ingest history with intentional
            # dangling references (git-history affects->deleted-file) set "prune".
            TransformConsistencyGate(dangling_policy=load_dangling_policy(dep_dir)),
            *load_consistency_gates(dep_dir),
            # No stored node of a live type (#3666): its own check id, so the
            # transform gate keeps its incremental basis. One catalog query
            # when the catalog declares no live type.
            StoredLiveNodesGate(),
        ],
        post_publish_consistency_checks=[
            # transform-IR checks (the standard); the SQL-blob checks below are
            # transitional (consistency-checks-as-ir: freeze, then delete).
            *(IrPostPublishCheck(spec)
              for spec in load_ir_consistency_checks(dep_dir)),
            *load_post_publish_consistency_checks(dep_dir),
        ],
        lenses=load_lens_definitions(
            dep_dir,
            deployment_name=deployment_name,
        ),
        checkpoint_attestors=_load_checkpoint_attestors(
            dep_dir,
            deployment_name=deployment_name,
        ),
        enqueue_embeddings=embedding_plan.enabled,
        native_view_min_refresh_interval_ms=(
            native_view_policy["min_refresh_interval_ms"]
        ),
        native_view_materialization_delay_ms=(
            native_view_policy["materialization_delay_ms"]
        ),
        attribute_effective_view_empty_overlay_fast_path=bool(
            native_view_policy["attribute_effective_view_empty_overlay_fast_path"]
        ),
        history_retention_policy=(
            dict(history_retention)
            if isinstance(history_retention, dict)
            else None
        ),
        resolution_aware_reuse_enabled=load_resolution_aware_reuse_enabled(dep_dir),
    )


def _load_checkpoint_attestors(
    deployment_dir: Path,
    *,
    deployment_name: str,
) -> list[Any]:
    from okg.substrate.deployments import (
        DeploymentResolutionError,
        resolve_deployment,
    )
    from okg.substrate.publisher.checkpoint_attestations import (
        CheckpointAttestationConfigurationError,
        load_checkpoint_attestors,
    )

    # Mirror the canonical marker set in runtime.release_authority. Any one of
    # these means the process attempted sealed authority, so no partial state
    # may fall through to tracking-mode deployment.yaml.
    sealed_marker_names = (
        "OKG_ACTIVE_RELEASE_POINTER",
        "OKG_ENVIRONMENT_DESCRIPTOR",
        "OKG_ENVIRONMENT_ID",
        "OKG_RELEASE_ARTIFACT_ID",
        "OKG_RELEASE_INPUT_ID",
        "OKG_RELEASE_ROOT",
    )
    present_sealed_markers = tuple(
        name for name in sealed_marker_names if os.environ.get(name)
    )
    if present_sealed_markers:
        if not os.environ.get("OKG_ENVIRONMENT_DESCRIPTOR") or not os.environ.get(
            "OKG_RELEASE_ROOT"
        ):
            missing = tuple(
                name
                for name in ("OKG_ENVIRONMENT_DESCRIPTOR", "OKG_RELEASE_ROOT")
                if not os.environ.get(name)
            )
            raise DeploymentResolutionError(
                "sealed checkpoint attestor authority environment is incomplete: "
                f"present={present_sealed_markers} missing={missing}"
            )
        manifest = resolve_deployment(deployment_name)
        authority_dir = manifest.deployment_dir.resolve()
        requested_dir = Path(deployment_dir).resolve()
        if authority_dir != requested_dir:
            raise DeploymentResolutionError(
                "sealed checkpoint attestor definition root differs from the "
                f"publisher deployment root: requested={requested_dir} "
                f"active={authority_dir}"
            )
        attestor_root = (
            manifest.checkpoint_attestor_root or manifest.deployment_dir
        ).resolve()
        declarations = (
            manifest.effective_raw["checkpoint_attestors"]
            if "checkpoint_attestors" in manifest.effective_raw
            else []
        )
        return load_checkpoint_attestors(
            manifest.deployment_dir,
            deployment_name=deployment_name,
            declarations=declarations,
            implementation_root=attestor_root,
        )

    tracking_manifest = _lenient_manifest(deployment_dir)
    tracking_root = (
        tracking_manifest.checkpoint_attestor_root
        if tracking_manifest is not None
        else None
    )
    if (
        tracking_manifest is not None
        and "checkpoint_attestor_root" in tracking_manifest.effective_raw
        and tracking_root is None
    ):
        raise CheckpointAttestationConfigurationError(
            "checkpoint_attestor_root is unavailable"
        )
    return load_checkpoint_attestors(
        deployment_dir,
        deployment_name=deployment_name,
        implementation_root=tracking_root,
    )


def _lenient_manifest(deployment_dir: Path | str):
    """Parse a deployment's manifest into the typed model for a
    single-block read, or None when no manifest file exists.

    Reads through ``DeploymentManifest.from_yaml_lenient`` (the typed
    model, deployment-manifest-model PACT) rather than re-opening and
    re-parsing ``deployment.yaml`` here. Lenient because these block
    readers also run against partial fixtures (only ``name:`` + one
    config block); strict whole-manifest validation belongs on the
    deployment-RESOLVE path, not on a config-block read.
    """
    from okg.substrate.deployments import DeploymentManifest

    manifest_path = Path(deployment_dir) / "deployment.yaml"
    if not manifest_path.is_file():
        return None
    return DeploymentManifest.from_yaml_lenient(manifest_path)


def load_native_view_refresh_policy(deployment_dir: Path | str) -> dict[str, Any]:
    """Read optional native-view refresh coalescing policy."""

    manifest = Path(deployment_dir) / "deployment.yaml"
    policy: dict[str, Any] = {
        "min_refresh_interval_ms": 0,
        "materialization_delay_ms": 0,
        "attribute_effective_view_empty_overlay_fast_path": False,
    }
    if not manifest.is_file():
        return policy
    with manifest.open() as f:
        doc = fastyaml.safe_load(f) or {}
    block = doc.get("native_views") if isinstance(doc, dict) else None
    if not isinstance(block, dict):
        return policy
    for source_key, target_key in (
        ("min_refresh_interval_ms", "min_refresh_interval_ms"),
        ("materialization_delay_ms", "materialization_delay_ms"),
    ):
        value = block.get(source_key)
        if value is None:
            continue
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            continue
        policy[target_key] = max(0, parsed)
    value = block.get("attribute_effective_view_empty_overlay_fast_path")
    if value is not None:
        if isinstance(value, bool):
            policy["attribute_effective_view_empty_overlay_fast_path"] = value
        elif isinstance(value, str):
            policy["attribute_effective_view_empty_overlay_fast_path"] = (
                value in {"1", "true", "TRUE", "yes", "YES"}
            )
    return policy


def load_dangling_policy(deployment_dir: Path | str) -> str:
    """Read ``consistency.dangling_edges`` from the typed manifest model.

    ``"block"`` (default) — a dangling edge blocks the publish (strict integrity).
    ``"prune"`` — dangling edges are dropped at publish instead of blocking, for
    deployments that ingest history with intentional dangling references (e.g.
    okg-workspace's git-history ``affects``->deleted-file edges). Unknown/missing
    values fall back to ``"block"`` (fail safe — never silently loosen integrity)."""
    manifest = _lenient_manifest(deployment_dir)
    block = manifest.consistency if manifest is not None else None
    policy = block.get("dangling_edges") if isinstance(block, dict) else None
    return policy if policy in ("block", "prune") else "block"


def load_resolution_aware_reuse_enabled(deployment_dir: Path | str) -> bool:
    """Read the opt-in ``consistency.resolution_aware_reuse`` flag.

    ``True`` — the publish cycle runs the resolution-aware reuse pass (PACT
    consistency-resolution-aware-reuse, task.reuse.scheduler) as a DBOS step,
    recording a certificate behind every skip on the resolved state's claim-plane
    cone. Absent / anything but a true value → ``False`` (fail safe — a deployment
    opts in explicitly; the reuse pass never turns itself on). Only the
    ``consistency-fixture`` deployment opts in this wave, so every other
    deployment's cycle is unchanged."""
    manifest = _lenient_manifest(deployment_dir)
    block = manifest.consistency if manifest is not None else None
    value = block.get("resolution_aware_reuse") if isinstance(block, dict) else None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value in {"1", "true", "TRUE", "yes", "YES"}
    return False


def load_projection_extractor(deployment_dir: Path | str) -> Any | None:
    """Instantiate the typed manifest model's optional projection extractor."""

    manifest = _lenient_manifest(deployment_dir)
    block = manifest.extractor if manifest is not None else None
    if not isinstance(block, dict):
        return None
    cls_path = block.get("class")
    if not cls_path:
        return None
    config = block.get("config") or {}
    module_path, class_name = str(cls_path).rsplit(".", 1)
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)
    return cls(**config)


def load_projection_extractor_source_subtypes(
    deployment_dir: Path | str,
) -> list[str] | None:
    """Read optional subtype scoping for projection-time extraction."""

    manifest = _lenient_manifest(deployment_dir)
    block = manifest.extractor if manifest is not None else None
    if not isinstance(block, dict):
        return None
    raw = block.get("source_subtypes")
    if raw is None:
        return None
    if not isinstance(raw, list):
        raise ValueError("extractor.source_subtypes must be a list")
    return [str(item).strip() for item in raw if str(item).strip()]


def load_derived_producer_policies(
    deployment_dir: Path | str,
) -> dict[str, dict[str, Any]]:
    """Load deployment-declared derived producer policies.

    Inline enricher ``authority:`` blocks are attached by
    :func:`load_enrichers`. This helper covers derived producers that
    do not map one-to-one with an instantiated class, especially
    extractor source families such as ``_extractor:change_slug``.
    """

    manifest = Path(deployment_dir) / "deployment.yaml"
    if not manifest.is_file():
        return {}
    with manifest.open() as f:
        doc = fastyaml.safe_load(f) or {}
    if not isinstance(doc, dict):
        return {}

    policies: dict[str, dict[str, Any]] = {}
    for kind, entries in _derived_producer_sections(doc):
        for producer_id, authority in _iter_derived_policy_entries(
            entries,
            producer_kind=kind,
        ):
            policy = dict(authority)
            policy.setdefault("producer_id", producer_id)
            policy.setdefault("producer_kind", kind)
            policy.setdefault("source_ref", {
                "deployment_manifest": str(manifest),
                "producer_id": producer_id,
            })
            policies[producer_id] = policy
    return policies


def _derived_producer_sections(
    manifest: dict[str, Any],
) -> list[tuple[str, Any]]:
    sections: list[tuple[str, Any]] = []
    block = manifest.get("derived_producers") or {}
    if isinstance(block, dict):
        sections.append(("extractor", block.get("extractors")))
        sections.append(("enricher", block.get("enrichers")))

    extractor_block = manifest.get("extractor")
    if isinstance(extractor_block, dict):
        authority = extractor_block.get("authority")
        if isinstance(authority, dict):
            sections.append(("extractor", authority.get("producers")))
    return sections


def _iter_derived_policy_entries(
    entries: Any,
    *,
    producer_kind: str,
) -> list[tuple[str, dict[str, Any]]]:
    if not entries:
        return []
    out: list[tuple[str, dict[str, Any]]] = []
    if isinstance(entries, dict):
        for raw_id, authority in entries.items():
            if not isinstance(authority, dict):
                continue
            producer_id = _normalize_derived_producer_id(
                str(raw_id),
                producer_kind=producer_kind,
            )
            out.append((producer_id, dict(authority)))
        return out
    if isinstance(entries, list):
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            raw_id = entry.get("producer_id") or entry.get("id")
            if not raw_id:
                continue
            producer_id = _normalize_derived_producer_id(
                str(raw_id),
                producer_kind=producer_kind,
            )
            out.append((producer_id, dict(entry)))
    return out


def _normalize_derived_producer_id(
    producer_id: str,
    *,
    producer_kind: str,
) -> str:
    if producer_id.startswith("_"):
        return producer_id
    if producer_kind == "extractor":
        return f"_extractor:{producer_id}"
    if producer_kind == "enricher":
        return f"_enricher:{producer_id}"
    return producer_id


def load_projection_routes(deployment_dir: Path | str) -> list[Any] | None:
    """Load optional deployment linker routes for projection extraction."""

    path = Path(deployment_dir) / "linker_config.yaml"
    if not path.is_file():
        return None
    from okg.substrate.catalog.linker_config import load_linker_config

    return load_linker_config(path)


def embeddings_enabled(deployment_dir: Path | str) -> bool:
    """Read the one admitted typed embedding contract."""

    manifest = _lenient_manifest(deployment_dir)
    if manifest is None:
        return False
    return manifest.embedding_config.enabled


def load_maintained_derivations(deployment_dir: Path | str) -> Any | None:
    """Load an optional deployment-owned maintained-derivation registry.

    Canonical manifest shape:

    ``maintained_derivations.registry: module:factory_or_registry``

    The reference is deployment-relative because ``inject_deployment_path`` has
    already placed the deployment root on ``sys.path`` before this function runs.
    A callable reference is invoked with no arguments and must return either a
    ``DerivationRegistry`` or an iterable of ``DerivationRow`` objects.
    """

    manifest = Path(deployment_dir) / "deployment.yaml"
    if not manifest.is_file():
        return None
    with manifest.open() as f:
        doc = fastyaml.safe_load(f) or {}
    block = doc.get("maintained_derivations") if isinstance(doc, dict) else None
    if not block:
        return None
    if isinstance(block, str):
        ref = block
    elif isinstance(block, dict):
        ref = block.get("registry") or block.get("loader")
    else:
        raise ValueError("deployment.yaml maintained_derivations must be a mapping")
    if not ref:
        raise ValueError(
            "deployment.yaml maintained_derivations must declare registry: module:attr"
        )
    registry = _resolve_deployment_attr(str(ref), context="maintained_derivations")
    registry = registry() if callable(registry) else registry
    if registry is None:
        return None
    return _validated_maintained_derivation_registry(registry)


def _validated_maintained_derivation_registry(registry: Any) -> Any:
    from okg.substrate.derived_artifacts.derivation_registry import (
        DerivationRegistry,
        DerivationRow,
    )

    if isinstance(registry, DerivationRegistry):
        return registry
    rows = registry.rows() if hasattr(registry, "rows") else registry
    try:
        rows = list(rows)
    except TypeError as exc:
        raise ValueError(
            "maintained_derivations registry must expose rows() or be iterable"
        ) from exc
    validated = DerivationRegistry()
    for index, row in enumerate(rows):
        if not isinstance(row, DerivationRow):
            raise ValueError(
                "maintained_derivations iterable entries must be DerivationRow "
                f"instances; entry {index} is {type(row).__name__}"
            )
        validated.register(row)
    return validated


def _resolve_deployment_attr(ref: str, *, context: str) -> Any:
    module_path, _, attr = ref.partition(":")
    if not module_path or not attr:
        raise ValueError(f"{context} import ref must be 'module:attribute', got {ref!r}")
    importlib.invalidate_caches()
    sys.modules.pop(module_path, None)
    module = importlib.import_module(module_path)
    try:
        return getattr(module, attr)
    except AttributeError as exc:
        raise ValueError(
            f"{context} import ref {ref!r} failed: module has no attribute {attr!r}"
        ) from exc


def discover_materializers(
    *,
    dsn: str,
    deployment_name: str,
    deployment_root: Path,
) -> list[Any]:
    """Discover native view materializers registered for deployment."""

    # Materializer discovery is registry-authority now; the worker DSN is
    # passed only to explicitly tracking-enabled model clients.
    from okg.substrate.views.registry import (
        load_views_registry,
        materializers_from_registry_entries,
    )

    try:
        registry_path = deployment_root / "views_registry.yaml"
        if not registry_path.exists():
            return []
        entries = load_views_registry(deployment_root)
        if not entries:
            return []
        return materializers_from_registry_entries(
            entries,
            deployment_name=deployment_name,
            deployment_root=deployment_root,
            runtime_dsn=dsn,
        )
    except psycopg.OperationalError:
        return []


def load_lens_definitions(
    deployment_dir: Path | str,
    *,
    deployment_name: str,
) -> list[Any]:
    """Load optional deployment-declared editable lens definitions."""

    manifest = Path(deployment_dir) / "deployment.yaml"
    if not manifest.is_file():
        return []
    with manifest.open() as f:
        doc = fastyaml.safe_load(f) or {}
    blocks = doc.get("lenses") or []
    if not isinstance(blocks, list):
        raise ValueError("deployment.yaml lenses must be a list")

    from okg.substrate.derived_artifacts import stable_hash
    from okg.substrate.lenses import LensDefinition

    definitions: list[LensDefinition] = []
    for index, raw_block in enumerate(blocks):
        if not isinstance(raw_block, dict):
            raise ValueError(f"lens entry {index} must be a mapping")
        block = dict(raw_block)
        lens_id = _required_text(block, "lens_id", index)
        view_id = _required_text(block, "view_id", index)
        view_definition_id = str(
            block.get("view_definition_id") or f"view:{view_id}"
        )
        definition_id = str(block.get("definition_id") or f"lens:{lens_id}")
        editable_slots = block.get("editable_slots")
        lens_contract = block.get("lens_contract")
        if lens_contract is None:
            if not isinstance(editable_slots, dict) or not editable_slots:
                raise ValueError(
                    f"lens entry {index} must declare non-empty "
                    "editable_slots or lens_contract"
                )
            lens_contract = {"editable_slots": editable_slots}
        if not isinstance(lens_contract, dict):
            raise ValueError(f"lens entry {index} lens_contract must be a map")
        _validate_lens_contract(lens_contract, index)

        putback_policy = block.get("putback_policy") or {
            "mode": "overlay",
            "validator": "expected_value_compare_v1",
        }
        if not isinstance(putback_policy, dict):
            raise ValueError(
                f"lens entry {index} putback_policy must be a map"
            )
        if putback_policy.get("mode") != "overlay":
            raise ValueError(
                f"lens entry {index} only overlay putback is supported"
            )

        source_authority_policy = block.get("source_authority_policy")
        if not isinstance(source_authority_policy, dict):
            raise ValueError(
                f"lens entry {index} must declare source_authority_policy"
            )
        allowed = source_authority_policy.get("allowed_authorities")
        if (
            not isinstance(allowed, list)
            or not allowed
            or any(not isinstance(item, str) or not item for item in allowed)
        ):
            raise ValueError(
                f"lens entry {index} source_authority_policy must declare "
                "allowed_authorities"
            )
        commit_modes = source_authority_policy.get("commit_modes") or [
            "overlay",
        ]
        if not isinstance(commit_modes, list) or commit_modes != ["overlay"]:
            raise ValueError(
                f"lens entry {index} only overlay commit_modes are supported"
            )
        source_authority_policy = {
            **source_authority_policy,
            "commit_modes": ["overlay"],
        }

        renderer_ref = str(
            block.get("renderer_ref") or "okg.renderer.lens.overlay.v1"
        )
        definitions.append(
            LensDefinition(
                definition_id=definition_id,
                deployment_name=deployment_name,
                lens_id=lens_id,
                view_definition_id=view_definition_id,
                view_id=view_id,
                compute_hash=stable_hash(
                    {
                        "kind": "deployment_lens_v1",
                        "lens_id": lens_id,
                        "view_definition_id": view_definition_id,
                        "view_id": view_id,
                    }
                ),
                output_schema_hash=stable_hash(lens_contract),
                renderer_hash=stable_hash(
                    {
                        "renderer_ref": renderer_ref,
                        "renderer_config": block.get("renderer_config") or {},
                    }
                ),
                dirty_rule_hash=stable_hash(
                    {
                        "putback_policy": putback_policy,
                        "source_authority_policy": source_authority_policy,
                    }
                ),
                renderer_ref=renderer_ref,
                lens_contract=lens_contract,
                putback_policy=putback_policy,
                source_authority_policy=source_authority_policy,
            )
        )
    return definitions


def _required_text(block: dict[str, Any], key: str, index: int) -> str:
    value = block.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"lens entry {index} must declare {key}")
    return value


def _validate_lens_contract(contract: dict[str, Any], index: int) -> None:
    editable_slots = contract.get("editable_slots")
    if not isinstance(editable_slots, dict) or not editable_slots:
        raise ValueError(
            f"lens entry {index} lens_contract must declare editable_slots"
        )
    for slot_name, raw_slot in editable_slots.items():
        if not isinstance(slot_name, str) or not slot_name:
            raise ValueError(f"lens entry {index} slot names must be text")
        if not isinstance(raw_slot, dict):
            raise ValueError(
                f"lens entry {index} editable slot {slot_name!r} "
                "must be a map"
            )
        if raw_slot.get("row_identity") != "row_key":
            raise ValueError(
                f"lens entry {index} editable slot {slot_name!r} must use "
                "row_identity: row_key"
            )
        editable_fields = raw_slot.get("editable_fields")
        if not isinstance(editable_fields, list) or not editable_fields:
            raise ValueError(
                f"lens entry {index} editable slot {slot_name!r} must "
                "declare editable_fields"
            )
        if any(
            not isinstance(field, str) or not field
            for field in editable_fields
        ):
            raise ValueError(
                f"lens entry {index} editable_fields must be non-empty text"
            )
