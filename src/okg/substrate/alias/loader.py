"""Read deployment.yaml's `alias_resolver:` block.

Mirrors the shape of `enricher_loader.py`. Missing block returns the substrate
default (`CascadeAliasResolver([SqlCascadeBackend(conn)])`). If the admitted
deployment contract enables embeddings and the caller supplies a verified
execution plan, the default cascade also includes `EmbeddingAliasBackend`.

Per `redesign-alias-resolution` (2026-04-26).
"""
from __future__ import annotations

import importlib
import inspect
from pathlib import Path
from typing import Any, Optional

import psycopg

from okg.substrate.alias.cascade import CascadeAliasResolver
from okg.substrate.alias.protocol import AliasBackend
from okg.substrate.alias.backends.sql_cascade import SqlCascadeBackend
from okg.substrate.errors import SubstrateError


__all__ = [
    "AliasBackendLoadError",
    "load_alias_resolver",
    "make_alias_resolver_factory",
]


class AliasBackendLoadError(SubstrateError):
    code = "alias_backend_load_error"


def _resolve_class(class_path: str):
    try:
        module_path, class_name = class_path.rsplit(".", 1)
    except ValueError as e:
        raise AliasBackendLoadError(
            f"class path {class_path!r} is not in 'pkg.mod.Class' "
            f"form",
            data={"class": class_path},
        ) from e
    try:
        module = importlib.import_module(module_path)
    except ImportError as e:
        raise AliasBackendLoadError(
            f"could not import module {module_path!r}",
            data={"class": class_path, "import_error": str(e)},
            fix_hint=f"check that {module_path} is on the Python path",
        ) from e
    try:
        return getattr(module, class_name)
    except AttributeError as e:
        raise AliasBackendLoadError(
            f"module {module_path!r} has no attribute {class_name!r}",
            data={"class": class_path},
        ) from e


def _instantiate_backend(
    class_path: str,
    config: Optional[dict[str, Any]],
    conn: Optional[psycopg.Connection],
    embedder: Optional[Any] = None,
) -> AliasBackend:
    cls = _resolve_class(class_path)
    cfg = dict(config or {})
    sig = inspect.signature(cls)
    kwargs: dict[str, Any] = dict(cfg)
    if "conn" in sig.parameters and "conn" not in kwargs:
        if conn is None:
            raise AliasBackendLoadError(
                f"backend {class_path!r} needs a Postgres "
                f"connection but none was provided",
                data={"class": class_path, "config_keys": list(cfg)},
                fix_hint="pass conn= to load_alias_resolver",
            )
        kwargs["conn"] = conn
    if "embedder" in sig.parameters and "embedder" not in kwargs:
        if embedder is None:
            raise AliasBackendLoadError(
                f"backend {class_path!r} needs an embedder but "
                f"none was provided",
                data={"class": class_path, "config_keys": list(cfg)},
                fix_hint=(
                    "pass embedder= to load_alias_resolver or drop "
                    "this backend from alias_resolver.backends"
                ),
            )
        if not getattr(embedder, "node_column", True):
            raise AliasBackendLoadError(
                f"backend {class_path!r} needs an embedder for the 2560-wide "
                "node column, but the deployment's search vector set has "
                "its own width",
                data={"class": class_path,
                      "dimension": getattr(embedder, "observed_dimension", None)},
                fix_hint=(
                    "drop this backend from alias_resolver.backends, or make "
                    "the search set's width 2560"
                ),
            )
        kwargs["embedder"] = embedder
    try:
        return cls(**kwargs)
    except TypeError as e:
        raise AliasBackendLoadError(
            f"backend {class_path!r} could not be instantiated "
            f"with config keys {list(cfg.keys())}",
            data={
                "class": class_path,
                "config_keys": list(cfg.keys()),
                "error": str(e),
            },
            fix_hint="check the constructor signature",
        ) from e
    except SubstrateError as e:
        raise AliasBackendLoadError(
            f"backend {class_path!r} refused to instantiate: "
            f"{type(e).__name__}: {e}",
            data={
                "class": class_path,
                "config_keys": list(cfg.keys()),
                "underlying_code": e.code,
            },
            fix_hint=e.fix_hint or (
                f"see {type(e).__name__} for backend-specific guidance"
            ),
        ) from e


def load_alias_resolver(
    deployment_dir: Path | str,
    *,
    conn: Optional[psycopg.Connection] = None,
    embedding_plan: Optional[Any] = None,
) -> CascadeAliasResolver:
    """Build a CascadeAliasResolver from
    `<deployment_dir>/deployment.yaml`'s `alias_resolver:` block.

    Missing block (or missing manifest) returns the substrate
    default: `CascadeAliasResolver([SqlCascadeBackend(conn)])`.
    """
    deployment_dir = Path(deployment_dir)
    manifest_path = deployment_dir / "deployment.yaml"
    block: Optional[dict] = None
    from okg.substrate.embeddings import EmbeddingConfig

    manifest_embedding_config = EmbeddingConfig()
    if manifest_path.is_file():
        from okg.substrate.deployments import DeploymentManifest

        manifest = DeploymentManifest.from_yaml_lenient(manifest_path)
        block = manifest.effective_raw.get("alias_resolver")
        manifest_embedding_config = manifest.embedding_config

    if embedding_plan is not None:
        if embedding_plan.config != manifest_embedding_config:
            raise AliasBackendLoadError(
                "embedding execution plan disagrees with the admitted "
                "deployment embedding configuration",
                data={
                    "manifest_config": repr(manifest_embedding_config),
                    "plan_config": repr(embedding_plan.config),
                },
            )
        # Pass the verified plan itself. It implements the Embedder protocol
        # while retaining the capability fingerprint and rechecking every
        # emitted vector against the preflighted width.
        embedder = embedding_plan
    else:
        embedder = None
    embeddings_enabled = manifest_embedding_config.enabled
    if embeddings_enabled and embedding_plan is None:
        raise AliasBackendLoadError(
            "deployment enables embeddings but alias resolution received no "
            "verified embedding execution plan",
            data={"manifest_enabled": True, "plan_present": False},
            fix_hint=(
                "build the deployment publisher stack so backend capability "
                "is verified before alias resolver construction"
            ),
        )

    if not block:
        if conn is None:
            raise AliasBackendLoadError(
                "default alias resolver requires a Postgres "
                "connection (SqlCascadeBackend); none was provided",
                data={"path": str(manifest_path)},
                fix_hint="pass conn= or declare alias_resolver:",
            )
        # Per `auto-default-alias-resolver` (2026-04-26): when
        # an embedder is supplied and the deployment has embeddings
        # enabled, the default cascade includes the embedding backend
        # after SQL. Without an embedder, or when embeddings are
        # disabled, falls back to SQL-only.
        backends: list[AliasBackend] = [SqlCascadeBackend(conn)]
        # A paid backend's plan carries no backend (`can_embed` False): alias
        # resolution during a publish would make uncapped, unrecorded paid
        # calls, so the default cascade stays SQL-only for it.
        # A plan at a vector set's own width (PACT vector-set-pgvector-widths)
        # cannot be compared with the 2560-wide node column the embedding
        # backend reads, so the cascade stays SQL-only for it too.
        if (
            embedder is not None
            and embeddings_enabled
            and getattr(embedder, "can_embed", True)
            and getattr(embedder, "node_column", True)
        ):
            from okg.substrate.alias.backends import (
                EmbeddingAliasBackend,
            )
            backends.append(
                EmbeddingAliasBackend(conn, embedder=embedder),
            )
        return CascadeAliasResolver(backends, on_empty="drop")

    on_empty = block.get("on_empty", "drop")
    specs = block.get("backends") or []
    if not isinstance(specs, list) or not specs:
        raise AliasBackendLoadError(
            f"{manifest_path}: alias_resolver.backends must be "
            f"a non-empty list",
            data={"path": str(manifest_path)},
        )
    backends: list[AliasBackend] = []
    for spec in specs:
        if not isinstance(spec, dict):
            raise AliasBackendLoadError(
                f"{manifest_path}: alias_resolver entry must be "
                f"a mapping with `class:`",
                data={"path": str(manifest_path), "entry": spec},
            )
        cls_path = spec.get("class")
        if not cls_path:
            raise AliasBackendLoadError(
                f"{manifest_path}: alias_resolver entry needs a "
                f"`class:` key",
                data={"path": str(manifest_path), "entry": spec},
            )
        backends.append(_instantiate_backend(
            cls_path, spec.get("config"), conn, embedder,
        ))
    return CascadeAliasResolver(backends, on_empty=on_empty)


def make_alias_resolver_factory(
    deployment_dir: Path | str,
    *,
    embedding_plan: Optional[Any] = None,
):
    """Return a `lambda conn: CascadeAliasResolver(...)` factory
    that the projector calls per batch worker.

    The deployment.yaml is parsed once at construction time;
    the closure builds fresh backend instances per conn so each
    worker's SQL backend uses its own connection.
    """
    deployment_dir = Path(deployment_dir)

    def factory(conn: psycopg.Connection) -> CascadeAliasResolver:
        return load_alias_resolver(
            deployment_dir, conn=conn, embedding_plan=embedding_plan,
        )
    return factory
