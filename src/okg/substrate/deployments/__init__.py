"""Canonical deployment loader for the substrate.

Single source of truth for deployment discovery, manifest parse,
and `sys.path` injection. Every entry point that needs to locate
or load a deployment — substrate CLI verbs, MCP server boot,
legacy ingest CLI — uses the helpers here. Per
`unify-deployment-loader` (2026-04-27).

Discovery cascade (`find_deployments_root`):
    1. Explicit override (caller-supplied path)
    2. ``OKG_DEPLOYMENTS_DIR`` environment variable
    3. ``./deployments/`` adjacent to a ``pyproject.toml`` /
       ``.git`` sentinel found by walking up from cwd
    4. ``deployments/`` derived from this module's path
       (in-tree fallback for development inside the okg repo)

Steps 1 and 2, when set but not pointing at an existing
directory, raise :class:`DeploymentResolutionError` rather than
silently falling through. Per the substrate's "no silent
fallbacks" discipline (`history/openspec/project.md`).

String-valued fields inside a manifest may contain
``${manifest_dir}``, which expands to the absolute path of the
directory containing the manifest. Unrecognised ``${...}`` tokens
fail loudly via :class:`DeploymentConfigError`.
"""
from __future__ import annotations

import contextvars
import importlib
import math
import os
import re
import stat
import sys
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
import unicodedata
from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING, Any, Optional

from okg.substrate import fastyaml
from okg.substrate.deployments.sync_root import (
    env_and_profile_values,
    manifest_path_anchors,
    resolve_declared_sync_root,
)

if TYPE_CHECKING:
    from ..embeddings import EmbeddingConfig
    from ..runtime import RuntimeConfig


__all__ = [
    "DeploymentConfigError",
    "DeploymentManifest",
    "DeploymentResolutionError",
    "FirstPublishWarningBudget",
    "PublisherConfig",
    "bind_resolved_deployment",
    "check_manifest_keys",
    "deployment_binding_scope",
    "deployment_loads_module",
    "deployment_name_lookup",
    "evict_deployment_modules",
    "find_deployment_dir",
    "find_deployments_root",
    "inject_deployment_path",
    "require_postgres_dsn",
    "resolve_deployment",
    "resolve_source_registry_path",
    "user_state_chat_instances_root",
]


_DEPLOYMENT_ALIASES: dict[str, str] = {}


def _deployment_name_candidates(name: str) -> tuple[str, ...]:
    alias_target = _DEPLOYMENT_ALIASES.get(name)
    if alias_target is None:
        return (name,)
    return (name, alias_target)


def deployment_loads_module(
    deployment_name: str, module_name: str,
) -> bool:
    """Return True when ``deployment_name``'s manifest's
    ``modules:`` list contains ``module_name``.

    Used as a gate by module-conditional MCP tools (per Layer 3B
    of ``add-portable-memex-deployments``: the
    ``generate_context_packet`` tool registers iff the deployment
    loads the ``reasoning`` module). Returns ``False`` when the
    deployment dir is not resolvable so missing deployments
    don't accidentally expose conditional tools. Returns
    ``False`` on malformed manifests too — the gate stays safe
    and silent; manifest bugs surface via the load path.

    Lives in the deployments package (rather than the mcp
    package) so non-mcp callers + tests can use it without
    importing the mcp SDK.
    """
    active_environment = _active_release_environment(deployment_name)
    if active_environment is not None:
        return module_name in tuple(active_environment.manifest.modules)
    try:
        deployment_dir = find_deployment_dir(deployment_name)
    except DeploymentResolutionError:
        return False
    if deployment_dir is None:
        return False
    manifest_path = deployment_dir / "deployment.yaml"
    if not manifest_path.is_file():
        return False
    try:
        manifest = DeploymentManifest.from_yaml(manifest_path)
    except Exception:  # noqa: BLE001 — malformed deployment.yaml: treat as "module not enabled" so caller doesn't crash on a doctor probe.
        return False
    return module_name in tuple(manifest.modules)


def _runtime_config_from_manifest(raw: dict | None):
    """Parse the manifest's ``runtime:`` block into a RuntimeConfig.

    Imported lazily to keep the substrate-deployments package
    importable from the runtime package itself if needed (the runtime
    module imports nothing from this loader; only this loader imports
    runtime, and only inside this function).
    """
    from ..runtime import RuntimeConfig
    return RuntimeConfig.from_manifest_block(raw)


def _embedding_config_from_manifest(
    raw: dict[str, Any], *, manifest_path: Path,
):
    """Parse both accepted spellings through the one closed contract."""
    from ..embeddings import EmbeddingConfigError, parse_embedding_config

    values: dict[str, object] = {}
    if "embedding" in raw:
        values["singular"] = raw["embedding"]
    if "embeddings" in raw:
        values["plural"] = raw["embeddings"]
    try:
        return parse_embedding_config(**values)
    except EmbeddingConfigError as exc:
        raise DeploymentConfigError(f"{exc} (Manifest: {manifest_path})") from exc


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class DeploymentConfigError(Exception):
    """Raised when a deployment manifest contains an unresolvable
    ``${...}`` placeholder or other malformed templated value."""


_YAML_TYPE_WORDS = {
    "null": "null",
    "boolean": "a boolean",
    "integer": "an integer",
    "number": "a number",
    "string": "a string",
    "array": "a list",
    "object": "a mapping",
    "date": "a date",
    "timestamp": "a timestamp",
}


class DeploymentKeyTypeError(DeploymentConfigError):
    """A top-level ``deployment.yaml`` value has a type its key does not allow.

    The allowed types come from ``okg.substrate.deployments.keys``.
    """

    def __init__(
        self,
        key: str,
        *,
        allowed: tuple[str, ...],
        found: str,
        manifest_path: Path,
    ) -> None:
        self.key = key
        self.allowed = allowed
        self.found = found
        self.manifest_path = manifest_path
        hint = (
            "; quote the value if you meant a string"
            if found in {"date", "timestamp"} else ""
        )
        # Say it in YAML's words; null goes last ("a mapping or null").
        wanted = [_YAML_TYPE_WORDS.get(t, t) for t in allowed if t != "null"]
        if "null" in allowed:
            wanted.append("null")
        super().__init__(
            f"deployment.yaml key {key} must be {' or '.join(wanted)}, "
            f"not {_YAML_TYPE_WORDS.get(found, found)}{hint} "
            f"(Manifest: {manifest_path})"
        )

    def __reduce__(self):
        # Workflow runtimes store errors by pickling; the keyword-only
        # constructor needs its arguments passed back explicitly.
        return (
            _rebuild_key_type_error,
            (self.key, self.allowed, self.found, self.manifest_path),
        )


class DeploymentManifestNotMappingError(DeploymentConfigError, ValueError):
    """The manifest's top level is a list or a scalar, not a mapping.

    Also a ``ValueError``: before #2713 the strict loader reported this file
    as ``missing required key: 'schema_dir'`` (a ``ValueError``), so a caller
    that caught that still catches this.
    """


def _reject_non_mapping_manifest(raw: Any, manifest_path: Path) -> None:
    """Refuse a manifest whose top level is a list or scalar (#2713).

    An empty file (``None``) is left to the callers: the lenient reader
    treats it as a partial manifest, and the strict loader names the first
    missing required key.
    """
    if raw is None or isinstance(raw, dict):
        return
    found = {list: "a list", str: "a string", bool: "a boolean",
             int: "an integer", float: "a number"}.get(
        type(raw), type(raw).__name__
    )
    raise DeploymentManifestNotMappingError(
        f"deployment.yaml top level must be a mapping of keys, not {found} "
        f"(Manifest: {manifest_path})"
    )


def _rebuild_key_type_error(
    key: str, allowed: tuple[str, ...], found: str, manifest_path: Path,
) -> "DeploymentKeyTypeError":
    return DeploymentKeyTypeError(
        key, allowed=allowed, found=found, manifest_path=manifest_path,
    )


class DeploymentResolutionError(Exception):
    """Raised when :func:`resolve_deployment` cannot locate exactly
    one deployment manifest, or when an explicit override / env var
    points at a non-existent directory.

    Library code (MCP server, audit framework) should catch this
    and degrade gracefully; CLI entry points should let it
    propagate to ``main()`` which converts it to a nonzero exit.
    """


def resolve_source_registry_path(
    deployment_dir: Path | str,
    manifest: dict[str, Any] | None = None,
) -> Path:
    """Resolve a deployment's declared registry, including thin overlays."""
    root = Path(deployment_dir)
    if manifest is None:
        manifest_path = root / "deployment.yaml"
        if not manifest_path.is_file():
            return root / "source_registry.yaml"
        raw = fastyaml.safe_load(manifest_path.read_text(encoding="utf-8")) or {}
    else:
        raw = manifest
    value = str(raw.get("source_registry") or "source_registry.yaml")
    path = Path(value)
    return path if path.is_absolute() else root / path


def _active_release_environment(name: str | None):
    """Resolve only the launcher-pinned descriptor in sealed mode."""
    descriptor = os.environ.get("OKG_ENVIRONMENT_DESCRIPTOR")
    release_root = os.environ.get("OKG_RELEASE_ROOT")
    if not descriptor and not release_root:
        return None
    if not descriptor or not release_root:
        raise DeploymentResolutionError(
            "sealed deployment resolution environment is incomplete"
        )
    try:
        from .environment import load_environment

        environment = load_environment(descriptor, environ=os.environ)
    except Exception as exc:  # noqa: BLE001 - normalize active descriptor errors.
        raise DeploymentResolutionError(
            f"active sealed environment descriptor is invalid: {exc}"
        ) from exc
    requested = name or os.environ.get("OKG_DEPLOYMENT")
    if requested not in {environment.id, environment.graph_deployment_name}:
        raise DeploymentResolutionError(
            "sealed process cannot resolve a deployment outside its active "
            f"environment: requested={requested!r} "
            f"active={environment.graph_deployment_name!r}"
        )
    return environment


def require_postgres_dsn(manifest, *, deployment: str | None = None) -> str:
    """Return the manifest's resolved postgres_dsn, raising
    ``DeploymentResolutionError`` if it is None.

    Per the `fix-substrate-silent-failures` proposal, manifest config
    resolution refuses to return values containing unsubstituted
    `${VAR}` placeholders (see `_resolve_postgres_dsn`). The result
    is that `manifest.postgres_dsn` is None when the manifest
    referenced an env var that wasn't set. This helper turns that
    None into a structured, debuggable error at the workflow boundary
    instead of letting a literal None propagate to psycopg.

    Usage:

        from okg.substrate.deployments import require_postgres_dsn
        dsn = require_postgres_dsn(manifest, deployment="my-deploy")

    The ``deployment`` argument is optional; if provided it's included
    in the error message for operator context.
    """
    dsn = getattr(manifest, "postgres_dsn", None)
    if dsn:
        # The manifest resolved to a value, but a libpq keyword/value
        # string is not one this system can use: Chronos picks its backend
        # by URL scheme, so a scheme-less DSN reaches the publish path and
        # writes a FILE named after the connection string (issue #1971).
        # Refuse it here, at the workflow boundary, naming what the
        # operator edited -- the alternative is the same refusal much
        # later, phrased in Chronos' terms (issue #2001).
        from okg.substrate.dsn_form import (
            is_keyword_form_dsn,
            keyword_form_refusal,
        )

        if is_keyword_form_dsn(dsn):
            dep = f" of deployment {deployment!r}" if deployment else ""
            raise DeploymentResolutionError(
                keyword_form_refusal(
                    f"the manifest's `postgres.dsn`{dep} "
                    "(or the env var interpolated into it)",
                    dsn,
                )
            )
        return dsn
    dep_phrase = f" for deployment {deployment!r}" if deployment else ""
    raise DeploymentResolutionError(
        f"manifest's postgres.dsn is unresolvable{dep_phrase}. "
        "Most likely cause: the manifest's `postgres.dsn` field "
        "references an env var (e.g. `${OKG_DSN}`) that is not set "
        "in this process's environment. `_resolve_postgres_dsn` "
        "refuses to return literal placeholder strings (since "
        "2026-05-11), so the resolver returned None. Set the env "
        "var, or pass `--dsn` to the worker which auto-exports "
        "OKG_DSN for in-workflow re-resolution."
    )


def check_manifest_keys(manifest_path: Path) -> None:
    """Run the loader's key refusals on a manifest, nothing more.

    For a reader that needs the manifest's raw text (``okg deployment env``
    reads ``postgres.dsn`` before any ``${VAR}`` is expanded) but must not
    skip a refusal the loader would raise. It refuses a list or scalar top
    level and an unknown top-level key, as the strict loader does, and then
    builds the lenient model, so
    every check that runs in both modes (wrong top-level types, and the
    block-key checks) runs here too. It does not require ``schema_dir`` or
    other required keys and does not check that declared paths exist, so a
    partial manifest passes. Raises :class:`DeploymentConfigError`.
    """
    manifest_path = Path(manifest_path)
    with open(manifest_path) as f:
        raw = fastyaml.safe_load(f)
    _reject_non_mapping_manifest(raw, manifest_path)
    deployment_dir = manifest_path.parent
    raw = _substitute_manifest_placeholders(raw, deployment_dir)
    _reject_unknown_top_level_keys(raw, manifest_path)
    DeploymentManifest._build_from_raw(
        raw, deployment_dir, manifest_path, strict=False,
    )


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------


# Module-level cache for `DeploymentManifest.from_yaml`. Key:
# `(absolute manifest path, mtime_ns)`. Value: `(manifest,
# env_signature)`, where `env_signature` (see `_env_signature`) records
# the values of the env vars the manifest interpolates via `${VAR}` at
# the time the entry was built. The frozen dataclass result is safe to
# share across callers. Race-window note matches `_REGISTRY_CACHE` in
# sources/registry.py: two concurrent first-time callers may both build
# + write; the file content drives both, so the result is
# correctness-equivalent, just wastes one parse.
#
# Env-awareness: env-var interpolation (e.g. `${OKG_DSN}` in
# postgres.dsn) runs inside `_parse_with_env_deps` and its result is
# cached. Because the cached value carries the env signature it was built
# against, a resolve under a CHANGED environment (a test setting
# `DSG_OKG_DSN` after a sibling resolved the same file with it unset)
# rebuilds instead of serving a manifest whose `postgres_dsn` is stale.
# A worker with a stable environment stays a `(path, mtime)` hit — the
# env check is a handful of `os.environ.get` reads, not a re-parse.
_MANIFEST_CACHE: dict[
    tuple[str, int],
    tuple["DeploymentManifest", tuple[tuple[str, str | None], ...]],
] = {}


# Known top-level deployment.yaml keys. Censused across every available
# manifest (the 6 in-repo deployments AND the cached external ones under
# ``~/.cache/okg/deployments/*/*/deployment.yaml``) per the
# deployment-manifest-model PACT, so a legitimate external key is never
# falsely rejected. A key absent here in a loaded manifest is a typo /
# unknown block and fails loudly via DeploymentConfigError rather than
# being silently dropped. Add a key here (with a captured field or an
# explicit "raw pass-through, owned elsewhere" note) when a new block is
# introduced.
def resolve_declared_module_providers(
    raw: object,
    deployment_dir: Path,
    *,
    manifest_path: Path,
    strict: bool = True,
) -> dict[str, Path]:
    """Resolve `module_providers:` to owner -> contained directory.

    Every value must stay inside the deployment directory. Nothing else in
    this loader checks containment, so this key checks its own, where it is
    read: an absolute value would silently replace the base, `..` would climb
    out of it, and a symlinked component could point anywhere. A packaged
    install binds these files into its release identity, so a path that
    escapes the tree escapes the digest too.
    """
    from okg.substrate.catalog.module_providers import (
        BUILTIN_OWNER, _valid_segment, require_nfc_identity,
    )

    if not isinstance(raw, dict) or "module_providers" not in raw:
        return {}
    # An environment definition carries this key too, so every refusal names
    # the file the reader is looking at rather than always `deployment.yaml`.
    label = manifest_path.name
    raw_value = raw["module_providers"]
    if not isinstance(raw_value, dict) or not raw_value:
        # `null` and `{}` are refused alongside a non-mapping: a key that is
        # present declares something, and a reader cannot tell an empty
        # declaration from a mistake.
        raise DeploymentConfigError(
            f"{label} key 'module_providers' must be a non-empty "
            f"mapping of owner to directory (Manifest: {manifest_path})"
        )
    base = deployment_dir.resolve()
    out: dict[str, Path] = {}
    folded: dict[str, str] = {}
    by_directory: dict[Path, str] = {}
    for owner, value in raw_value.items():
        if not isinstance(owner, str) or not isinstance(value, str) or not value:
            raise DeploymentConfigError(
                f"{label} 'module_providers' entries must be "
                f"owner: directory strings (Manifest: {manifest_path})"
            )
        try:
            normalized = require_nfc_identity(owner, "module provider owner")
        except Exception as error:
            raise DeploymentConfigError(
                f"{label} 'module_providers' owner {owner!r} is not a "
                f"valid identity: {error} (Manifest: {manifest_path})"
            ) from error
        if not _valid_segment(normalized):
            raise DeploymentConfigError(
                f"{label} 'module_providers' owner {owner!r} is not a "
                f"valid owner segment (Manifest: {manifest_path})"
            )
        # Fold before comparing to the built-in owner too. Case alone is not
        # enough: NFC keeps the full-width spelling distinct, so `OKG` and
        # `\uff4f\uff4b\uff47` would both slip past and shadow the shipped
        # library on a filesystem that folds them together.
        fold = unicodedata.normalize("NFKC", normalized).casefold()
        if fold == unicodedata.normalize("NFKC", BUILTIN_OWNER).casefold():
            raise DeploymentConfigError(
                f"{label} 'module_providers' must not declare owner "
                f"{owner!r}; built-in modules always resolve from the shipped "
                f"library (Manifest: {manifest_path})"
            )
        claimed = folded.setdefault(fold, normalized)
        if claimed != normalized:
            raise DeploymentConfigError(
                f"{label} 'module_providers' owners {claimed!r} and "
                f"{normalized!r} collide (Manifest: {manifest_path})"
            )
        candidate = PurePosixPath(value)
        # Split the raw string too: PurePosixPath drops a `.` segment, so the
        # parts tuple cannot see one.
        if (candidate.is_absolute() or ".." in candidate.parts
                or {".", ".."} & set(value.split("/"))):
            raise DeploymentConfigError(
                f"{label} 'module_providers' path {value!r} must be a "
                f"relative path inside the deployment, with no `.` or `..` "
                f"segment (Manifest: {manifest_path})"
            )
        resolved = deployment_dir / value
        # Reject a symlink on any component, including the leaf: the
        # containment test below follows links, so a link inside the tree
        # pointing outside it would otherwise pass.
        probe = deployment_dir
        for part in candidate.parts:
            probe = probe / part
            if probe.is_symlink():
                raise DeploymentConfigError(
                    f"{label} 'module_providers' path {value!r} "
                    f"crosses a symlink at {probe} (Manifest: {manifest_path})"
                )
        if not resolved.resolve().is_relative_to(base):
            raise DeploymentConfigError(
                f"{label} 'module_providers' path {value!r} resolves "
                f"outside the deployment directory (Manifest: {manifest_path})"
            )
        if strict and not resolved.is_dir():
            raise DeploymentConfigError(
                f"{label} 'module_providers' path {value!r} is not an "
                f"existing directory (Manifest: {manifest_path})"
            )
        # Two owners reading one directory would each claim the same modules,
        # and composition would record whichever it saw first as the origin.
        settled = resolved.resolve()
        holder = by_directory.setdefault(settled, normalized)
        if holder != normalized:
            raise DeploymentConfigError(
                f"{label} 'module_providers' owners {holder!r} and "
                f"{normalized!r} both declare {value!r} "
                f"(Manifest: {manifest_path})"
            )
        out[normalized] = resolved
    return out


# Every accepted top-level key, its allowed JSON types and what reads it are
# declared once in `okg.substrate.deployments.keys` (issue #2571). The loader's
# accepted set is derived from that table; `schemas/deployment.schema.json` is
# generated from it, so the two cannot drift apart again.
from okg.substrate.deployments.keys import (  # noqa: E402
    DEPLOYMENT_KEYS_BY_NAME as _DEPLOYMENT_KEYS_BY_NAME,
    KNOWN_TOP_LEVEL_KEYS as _KNOWN_TOP_LEVEL_KEYS,
    json_type as _json_type,
    type_allowed as _type_allowed,
)

# Issue #1291 made publish_lock_headroom the single home of the 900s bound and
# the 2.0 factor so the receipt, the publish proof and deployment readiness
# resolve ONE value. Import them rather than re-typing the numbers here: a
# literal copy would let the canonical constant move while the manifest
# default, this loader fallback and the clamp inside headroom_policy silently
# disagreed.
from okg.substrate.publish_lock_headroom import (  # noqa: E402
    DEFAULT_LOCK_HOLD_HEADROOM_FACTOR as _DEFAULT_HEADROOM_FACTOR,
    DEFAULT_PUBLISH_STATEMENT_TIMEOUT_SECONDS as _DEFAULT_STATEMENT_TIMEOUT_S,
)

_PUBLISHER_DURATION_RE = re.compile(r"^[0-9]+[smh]$")
_PUBLISHER_KEYS = frozenset({
    "kickoff_interval",
    "cross_source_gate_budget",
    "per_invariant_timeout",
    "hard_statement_timeout_seconds",
    "lock_hold_headroom_factor",
    "allow_raised_statement_timeout",
    "first_publish_warning_budget",
})
# The only keys `publisher.first_publish_warning_budget` accepts; the printed
# schema closes that block from this constant.
_PUBLISHER_WARNING_BUDGET_KEYS = frozenset({"node_count", "edge_count"})


@dataclass(frozen=True)
class FirstPublishWarningBudget:
    """Deployment-authored row-count warning boundary for a first publish."""

    node_count: int
    edge_count: int

    def as_dict(self) -> dict[str, int]:
        return {
            "node_count": self.node_count,
            "edge_count": self.edge_count,
        }


@dataclass(frozen=True)
class PublisherConfig:
    """Strict typed publisher policy loaded from ``deployment.yaml``.

    The existing duration fields are retained so the Python loader and JSON
    schema accept the same block. The new release-policy settings carry their
    authority source all the way to Publisher receipts.
    """

    kickoff_interval: str = "30s"
    cross_source_gate_budget: str = "30s"
    per_invariant_timeout: str = "5s"
    hard_statement_timeout_seconds: float = float(_DEFAULT_STATEMENT_TIMEOUT_S)
    hard_statement_timeout_source: str = "compatibility_default"
    lock_hold_headroom_factor: float = float(_DEFAULT_HEADROOM_FACTOR)
    lock_hold_headroom_factor_source: str = "compatibility_default"
    allow_raised_statement_timeout: bool = False
    allow_raised_statement_timeout_source: str = "compatibility_default"
    first_publish_warning_budget: FirstPublishWarningBudget | None = None
    first_publish_warning_budget_source: str = "undeclared"

    @property
    def hard_statement_timeout_ms(self) -> int:
        return max(1, int(self.hard_statement_timeout_seconds * 1000.0))

    def authority_receipt(self) -> dict[str, Any]:
        return {
            "schema": "okg.publisher.configuration.v1",
            "hard_statement_timeout_ms": self.hard_statement_timeout_ms,
            "hard_statement_timeout_source": self.hard_statement_timeout_source,
            "lock_hold_headroom_factor": self.lock_hold_headroom_factor,
            "lock_hold_headroom_factor_source": (
                self.lock_hold_headroom_factor_source
            ),
            "allow_raised_statement_timeout": (
                self.allow_raised_statement_timeout
            ),
            "allow_raised_statement_timeout_source": (
                self.allow_raised_statement_timeout_source
            ),
            "first_publish_warning_budget": (
                self.first_publish_warning_budget.as_dict()
                if self.first_publish_warning_budget is not None
                else None
            ),
            "first_publish_warning_budget_source": (
                self.first_publish_warning_budget_source
            ),
        }


def _publisher_number(
    value: Any,
    *,
    field_name: str,
    minimum: float,
    exclusive: bool,
    manifest_path: Path,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DeploymentConfigError(
            f"deployment.yaml key 'publisher.{field_name}' must be a finite "
            f"number (Manifest: {manifest_path})"
        )
    number = float(value)
    valid = number > minimum if exclusive else number >= minimum
    if not math.isfinite(number) or not valid:
        comparison = ">" if exclusive else ">="
        raise DeploymentConfigError(
            f"deployment.yaml key 'publisher.{field_name}' must be finite and "
            f"{comparison} {minimum:g} (Manifest: {manifest_path})"
        )
    return number


def _publisher_config_from_manifest(
    raw: dict[str, Any], *, manifest_path: Path,
) -> PublisherConfig:
    block = raw.get("publisher")
    if "publisher" not in raw:
        return PublisherConfig()
    if not isinstance(block, dict):
        raise DeploymentConfigError(
            "deployment.yaml key 'publisher' must be a mapping "
            f"(Manifest: {manifest_path})"
        )
    unknown = sorted(set(block) - _PUBLISHER_KEYS)
    if unknown:
        raise DeploymentConfigError(
            "deployment.yaml publisher block contains unknown key(s): "
            + ", ".join(unknown)
            + f" (Manifest: {manifest_path})"
        )

    durations: dict[str, str] = {}
    for key, default in (
        ("kickoff_interval", "30s"),
        ("cross_source_gate_budget", "30s"),
        ("per_invariant_timeout", "5s"),
    ):
        value = block.get(key, default)
        if not isinstance(value, str) or not _PUBLISHER_DURATION_RE.fullmatch(value):
            raise DeploymentConfigError(
                f"deployment.yaml key 'publisher.{key}' must match "
                f"'^[0-9]+[smh]$' (Manifest: {manifest_path})"
            )
        durations[key] = value

    timeout_declared = "hard_statement_timeout_seconds" in block
    hard_timeout_seconds = _publisher_number(
        block.get(
            "hard_statement_timeout_seconds",
            float(_DEFAULT_STATEMENT_TIMEOUT_S),
        ),
        field_name="hard_statement_timeout_seconds",
        minimum=0.0,
        exclusive=True,
        manifest_path=manifest_path,
    )
    factor_declared = "lock_hold_headroom_factor" in block
    headroom_factor = _publisher_number(
        block.get(
            "lock_hold_headroom_factor", float(_DEFAULT_HEADROOM_FACTOR),
        ),
        field_name="lock_hold_headroom_factor",
        minimum=1.0,
        exclusive=False,
        manifest_path=manifest_path,
    )
    raised_declared = "allow_raised_statement_timeout" in block
    allow_raised = block.get("allow_raised_statement_timeout", False)
    if not isinstance(allow_raised, bool):
        raise DeploymentConfigError(
            "deployment.yaml key "
            "'publisher.allow_raised_statement_timeout' must be a boolean "
            f"(Manifest: {manifest_path})"
        )

    warning_raw = block.get("first_publish_warning_budget")
    warning: FirstPublishWarningBudget | None = None
    warning_source = "undeclared"
    if "first_publish_warning_budget" in block:
        if not isinstance(warning_raw, dict):
            raise DeploymentConfigError(
                "deployment.yaml key 'publisher.first_publish_warning_budget' "
                f"must be a mapping (Manifest: {manifest_path})"
            )
        warning_unknown = sorted(set(warning_raw) - _PUBLISHER_WARNING_BUDGET_KEYS)
        warning_missing = sorted(_PUBLISHER_WARNING_BUDGET_KEYS - set(warning_raw))
        if warning_unknown or warning_missing:
            details: list[str] = []
            if warning_unknown:
                details.append("unknown=" + ",".join(warning_unknown))
            if warning_missing:
                details.append("missing=" + ",".join(warning_missing))
            raise DeploymentConfigError(
                "deployment.yaml publisher.first_publish_warning_budget must "
                "contain exactly node_count and edge_count ("
                + "; ".join(details)
                + f") (Manifest: {manifest_path})"
            )
        counts: dict[str, int] = {}
        for key in ("node_count", "edge_count"):
            value = warning_raw[key]
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise DeploymentConfigError(
                    "deployment.yaml key "
                    f"'publisher.first_publish_warning_budget.{key}' must be a "
                    f"positive integer (Manifest: {manifest_path})"
                )
            counts[key] = value
        warning = FirstPublishWarningBudget(**counts)
        warning_source = "deployment_manifest"

    return PublisherConfig(
        **durations,
        hard_statement_timeout_seconds=hard_timeout_seconds,
        hard_statement_timeout_source=(
            "deployment_manifest" if timeout_declared else "compatibility_default"
        ),
        lock_hold_headroom_factor=headroom_factor,
        lock_hold_headroom_factor_source=(
            "deployment_manifest" if factor_declared else "compatibility_default"
        ),
        allow_raised_statement_timeout=allow_raised,
        allow_raised_statement_timeout_source=(
            "deployment_manifest" if raised_declared else "compatibility_default"
        ),
        first_publish_warning_budget=warning,
        first_publish_warning_budget_source=warning_source,
    )


@dataclass(frozen=True)
class DeploymentManifest:
    """Resolved paths and config for a deployment."""

    name: str
    description: str
    deployment_dir: Path
    schema_dir: Path
    source_registry: Path
    enricher_registry: Path | None
    taxonomy: Path | None
    aliases: Path | None
    link_config: dict | None = None
    mcp_instructions: Path | None = None
    skills_dir: Path | None = None
    mcp_trace_path: Path | None = None
    audit_questions: Path | None = None
    tool_description_overrides: Path | None = None
    skill_triggers: Path | None = None
    refreshers: list[dict] | None = None
    postgres_dsn: str | None = None
    # `postgres.idle_in_transaction_session_timeout_ms` — the bound this
    # deployment's own Postgres sessions carry on sitting `idle in
    # transaction` (issue #1310). None when the manifest is silent, in which
    # case `okg.substrate.db.connection` applies its built-in default; `0`
    # disables the bound. Explicit process env still wins, as with `chronos`.
    postgres_idle_in_transaction_timeout_ms: int | None = None
    snapshot_path: Path | None = None
    # `module_providers` — owner -> directory supplying that owner's ontology
    # modules, as `<dir>/<module>/schema.yaml`. Built-in `okg` modules always
    # resolve from the shipped library; this key only ADDS owners, which is why
    # `okg` is refused as a key. Values are resolved Paths, so a caller that
    # walks the manifest's Path fields sees them without knowing this key
    # exists; containment itself is enforced when the key is parsed.
    module_providers: dict[str, Path] = field(default_factory=dict)
    modules: tuple[str, ...] = field(default_factory=tuple)
    modules_config: dict[str, Any] = field(default_factory=dict)
    record_prefixes: tuple[dict[str, Any], ...] = field(default_factory=tuple)
    session_start_context: dict[str, Any] | None = None
    agent_tools: Any = field(default_factory=dict)
    # Per add-substrate-pre-join-mcp-framework / S-A7-R1:
    # `mcp_pre_join_tools` is the resolved Path to the
    # deployment's pre-join tool YAML (default
    # `<deployment_dir>/mcp_pre_join_tools.yaml`); set only when
    # the file exists. `pre_join_resolvers` is the raw mapping
    # from `deployment.yaml`'s top-level `resolvers:` block per
    # S-A7-R3; empty when absent. Both fields are additive and
    # default to None / {} so deployments without pre-join
    # configuration are unaffected.
    mcp_pre_join_tools: Path | None = None
    pre_join_resolvers: dict[str, Any] = field(default_factory=dict)
    viz: dict[str, Any] | None = None
    viz_path: Path | None = None
    profiling: dict[str, Any] | None = None
    publisher: PublisherConfig = field(default_factory=PublisherConfig)
    # One fail-closed embedding configuration. The loader accepts the legacy
    # singular spelling only at admission; consumers never see raw blocks.
    embedding_config: "EmbeddingConfig" = field(default=None)  # type: ignore[assignment]
    # Chronos graph-storage tuning. Common shape:
    #   chronos:
    #     interval_continuation_percent: 95
    # Deployment values are defaults; explicit process env still wins.
    chronos: dict[str, Any] | None = None
    # Analyst search profiles; raw YAML dict form, None when absent. Shape:
    #   search:
    #     default_profile: analyst
    #     profiles:
    #       analyst: {subtypes: [chunk, entity]}
    # Validated strictly (including nested keys) at load by
    # `mcp/search_profiles.py::parse_search_config`, and resolved against the
    # pinned catalog by the CLI/MCP search surfaces. Per
    # `analyst-search-profiles` (issue #797).
    search: dict[str, Any] | None = None
    # Deployment-declared known coverage gaps; raw YAML dict form, None when
    # absent. Shape:
    #   coverage:
    #     known_gaps:
    #       - source: fireflies
    #         description: transcripts before 2024 were never collected
    # Validated strictly (including nested keys) at load by
    # `mcp/source_coverage.py::parse_coverage_config`, and echoed as the
    # deployment's own disclosure — never as a measurement — by the
    # `inspect(coverage)` manifest.
    coverage: dict[str, Any] | None = None
    # What the deployment declares it did not ingest, as short phrases:
    #   not_ingested:
    #     - TWiki pages
    #     - site-status pages
    # Validated at load by `_validated_not_ingested` and stated by the MCP
    # dataset card (`mcp/dataset_card.py`, PACT dataset-card), so an agent can
    # tell "the graph has none of it" from "the search missed it".
    not_ingested: tuple[str, ...] = field(default_factory=tuple)
    # MCP tool profiles bounding the SERVED tool surface; raw YAML dict form,
    # None when absent. Shape:
    #   mcp:
    #     default_tool_profile: discovery
    #     tool_profiles:
    #       analyst: {tools: [inspect, search, expand]}
    # Validated strictly (including nested keys, reserved and built-in profile
    # names, and unknown tool names) at load by
    # `mcp/tool_profiles.py::parse_tool_profile_config`, and resolved at MCP
    # launch. Distinct from `chat.mcp`, which filters a chat preset's tool
    # list AFTER the server has registered its surface. Per
    # `mcp-tool-surface-profiles` (issue #1457).
    mcp: dict[str, Any] | None = None
    # Governed chat-site identity; typed form, None when the manifest omits
    # the block. Shape and rules in `deployments/chat_block.py` (PACT
    # chat-manifest-block, design.md section 4):
    #   chat:
    #     enabled: true
    #     ui: {site_name: ..., theme: {base: light, accent: "#10b981"}}
    #     models: {builtin: [...], default: ...}
    #     preset: {display_name: ..., system_prompt_ref: ...}
    #     search_profile: analyst      # must name a declared search profile
    #     mcp: {port: 8101, tools: [...]}
    # Validated strictly at EVERY level at load: an unknown key at any depth,
    # an undeclared search profile, a tool outside the known universe, or a
    # default model outside the built-in list raises. Nothing consumes the
    # parsed block yet — the consumer is the chat-sync-projection PACT — so
    # validation IS the deliverable here.
    chat: Any = None
    # Deployment release contract. Common shape:
    #   release:
    #     channel: dev | prod
    #     pin:
    #       code_revision: ...
    #       code_repository: /path/to/the/framework/checkout
    #       deployment_revision: ...  # optional; legacy default is repo HEAD
    #       schema_checksum: ...
    #       catalog_fingerprint: ...
    #       skill_bundle_hash: ...
    # Parsed as raw YAML so deployment_release.py owns validation and
    # fail-loud skew semantics.
    release: dict[str, Any] | None = None
    # Registered producing checkout for canonical dogfood deployments. Unlike
    # declared component paths, this may point outside the deployment dir and is
    # allowed to be missing during dry model loads; provenance checks own the
    # fail-closed publish policy.
    sync_root: Path | None = None
    # Environment variables a DECLARED sync_root references that this machine
    # could not resolve (issue #1014). Empty when sync_root resolved AND when
    # the manifest declares none -- the two are distinguished by `sync_root`
    # being None with this tuple empty (undeclared) versus None with this tuple
    # populated (declared but unresolvable on this machine). `from_yaml` raises
    # rather than returning the latter; `from_yaml_lenient` returns it so
    # callers that actually read the field can fail closed instead of falling
    # back to a cwd-derived checkout.
    sync_root_unresolved: tuple[str, ...] = ()
    # Deployment portfolio lifecycle contract. Raw form; validation lives in
    # deployment_contract.py so the manifest loader stays a strict parser.
    contract: dict[str, Any] | None = None
    # DBOS runtime layer configuration. Always present (defaults to a
    # disabled-by-default RuntimeConfig for manifests without a local
    # worker process). The legacy okg.jobs queue, publisher lease table,
    # and substrate pg_cron paths are retired.
    runtime: "RuntimeConfig" = field(default=None)  # type: ignore[assignment]
    # ------------------------------------------------------------------
    # Typed superset (deployment-manifest-model PACT). These capture
    # top-level blocks that from_yaml historically IGNORED, so the
    # scattered raw re-readers (publisher/deployment_stack.py and
    # friends) can consume one parsed model instead of re-opening
    # deployment.yaml. All default to None / empty so deployments that
    # omit a block are unaffected; the golden test enforces that the
    # existing fields above are unchanged.
    #
    # Consistency policy block. Common shape:
    #   consistency:
    #     dangling_edges: block | prune
    # Consumed by publisher/deployment_stack.py:load_dangling_policy.
    consistency: dict[str, Any] | None = None
    # Projection-time extractor declaration. Common shape:
    #   extractor:
    #     class: my.module.MyExtractor
    #     config: {...}
    #     source_subtypes: [...]
    #     authority: {...}
    # Consumed by publisher/deployment_stack.py:load_projection_extractor
    # and load_projection_extractor_source_subtypes.
    extractor: dict[str, Any] | None = None
    # Deployment-declared enricher block (inline list or registry-style).
    # Raw form; enricher instantiation still lives in enricher_loader.
    enrichers: Any = None
    # Ordered deployment validators run against the final candidate graph before
    # checkpoint creation. The publisher owns class/config validation; this field
    # keeps the selected declaration authority explicit.
    checkpoint_attestors: Any = None
    # Optional definition-owned root containing attestor implementation and
    # bounded data. Sealed definitions may point at a shared in-bundle root;
    # an omitted value means the definition directory itself.
    checkpoint_attestor_root: Path | None = None
    # THE declared agent surface (PACT effective-surface-declaration, issue
    # #2012). `None` means this deployment declares no block and keeps the
    # frozen legacy path: `mcp_instructions`, `chat.mcp.tools`,
    # `tool_description_overrides` and the CHAT_DEFAULT_TOOLS fallback. Issue
    # #2067 makes the block mandatory and deletes that path. The two cannot
    # coexist in one manifest: declaring the block refuses every legacy key.
    agent_surface: Any = None
    # Nomos policy block. Raw form; nomos modules own validation.
    nomos: dict[str, Any] | None = None
    # Deployment-up lifecycle policy. Raw form; deployment_upgrade owns
    # strict validation because omitted blocks intentionally preserve the
    # production current-full-restore default.
    deployment_up: dict[str, Any] | None = None
    # Fully composed, path-substituted manifest used by sealed environments.
    # Excluded from repr/comparison because it may contain the resolved DSN;
    # callers must not serialize it directly.
    effective_raw: dict[str, Any] = field(
        default_factory=dict,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        # Frozen dataclass: use object.__setattr__ to populate the
        # default RuntimeConfig when the caller did not pass one.
        # We delay the import to avoid a top-of-module cycle between
        # this loader and the runtime package.
        if self.runtime is None:
            from ..runtime import RuntimeConfig
            object.__setattr__(self, "runtime", RuntimeConfig())
        if self.embedding_config is None:
            from ..embeddings import EmbeddingConfig
            object.__setattr__(self, "embedding_config", EmbeddingConfig())

    @property
    def bridges_dir(self) -> Path | None:
        """Directory containing bridge ontology YAML files, or
        None. Convention: ``<schema_dir>/bridges/*.yaml``.
        """
        candidate = self.schema_dir / "bridges"
        return candidate if candidate.is_dir() else None

    @classmethod
    def from_yaml(cls, manifest_path: Path) -> DeploymentManifest:
        """Load a manifest from a deployment.yaml file.

        String-valued fields anywhere in the manifest (including
        leaves inside ``modules_config``) may contain
        ``${manifest_dir}`` / ``${deployment_dir}``, expanded to the
        absolute path of the directory containing the manifest, and
        ``${repo_root}``, expanded to the repository root when the
        deployment lives under ``<repo>/deployments/<name>``.

        Result cached by `(absolute_path, mtime_ns)`; see
        ``_MANIFEST_CACHE``. The publisher's `_locked_connection`
        re-resolves the deployment on every cycle — the
        ~8 ms YAML-parse cost is avoided after the first call.

        The cache is env-AWARE. A manifest's ``postgres.dsn`` (and any
        other ``${VAR}`` field) is interpolated from ``os.environ`` at
        build time, so each entry records the env-var values it was built
        against. A hit whose recorded values still match the current
        environment is returned as-is; if a referenced env var changed
        (e.g. a test sets ``DSG_OKG_DSN`` after a sibling resolved the
        same file with it unset), the entry is rebuilt rather than
        serving a manifest with a stale (or ``None``) ``postgres_dsn``.
        The common case — a worker with a stable environment — stays a
        ``(path, mtime)`` hit.
        """
        p = Path(manifest_path).resolve()
        try:
            mtime = p.stat().st_mtime_ns
        except FileNotFoundError:
            # Fall through to the canonical FileNotFoundError below.
            return cls._from_yaml_uncached(manifest_path)
        key = (str(p), mtime)
        cached = _MANIFEST_CACHE.get(key)
        if cached is not None:
            manifest, env_signature = cached
            if _env_signature_matches(env_signature, p.parent):
                return manifest
            # A referenced env var changed since this entry was built, so
            # its interpolated fields (the DSN, sync_root) are stale — rebuild.
        result, env_deps = cls._parse_with_env_deps(manifest_path)
        _MANIFEST_CACHE[key] = (result, _env_signature(env_deps, p.parent))
        return result

    @classmethod
    def from_yaml_lenient(cls, manifest_path: Path) -> DeploymentManifest:
        """Parse a manifest into the typed model WITHOUT whole-manifest
        validation.

        Same single parsing implementation as :meth:`from_yaml` (both
        delegate to :meth:`_build_from_raw`), but unknown-top-level-key
        rejection and declared-path existence enforcement are SKIPPED.

        This is the entry point for config-block readers (the publisher
        stack's ``load_dangling_policy`` / ``load_projection_extractor``
        / ``embeddings_enabled``) that are also invoked against PARTIAL
        manifests — a fixture declaring only ``name:`` + one config
        block, with no ``schema_dir``/``source_registry`` and no real
        registry files. For those, "block absent" is a legitimate state,
        not a misconfiguration; strict validation belongs on the
        deployment-RESOLVE path (:func:`resolve_deployment` →
        :meth:`from_yaml`), not on a single-block read. Not cached —
        callers are config-block reads, not the hot resolve path.
        """
        with open(manifest_path) as f:
            raw = fastyaml.safe_load(f)
        deployment_dir = manifest_path.parent
        raw = _substitute_manifest_placeholders(raw, deployment_dir)
        return cls._build_from_raw(
            raw, deployment_dir, manifest_path, strict=False,
        )

    @classmethod
    def _from_yaml_uncached(
        cls, manifest_path: Path,
    ) -> DeploymentManifest:
        manifest, _env_deps = cls._parse_with_env_deps(manifest_path)
        return manifest

    @classmethod
    def _parse_with_env_deps(
        cls, manifest_path: Path,
    ) -> tuple[DeploymentManifest, tuple[str, ...]]:
        """Parse + build a manifest AND report the env-var names it
        references via ``${VAR}`` (after path-anchor substitution).

        The referenced names drive ``_MANIFEST_CACHE``'s env-awareness:
        the caller snapshots their current values (``_env_signature``) so
        a later resolve under a changed environment rebuilds instead of
        serving a stale DSN. Path anchors (``${manifest_dir}`` etc.) are
        expanded first, so every remaining ``${VAR}`` is a genuine env
        reference. Not cached itself — ``from_yaml`` owns the cache.
        """
        with open(manifest_path) as f:
            raw = fastyaml.safe_load(f)

        deployment_dir = manifest_path.parent
        raw = _substitute_manifest_placeholders(raw, deployment_dir)
        env_deps = _referenced_env_var_names(raw)
        manifest = cls._build_from_raw(
            raw, deployment_dir, manifest_path, strict=True,
        )
        return manifest, env_deps

    @classmethod
    def _build_from_raw(
        cls,
        raw: Any,
        deployment_dir: Path,
        manifest_path: Path,
        *,
        strict: bool,
    ) -> DeploymentManifest:
        """Construct a manifest from an already-substituted raw dict.

        Single parsing implementation shared by :meth:`from_yaml`
        (``strict=True``) and :meth:`from_yaml_lenient`
        (``strict=False``). ``strict`` governs only the two validation
        gates added by the deployment-manifest-model PACT — unknown-key
        rejection and declared-path existence — never the captured field
        values, which are identical on both paths.
        """
        if strict:
            # A list or scalar document would otherwise surface as the
            # first missing required key (#2713).
            _reject_non_mapping_manifest(raw, manifest_path)
            # Whole-manifest validation (deployment-manifest-model PACT):
            # reject unknown top-level keys before any field parse, so a
            # typo'd block surfaces a structured error instead of being
            # silently dropped.
            _reject_unknown_top_level_keys(raw, manifest_path)
        # Both modes: the lenient reader is the one the publisher stack uses,
        # so a strict-only check would leave the silent drop where the value
        # is consumed. The check requires no key, so a partial manifest still
        # loads.
        _reject_wrong_top_level_types(raw, manifest_path)
        _reject_unknown_block_keys(raw, manifest_path)

        def _resolve(key: str) -> Path | None:
            """Resolve an OPTIONAL declared path. Absent key -> None.
            Present-but-missing path -> DeploymentConfigError naming the
            key in strict mode (never silently coerce a typo to None,
            which would silently disable the component the path
            configures); in lenient mode preserve the historical
            None-if-missing behavior for partial-manifest reads."""
            val = raw.get(key) if isinstance(raw, dict) else None
            if val is None:
                return None
            p = deployment_dir / val
            if not p.exists():
                if not strict:
                    return None
                raise DeploymentConfigError(
                    f"deployment.yaml key {key!r} points at a missing "
                    f"path: {p}. A declared optional path must exist; "
                    f"remove the key or create the path. (Manifest: "
                    f"{manifest_path})"
                )
            return p

        def _resolve_surface_instructions() -> Path | None:
            """`agent_surface.instructions`, resolved like `mcp_instructions`.

            The block replaces the top-level key (declaring both is refused
            by `parse_agent_surface`), so this is where the server's MCP
            instructions come from once a deployment declares the block.
            Before this, a declared `agent_surface.instructions` was scanned
            but never SENT, and moving `mcp_instructions` into the block
            would have served no instructions with nothing said. Same
            strictness as `_resolve`: a declared path must exist.
            """
            block = raw.get("agent_surface") if isinstance(raw, dict) else None
            val = block.get("instructions") if isinstance(block, dict) else None
            if val is None:
                return None
            p = deployment_dir / str(val)
            if not p.exists():
                if not strict:
                    return None
                raise DeploymentConfigError(
                    f"deployment.yaml key 'agent_surface.instructions' points "
                    f"at a missing path: {p}. A declared optional path must "
                    f"exist; remove the key or create the path. (Manifest: "
                    f"{manifest_path})"
                )
            return p

        def _resolve_module_providers() -> dict[str, Path]:
            return resolve_declared_module_providers(
                raw, deployment_dir, manifest_path=manifest_path, strict=strict,
            )

        def _require(key: str) -> Path:
            val = raw.get(key) if isinstance(raw, dict) else None
            if val is None:
                if not strict:
                    # Partial-manifest read: no required structure to
                    # resolve. Return a benign placeholder under the
                    # deployment dir; config-block readers ignore it.
                    return deployment_dir / key
                raise ValueError(
                    f"deployment.yaml missing required key: {key!r}"
                )
            p = deployment_dir / val
            if not p.exists() and strict:
                raise DeploymentConfigError(
                    f"deployment.yaml required key {key!r} points at a "
                    f"missing path: {p}. (Manifest: {manifest_path})"
                )
            return p

        if not isinstance(raw, dict):
            raw = {}

        # sync_root is per-machine runtime authority and may be declared as
        # `${VAR}` (issue #1014). Resolution is shared with the publish guard
        # and the drift probe so no two consumers disagree about what the
        # deployment declared.
        #
        # An unresolvable declaration is CARRIED, not raised, on both the
        # strict and lenient paths. `from_yaml` is the single door for every
        # read-only verb (`okg search`, `okg trace`, `okg doctor`), and a
        # manifest that ships inside an installed package would otherwise make
        # all of them die on a machine that has never set the variable --
        # shipping a regression to the very consumers this field exists to
        # serve. The refusal belongs where the value is USED as authority: the
        # Tier W publish guard, `deployment apply`, and the drift probe each
        # fail closed on `sync_root_unresolved` instead.
        sync_root_resolution = resolve_declared_sync_root(
            raw, deployment_dir=deployment_dir,
        )

        return cls(
            name=raw.get("name", manifest_path.parent.name),
            description=raw.get("description", ""),
            deployment_dir=deployment_dir,
            schema_dir=_require("schema_dir"),
            module_providers=_resolve_module_providers(),
            source_registry=_require("source_registry"),
            enricher_registry=_resolve("enricher_registry"),
            taxonomy=_resolve("taxonomy"),
            aliases=_resolve("aliases"),
            link_config=raw.get("link_config"),
            mcp_instructions=(
                _resolve("mcp_instructions") or _resolve_surface_instructions()
            ),
            skills_dir=_resolve("skills_dir"),
            mcp_trace_path=(
                deployment_dir / raw["mcp_trace_path"]
                if raw.get("mcp_trace_path") else None
            ),
            audit_questions=_resolve("audit_questions"),
            tool_description_overrides=_resolve(
                "tool_description_overrides"
            ),
            skill_triggers=_resolve("skill_triggers"),
            refreshers=raw.get("refreshers"),
            # postgres.dsn: support both the modern nested form
            # (`postgres: {dsn: ${OKG_DSN}}`, env-interpolated) and the
            # legacy flat `postgres_dsn: ...` form. The nested form is
            # the convention from `one-db-per-deployment` (2026-04-26);
            # `_resolve_deployment_dsn` in the CLI already reads it the
            # same way + interpolates. We do the same here so callers
            # of `manifest.postgres_dsn` see a usable value, not None.
            postgres_dsn=_resolve_postgres_dsn(raw),
            postgres_idle_in_transaction_timeout_ms=(
                _resolve_idle_in_transaction_timeout_ms(
                    raw, manifest_path=manifest_path,
                )
            ),
            snapshot_path=(
                deployment_dir / raw["snapshot_path"]
                if raw.get("snapshot_path") else None
            ),
            modules=tuple(raw.get("modules") or ()),
            modules_config=dict(raw.get("modules_config") or {}),
            record_prefixes=tuple(
                entry for entry in raw.get("record_prefixes") or ()
                if isinstance(entry, dict)
            ),
            session_start_context=(
                dict(raw["session_start_context"])
                if isinstance(raw.get("session_start_context"), dict)
                else None
            ),
            agent_tools=(
                raw["agent_tools"] if raw.get("agent_tools") is not None
                else {}
            ),
            viz=_validated_viz_block(raw, manifest_path=manifest_path),
            viz_path=_resolve("viz_path"),
            embedding_config=_embedding_config_from_manifest(
                raw, manifest_path=manifest_path,
            ),
            chronos=(
                dict(raw["chronos"])
                if isinstance(raw.get("chronos"), dict) else None
            ),
            # Validated eagerly at load rather than lazily at first search, so
            # a misspelled key under `search:` fails where the operator can see
            # it instead of silently leaving the deployment unscoped.
            search=_validated_search_block(
                raw.get("search"), manifest_path=manifest_path,
            ),
            coverage=_validated_coverage_block(
                raw.get("coverage"), manifest_path=manifest_path,
            ),
            not_ingested=_validated_not_ingested(
                raw.get("not_ingested"), manifest_path=manifest_path,
            ),
            # Same discipline as `search:` above: a misspelled key nested
            # under `mcp:` would otherwise leave the deployment believing it
            # had bounded its MCP surface when it had not.
            mcp=_validated_mcp_block(
                raw.get("mcp"), manifest_path=manifest_path,
            ),
            # Same discipline as `search:` above, and for the same reason: the
            # top-level census matches only top-level NAMES, so a misspelled
            # key nested inside `chat:` would otherwise leave the deployment
            # believing it configured a site it did not.
            chat=_validated_chat_block(raw, manifest_path=manifest_path),
            release=(
                dict(raw["release"])
                if isinstance(raw.get("release"), dict) else None
            ),
            sync_root=sync_root_resolution.path,
            sync_root_unresolved=sync_root_resolution.missing,
            contract=(
                dict(raw["contract"])
                if isinstance(raw.get("contract"), dict) else None
            ),
            profiling=(
                dict(raw["profiling"])
                if isinstance(raw.get("profiling"), dict) else None
            ),
            publisher=_publisher_config_from_manifest(
                raw, manifest_path=manifest_path,
            ),
            runtime=_runtime_config_from_manifest(raw.get("runtime")),
            mcp_pre_join_tools=_resolve_pre_join_tools(
                raw=raw, deployment_dir=deployment_dir,
            ),
            pre_join_resolvers=(
                dict(raw["resolvers"])
                if isinstance(raw.get("resolvers"), dict) else {}
            ),
            # Typed superset: capture the blocks from_yaml historically
            # ignored so consumers read the parsed model, not raw YAML.
            consistency=(
                dict(raw["consistency"])
                if isinstance(raw.get("consistency"), dict) else None
            ),
            extractor=(
                dict(raw["extractor"])
                if isinstance(raw.get("extractor"), dict) else None
            ),
            enrichers=(
                raw["enrichers"] if raw.get("enrichers") is not None
                else None
            ),
            checkpoint_attestors=(
                raw["checkpoint_attestors"]
                if raw.get("checkpoint_attestors") is not None
                else None
            ),
            checkpoint_attestor_root=_resolve("checkpoint_attestor_root"),
            agent_surface=_validated_agent_surface_block(
                raw, manifest_path=manifest_path,
            ),
            nomos=(
                dict(raw["nomos"])
                if isinstance(raw.get("nomos"), dict) else None
            ),
            deployment_up=(
                dict(raw["deployment_up"])
                if isinstance(raw.get("deployment_up"), dict) else None
            ),
            effective_raw=dict(raw),
        )


# ---------------------------------------------------------------------------
# Manifest placeholder substitution
# ---------------------------------------------------------------------------


# Compiled once at import: matches a `${VAR}` env-var reference. Shared
# by DSN interpolation (`_interpolate_env_vars`) and the manifest env-
# dependency scan (`_referenced_env_var_names`) so both agree on exactly
# what counts as an env reference.
_ENV_VAR_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def _interpolate_env_vars(value: str) -> str:
    """Expand ``${VAR}`` references against the environment.

    Mirrors ``cli/main.py:_interpolate_env`` (kept local here to avoid
    a circular import between the deployments loader and the CLI).
    Unset vars are left as-is so a caller can surface a clear error.
    """
    def _sub(m):
        return os.environ.get(m.group(1), m.group(0))

    return _ENV_VAR_RE.sub(_sub, value)


def _referenced_env_var_names(raw: Any) -> tuple[str, ...]:
    """Sorted, de-duplicated env-var names referenced via ``${VAR}``
    anywhere in the (already path-anchor-substituted) manifest.

    Path anchors (``${manifest_dir}`` / ``${deployment_dir}`` /
    ``${repo_root}``) are expanded by ``_substitute_manifest_placeholders``
    before this runs, so every remaining ``${VAR}`` is a genuine env
    reference. Only ``postgres.dsn`` interpolates env vars today, but
    scanning the whole document keeps ``_MANIFEST_CACHE``'s env signature
    correct if another field starts interpolating later: over-scanning is
    harmless (it can only trigger a rebuild that yields an identical
    manifest), whereas under-scanning would serve a stale value.
    """
    names: set[str] = set()
    _collect_env_var_names(raw, names)
    return tuple(sorted(names))


def _collect_env_var_names(node: Any, out: set[str]) -> None:
    if isinstance(node, str):
        out.update(_ENV_VAR_RE.findall(node))
    elif isinstance(node, dict):
        for value in node.values():
            _collect_env_var_names(value, out)
    elif isinstance(node, list):
        for value in node:
            _collect_env_var_names(value, out)


def _env_signature(
    env_var_names: tuple[str, ...],
    deployment_dir: Path | None = None,
) -> tuple[tuple[str, str | None, str | None], ...]:
    """Snapshot the current values of ``env_var_names`` for use as
    ``_MANIFEST_CACHE``'s env-awareness discriminator. A ``None`` value
    records "unset", so an env var going from unset to set (or vice
    versa) is detected as a change.

    Each name records BOTH sources separately: its process-environment value
    and its ``.okg/local.env`` value. Merging them into one value would break
    the DSN's invalidation -- ``_resolve_postgres_dsn`` interpolates from
    ``os.environ`` alone, so a variable that is set in the profile and later
    exported to the shell keeps the same MERGED value while the DSN the
    manifest was built with (``None``) becomes wrong. Recording both means a
    change in either source rebuilds (issue #1014)."""
    return tuple(
        (name, *env_and_profile_values(name, start=deployment_dir))
        for name in env_var_names
    )


def _env_signature_matches(
    signature: tuple[tuple[str, str | None, str | None], ...],
    deployment_dir: Path | None = None,
) -> bool:
    """True if every env var in ``signature`` still holds the values it
    had, in both sources, when the cached manifest was built."""
    if not signature:
        return True
    return all(
        env_and_profile_values(name, start=deployment_dir) == (process, profile)
        for name, process, profile in signature
    )


def _resolve_pre_join_tools(
    *, raw: dict, deployment_dir: Path,
) -> Path | None:
    """Resolve the path to the deployment's pre-join MCP tools YAML.

    Convention (S-A7-R1): the path is configurable via
    `mcp_pre_join_tools:` in `deployment.yaml`, defaulting to
    `<deployment_dir>/mcp_pre_join_tools.yaml`. The substrate
    treats a missing file as "no deployment-declared pre-join
    tools" — existing deployments are unaffected.
    """
    configured = raw.get("mcp_pre_join_tools")
    if isinstance(configured, str) and configured:
        candidate = deployment_dir / configured
    else:
        candidate = deployment_dir / "mcp_pre_join_tools.yaml"
    return candidate if candidate.is_file() else None


def _resolve_postgres_dsn(raw: dict) -> str | None:
    """Read the postgres DSN from a parsed manifest dict.

    Three accepted forms, checked in order:

      * ``postgres: {dsn: ${OKG_DSN}}`` (nested; the modern convention
        per `one-db-per-deployment`, 2026-04-26).
      * ``postgres_dsn: ${OKG_DSN}`` (flat; legacy back-compat).
      * Neither → returns None. Callers decide whether that's fatal.

    Env-var references are interpolated; **if a referenced var is
    unset, this function returns None** rather than passing the
    literal placeholder downstream. That literal-passthrough behavior
    caused a silent worker-failure bug (2026-05-11): a worker started
    with ``--dsn ... --deployment synth-bench`` but no ``OKG_DSN``
    env var let every source connection try
    ``postgres://...?postgres://...${OKG_DSN}`` and die deep inside
    psycopg with ``missing "=" after "${OKG_DSN}"``. The workflow
    silently aggregated per-source failures into ``any_failed=true``
    and returned SUCCESS. Refusing to return the placeholder makes
    callers surface a clear "DSN unresolvable" error at the point
    of use.
    """
    pg_block = raw.get("postgres")
    raw_dsn = None
    if isinstance(pg_block, dict) and isinstance(pg_block.get("dsn"), str):
        raw_dsn = pg_block["dsn"]
    else:
        flat = raw.get("postgres_dsn")
        if isinstance(flat, str):
            raw_dsn = flat
    if raw_dsn is None:
        return None
    expanded = _interpolate_env_vars(raw_dsn).strip()
    if not expanded:
        return None
    # If interpolation left ANY ${VAR} unsubstituted, the DSN is not
    # usable — refuse it. A literal `${VAR}` passed downstream fails deep
    # inside psycopg (see the silent-worker-failure note above).
    if _ENV_VAR_RE.search(expanded):
        return None
    return expanded


_IDLE_IN_TRANSACTION_KEY = "idle_in_transaction_session_timeout_ms"


def _resolve_idle_in_transaction_timeout_ms(
    raw: dict, *, manifest_path: Path,
) -> int | None:
    """Read ``postgres.idle_in_transaction_session_timeout_ms``.

    Returns None when the key is absent, in which case
    `okg.substrate.db.connection` applies its built-in default. A present but
    invalid value refuses at load: silently ignoring it would leave the
    operator believing a bound is in force when it is not, which is the
    failure mode issue #1310 is about.
    """
    pg_block = raw.get("postgres")
    if not isinstance(pg_block, dict) or _IDLE_IN_TRANSACTION_KEY not in pg_block:
        return None
    value = pg_block[_IDLE_IN_TRANSACTION_KEY]
    # bool is an int subclass; `true` here is a typo, not a duration.
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise DeploymentConfigError(
            f"deployment.yaml postgres.{_IDLE_IN_TRANSACTION_KEY} must be a "
            f"non-negative integer number of milliseconds, got {value!r}. "
            f"Set it in milliseconds (e.g. 300000 for 5 minutes), or 0 to "
            f"leave okg sessions unbounded; remove the key to take the "
            f"built-in default. (Manifest: {manifest_path})"
        )
    return value


def _validated_search_block(
    raw: Any, *, manifest_path: Path,
) -> dict[str, Any] | None:
    """Return the `search:` block, having proved it parses.

    The top-level census above only governs the top-level key SET; the string
    `search` appears throughout `src/okg/`, so a misspelled key NESTED under
    `search:` (`default_profle:`, `subtyps:`) would sail through it and leave
    the deployment silently unscoped. Parsing here makes that a load-time
    error. The raw dict is what the model stores — `parse_search_config` is
    re-run by the search surfaces, which need the typed form anyway.
    """
    if raw is None:
        return None
    from okg.substrate.mcp.search_profiles import parse_search_config

    parse_search_config(raw, manifest_path=manifest_path)
    return dict(raw) if isinstance(raw, dict) else raw


def _validated_coverage_block(
    raw: Any, *, manifest_path: Path,
) -> dict[str, Any] | None:
    """Return the `coverage:` block, having proved it parses.

    Same discipline as `_validated_search_block` and for the same reason: the
    top-level census matches only top-level NAMES, so a misspelled key nested
    under `coverage:` would be dropped in silence and the deployment would
    believe it had disclosed a coverage gap it had not.
    """
    if raw is None:
        return None
    from okg.substrate.mcp.source_coverage import parse_coverage_config

    parse_coverage_config(raw, manifest_path=manifest_path)
    return dict(raw) if isinstance(raw, dict) else raw


#: Limits on `not_ingested`: the list is served in every MCP session's
#: instructions (PACT dataset-card), so it stays a few short phrases.
NOT_INGESTED_MAX_ITEMS = 20
NOT_INGESTED_MAX_CHARS = 120


def _validated_not_ingested(
    raw: Any, *, manifest_path: Path,
) -> tuple[str, ...]:
    """Return the `not_ingested:` list, refusing anything else.

    Each entry is one short phrase naming what the deployment did not ingest
    ("TWiki pages", "site-status pages"). The MCP dataset card states them
    verbatim, so an entry is one line of at most `NOT_INGESTED_MAX_CHARS`
    characters, and the list holds at most `NOT_INGESTED_MAX_ITEMS`.
    """
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise DeploymentConfigError(
            f"'not_ingested' must be a list of short strings, got "
            f"{type(raw).__name__} (Manifest: {manifest_path})"
        )
    if len(raw) > NOT_INGESTED_MAX_ITEMS:
        raise DeploymentConfigError(
            f"'not_ingested' lists {len(raw)} entries; at most "
            f"{NOT_INGESTED_MAX_ITEMS} are allowed (Manifest: {manifest_path})"
        )
    out: list[str] = []
    for index, item in enumerate(raw):
        if (
            not isinstance(item, str)
            or not item.strip()
            or len(item) > NOT_INGESTED_MAX_CHARS
            or "\n" in item
            or "\r" in item
        ):
            raise DeploymentConfigError(
                f"'not_ingested[{index}]' must be one non-empty line of at "
                f"most {NOT_INGESTED_MAX_CHARS} characters, got {item!r} "
                f"(Manifest: {manifest_path})"
            )
        out.append(item.strip())
    return tuple(out)


def _validated_mcp_block(
    raw: Any, *, manifest_path: Path,
) -> dict[str, Any] | None:
    """Return the `mcp:` block, having proved it parses.

    Same reasoning as `_validated_search_block`: the top-level census governs
    only the top-level key SET, and `mcp` occurs throughout `src/okg/`, so a
    misspelled key NESTED under `mcp:` (`default_tool_profle:`, `tols:`)
    would sail through it and leave the deployment serving the full MCP
    surface while believing it had bounded it. Parsing here makes that a
    load-time error the operator can see.
    """
    if raw is None:
        return None
    from okg.substrate.mcp.tool_profiles import parse_tool_profile_config

    parse_tool_profile_config(raw, manifest_path=manifest_path)
    return dict(raw) if isinstance(raw, dict) else raw


def _validated_agent_surface_block(
    raw: Any, *, manifest_path: Path,
) -> Any:
    """Return the parsed ``agent_surface:`` block, or ``None`` when absent.

    Imported lazily, like ``parse_chat_block`` above, because
    ``mcp.effective_surface`` reaches back into the MCP package for the tool
    universe and the response bounds.

    Validated EAGERLY at load, and for the same reason the ``mcp:`` block is:
    this block is the only statement of what an agent may call, so a
    misspelled key nested inside it would leave the deployment believing it
    had bounded a surface it had not. The top-level census matches only
    top-level names and cannot see inside.
    """
    from okg.substrate.mcp.effective_surface import parse_agent_surface

    return parse_agent_surface(raw, manifest_path=manifest_path)


def _validated_viz_block(
    raw: Any, *, manifest_path: Path,
) -> dict[str, Any] | None:
    """Return the ``viz:`` block unchanged, having proved ``viz.theme`` validates.

    Same discipline as ``search:`` and ``chat:`` above, applied to exactly one
    sub-block. ``viz`` stays the untyped pass-through dict it has always been —
    ``port``, ``presentation`` and the layout tuning are unchanged and
    unchecked — but ``viz.theme`` is now a TYPED token set the operator console
    reads (PACT ``operator-ui-deployment-theme``), so a malformed token is
    refused where the operator can see it rather than served into a stylesheet.

    Validated eagerly at load, and for the reason ``viz`` needed it most:
    ``viz`` is a known top-level key and nothing validates its sub-keys, so
    before this a mistyped theme token loaded clean and painted nothing.
    """
    from .chat_block import parse_viz_theme

    parse_viz_theme(raw, manifest_path=manifest_path)
    block = raw.get("viz") if isinstance(raw, dict) else None
    return dict(block) if isinstance(block, dict) else None


def _validated_chat_block(
    raw: Any, *, manifest_path: Path,
) -> Any:
    """Return the parsed ``chat:`` block, having proved it validates.

    Imported lazily (like ``parse_search_config`` above) because
    ``chat_block`` imports this module for :class:`DeploymentConfigError`.

    Also runs the accent resolver, whose only purpose here is to REFUSE a
    ``chat.ui.theme.accent``: the recorded knob census measured the chat
    accent as not drivable at the pinned Open WebUI version, so a value
    declared there could never render, and silently dropping it would leave
    the operator staring at a color that never reaches a screen. The accent
    itself is an operator-console token, read by ``okg.viz.registry`` from
    the raw manifest rather than from this call — as is the wider typed
    ``viz.theme`` token set, whose own load-time refusal lives in
    ``_validated_viz_block`` above rather than here, because it is a ``viz:``
    rule and not a ``chat:`` one.

    The parser already rejects ``chat.ui.theme`` as an inadmissible field, so
    on this path the resolver is a second line of defense; it is the FIRST
    line on the registry's tolerant raw-YAML path, which never calls the
    parser.
    """
    if not isinstance(raw, dict):
        return None
    from .chat_block import parse_chat_block, resolve_theme_accent

    block = parse_chat_block(raw, manifest_path=manifest_path)
    resolve_theme_accent(raw, manifest_path=manifest_path)
    return block


def _reject_unknown_top_level_keys(
    raw: Any, manifest_path: Path,
) -> None:
    """Fail loudly on an unknown top-level deployment.yaml key.

    Per the deployment-manifest-model PACT, the manifest is the contract
    for ~25 consumers; a typo'd top-level block was previously silently
    no-op'd. The known-key set is censused over every available manifest
    (in-repo + cached external) so a legitimate key is not rejected.

    A non-mapping document (None / list / scalar) is left to the
    downstream ``_require`` / field parse to surface — this validator
    only governs the top-level key SET of a mapping.
    """
    if not isinstance(raw, dict):
        return
    # YAML 1.1 reads an unquoted `on:`/`yes:` key as a boolean; name it
    # instead of letting the sort below fail on mixed key types.
    not_text = [key for key in raw if not isinstance(key, str)]
    if not_text:
        raise DeploymentConfigError(
            "deployment.yaml has top-level key(s) that are not text: "
            + ", ".join(repr(key) for key in not_text)
            + ". YAML reads an unquoted on/off/yes/no key as a boolean; quote "
            f"it. (Manifest: {manifest_path})"
        )
    unknown = sorted(set(raw.keys()) - _KNOWN_TOP_LEVEL_KEYS)
    if unknown:
        keys_phrase = ", ".join(repr(k) for k in unknown)
        raise DeploymentConfigError(
            f"deployment.yaml has unknown top-level key(s): {keys_phrase}. "
            f"Known keys: {sorted(_KNOWN_TOP_LEVEL_KEYS)}. A typo'd block "
            f"is silently ignored otherwise — fix the key or add it to the "
            f"known-key census. (Manifest: {manifest_path})"
        )


def _reject_wrong_top_level_types(raw: Any, manifest_path: Path) -> None:
    """Refuse a known top-level value whose JSON type its key does not allow.

    Keys whose own block parser already refuses a wrong value are skipped so
    that parser's error is the one reported. Unknown keys are left to
    ``_reject_unknown_top_level_keys``.
    """
    if not isinstance(raw, dict):
        return
    for key, value in raw.items():
        row = _DEPLOYMENT_KEYS_BY_NAME.get(key) if isinstance(key, str) else None
        if row is None or row.parser or _type_allowed(row, value):
            continue
        raise DeploymentKeyTypeError(
            key,
            allowed=row.types,
            found=_json_type(value),
            manifest_path=manifest_path,
        )


# The only keys okg reads from the `postgres` and `chronos` blocks. The
# printed schema closes both blocks from these constants (#2634).
_POSTGRES_KEYS = frozenset({
    "dsn",
    "database",
    "self_hosted_authoritative",
    _IDLE_IN_TRANSACTION_KEY,
})
_CHRONOS_KEYS = frozenset({
    "interval_continuation_percent",
    "history_retention",
})


def _reject_unknown_block_keys(raw: Any, manifest_path: Path) -> None:
    """Refuse a key inside `postgres` or `chronos` that okg does not read.

    Their readers take single keys with ``.get()``, so a misspelled key would
    otherwise be dropped and the default would run in its place. A block that
    is not a mapping is left to ``_reject_wrong_top_level_types``.
    """
    if not isinstance(raw, dict):
        return
    for block, known in (("postgres", _POSTGRES_KEYS), ("chronos", _CHRONOS_KEYS)):
        value = raw.get(block)
        if not isinstance(value, dict):
            continue
        unknown = sorted((key for key in value if key not in known), key=repr)
        if unknown:
            raise DeploymentConfigError(
                f"deployment.yaml {block} block has unknown key(s): "
                + ", ".join(repr(key) for key in unknown)
                + f". Known keys: {', '.join(sorted(known))}. "
                f"(Manifest: {manifest_path})"
            )


def _substitute_manifest_placeholders(
    raw: Any, deployment_dir: Path,
) -> Any:
    """Recursively substitute stable deployment path anchors.

    The anchor table lives in ``deployments.sync_root`` so consumers that read
    a manifest WITHOUT this loader (``deployment_authority._read_manifest`` does
    a bare ``safe_load``) expand anchors identically instead of mistaking
    ``${repo_root}`` for an environment variable.
    """
    return _substitute_node(
        raw, manifest_path_anchors(deployment_dir), path="",
    )


def _substitute_node(
    node: Any, placeholders: dict[str, str], *, path: str,
) -> Any:
    if isinstance(node, str):
        # Substitute the manifest-relative anchor here. Any other
        # `${VAR}` placeholders are env-var references handled
        # downstream by the consumer (e.g. `cli/main.py`'s DSN
        # resolver `_interpolate_env`); leave them in place rather
        # than failing the parse.
        out = node
        for needle, replacement in placeholders.items():
            out = out.replace(needle, replacement)
        return out
    if isinstance(node, dict):
        return {
            k: _substitute_node(
                v, placeholders,
                path=f"{path}.{k}" if path else k,
            )
            for k, v in node.items()
        }
    if isinstance(node, list):
        return [
            _substitute_node(
                v, placeholders, path=f"{path}[{i}]",
            )
            for i, v in enumerate(node)
        ]
    return node


# ---------------------------------------------------------------------------
# Manifest path detection (for `--deployment` accepting a path)
# ---------------------------------------------------------------------------

def _looks_like_manifest_path(value: str) -> bool:
    """Return True if ``value`` should be treated as a filesystem
    path to a deployment manifest, rather than a bare deployment
    name.

    Accepted forms:
        - Absolute path ending in ``deployment.yaml``
        - Absolute path to a directory containing ``deployment.yaml``
        - Any path (absolute or relative) ending in ``.yaml`` /
          ``.yml``
        - A path with a separator that resolves to an existing
          directory with a ``deployment.yaml``
    """
    if not value:
        return False
    if value.endswith(".yaml") or value.endswith(".yml"):
        return True
    p = Path(value)
    if p.is_absolute():
        return True
    if os.sep in value or "/" in value:
        candidate = p / "deployment.yaml"
        if candidate.exists():
            return True
    return False


def _resolve_manifest_path(value: str) -> Path | None:
    """Resolve a manifest-path value to an existing
    ``deployment.yaml``. Returns None if no manifest exists.
    """
    p = Path(value).expanduser().resolve()
    if p.is_file():
        return p
    if p.is_dir():
        candidate = p / "deployment.yaml"
        if candidate.is_file():
            return candidate
    return None


# ---------------------------------------------------------------------------
# Discovery cascade
# ---------------------------------------------------------------------------

def _find_repo_root_from_cwd() -> Path | None:
    """Walk up from cwd looking for a ``pyproject.toml`` or
    ``.git`` sentinel.
    """
    cur = Path.cwd().resolve()
    for parent in [cur] + list(cur.parents):
        if (parent / "pyproject.toml").is_file():
            return parent
        if (parent / ".git").exists():
            return parent
    return None


# ---------------------------------------------------------------------------
# XDG paths (per `separate-framework-and-deployments` — 2026-05-22)
# ---------------------------------------------------------------------------


def _user_data_dir() -> Path:
    """Operator-state directory.

    `${OKG_USER_HOME:-${XDG_DATA_HOME:-$HOME/.local/share}/okg}`.
    The `OKG_USER_HOME` override exists for tests + CI that
    need to relocate the whole user-state tree to a tmp dir.
    """
    override = os.environ.get("OKG_USER_HOME")
    if override:
        return Path(override).expanduser()
    xdg = os.environ.get("XDG_DATA_HOME")
    base = Path(xdg).expanduser() if xdg else Path.home() / ".local/share"
    return base / "okg"


def _cache_dir() -> Path:
    """Cache directory for fetched reference deployments.

    `${OKG_CACHE_HOME:-${XDG_CACHE_HOME:-$HOME/.cache}/okg}`.
    Everything under here is regeneratable via
    `okg fetch-deployments` — wipe at any time.
    """
    override = os.environ.get("OKG_CACHE_HOME")
    if override:
        return Path(override).expanduser()
    xdg = os.environ.get("XDG_CACHE_HOME")
    base = Path(xdg).expanduser() if xdg else Path.home() / ".cache"
    return base / "okg"


def user_state_deployments_root() -> Path:
    """Where new operator deployments scaffold to.

    `okg install` SHALL write new deployments under this dir.
    Existing deployments may live elsewhere (in-repo, custom
    `OKG_DEPLOYMENTS_DIR`); `find_deployment_dir` walks all
    candidate roots.
    """
    return _user_data_dir() / "deployments"


def user_state_chat_instances_root() -> Path:
    """Where chat-instance operator records are stored.

    Chat runtime state is deliberately kept beside, not inside, the
    ``deployments/`` lookup root.  A record-only directory named after a
    deployment must never be able to shadow that deployment's manifest.
    ``OKG_USER_HOME`` and ``XDG_DATA_HOME`` follow the same test/operator
    override rules as :func:`user_state_deployments_root`.
    """
    return _user_data_dir() / "chat-instances"


def default_deployment_target(name: str) -> Path:
    """The scaffold target for a new operator deployment named
    `name`. Always under user state.
    """
    return user_state_deployments_root() / name


def _cached_deployments_root() -> Path:
    """Where `okg fetch-deployments` lands cached profiles.

    `${_cache_dir()}/deployments/`. Each subdir is one
    fetched repo (e.g. `mitdbg-okg-deployments/`).
    """
    return _cache_dir() / "deployments"


def find_deployments_root(
    *, deployment_dir_override: str | None = None,
) -> Optional[Path]:
    """Return the deployments root directory, or None if not found.

    Cascade:
      1. ``deployment_dir_override`` (caller-supplied)
      2. ``OKG_DEPLOYMENTS_DIR`` env var
      3. ``<repo_root>/deployments/`` via cwd-walk for a
         ``pyproject.toml`` / ``.git`` sentinel
      4. ``<okg_repo>/deployments/`` derived from this module's
         path (in-tree fallback)

    Steps 1 and 2 raise :class:`DeploymentResolutionError` if set
    but not pointing at an existing directory; they do NOT
    silently fall through. Steps 3 and 4 fall through if absent.
    """
    if deployment_dir_override:
        p = Path(deployment_dir_override).expanduser()
        if p.is_dir():
            return p
        raise DeploymentResolutionError(
            f"deployment_dir_override {str(p)!r} is not an "
            f"existing directory"
        )

    env_dir = os.environ.get("OKG_DEPLOYMENTS_DIR")
    if env_dir:
        p = Path(env_dir).expanduser()
        if p.is_dir():
            return p
        raise DeploymentResolutionError(
            f"OKG_DEPLOYMENTS_DIR={str(p)!r} is not an existing "
            f"directory"
        )

    repo_root = _find_repo_root_from_cwd()
    if repo_root is not None:
        candidate = repo_root / "deployments"
        if candidate.is_dir():
            return candidate

    here = Path(__file__).resolve()
    in_tree = here.parents[4] / "deployments"
    if in_tree.is_dir():
        return in_tree

    return None


def _deployment_search_roots(
    *, deployment_dir_override: str | None = None,
) -> tuple[Path, ...]:
    """Return deployment roots in the resolver's actual priority order.

    This is intentionally separate from :func:`find_deployments_root`, whose
    historical contract returns one root for auto-discovery.  Name-based
    lookup must inspect every root, and diagnostics must report those same
    roots rather than a different single-root projection.
    """
    if deployment_dir_override:
        p = Path(deployment_dir_override).expanduser()
        if not p.is_dir():
            raise DeploymentResolutionError(
                f"deployment_dir_override {str(p)!r} is not an "
                f"existing directory"
            )
        return (p,)

    roots: list[Path] = []
    env_dir = os.environ.get("OKG_DEPLOYMENTS_DIR")
    if env_dir:
        p = Path(env_dir).expanduser()
        if not p.is_dir():
            raise DeploymentResolutionError(
                f"OKG_DEPLOYMENTS_DIR={str(p)!r} is not an "
                f"existing directory"
            )
        roots.append(p)

    roots.append(user_state_deployments_root())

    repo_root = _find_repo_root_from_cwd()
    if repo_root is not None:
        roots.append(repo_root / "deployments")

    here = Path(__file__).resolve()
    roots.append(here.parents[4] / "deployments")

    # Preserve priority while avoiding duplicate roots when the cwd is the
    # same checkout this module was imported from.
    unique: list[Path] = []
    seen: set[Path] = set()
    for root in roots:
        resolved = root.resolve()
        if resolved in seen or not resolved.is_dir():
            continue
        seen.add(resolved)
        unique.append(resolved)
    return tuple(unique)


def _is_regular_manifest(path: Path) -> bool:
    """Accept only a regular manifest file, never a symlink target."""
    try:
        return stat.S_ISREG(path.lstat().st_mode)
    except OSError:
        return False


# ---------------------------------------------------------------------------
# Folder-resolved deployments stay resolved (issue #2951)
# ---------------------------------------------------------------------------

# A command given `--deployment <folder>` resolves the manifest once and then
# works under the manifest's name. Code further down (runtime routing, publish
# provenance, the source runner's privacy posture, ...) looks the deployment
# up again by that name, and the name lookup only searches the deployments
# roots, so a deployment outside them was "not found" halfway through the
# command. Inside `deployment_binding_scope()`, `bind_resolved_deployment`
# records the folder the command already resolved; name lookups in the same
# command return that folder. Nothing is written to disk and the binding ends
# with the scope, so the lookup order for every other name is unchanged.
#
# The table is a context variable, not a module global: two commands running
# on different threads of one process each see only their own bindings, and
# one finishing cannot drop the other's. Work a command hands to a thread pool
# sees the binding only when it runs in a copied context
# (`contextvars.copy_context().run`), as the ingest source fan-out does.
_RESOLVED_DEPLOYMENT_BINDINGS: contextvars.ContextVar[
    Optional[dict[str, Path]]
] = contextvars.ContextVar("okg_resolved_deployment_bindings", default=None)


@contextmanager
def deployment_binding_scope() -> Iterator[None]:
    """Bound the lifetime of `bind_resolved_deployment` calls to one command."""
    token = _RESOLVED_DEPLOYMENT_BINDINGS.set(
        dict(_RESOLVED_DEPLOYMENT_BINDINGS.get() or {})
    )
    try:
        yield
    finally:
        _RESOLVED_DEPLOYMENT_BINDINGS.reset(token)


def bind_resolved_deployment(name: str, deployment_dir: Path) -> bool:
    """Make `name` resolve to the folder this command already resolved.

    Returns False, and binds nothing, in two cases. Outside a
    `deployment_binding_scope()` (a caller using the resolver directly, not a
    command): there is no command for the binding to end with, so name
    lookups keep their ordinary behaviour. And when the folder holds no
    regular `deployment.yaml`: a manifest composed in memory (a sealed
    environment) has no folder a name lookup could return, and its own
    resolution path already answers lookups of its name.
    """
    bindings = _RESOLVED_DEPLOYMENT_BINDINGS.get()
    if bindings is None:
        return False
    folder = Path(deployment_dir).resolve()
    if not _is_regular_manifest(folder / "deployment.yaml"):
        return False
    _RESOLVED_DEPLOYMENT_BINDINGS.set({**bindings, name: folder})
    return True


def _bound_deployment_dir(name: str | None) -> Optional[Path]:
    if not name:
        return None
    return (_RESOLVED_DEPLOYMENT_BINDINGS.get() or {}).get(name)


def canonical_deployment_reference(ref: str) -> tuple[str, Optional[Path]]:
    """Turn a `--deployment` value into the deployment's durable name.

    A command may be given a folder or a `deployment.yaml` path instead of a
    name. Everything the command then writes -- migrations, catalog ownership,
    source progress, admission envelopes, generations, publish authority --
    is keyed by the manifest's ``name``. Passing the raw path down gives the
    deployment a second identity: rows land under the path, and the publish
    refuses because its catalog identity carries the manifest name
    (``catalog_writer_scope_conflict``).

    A path-shaped ``ref`` is resolved through `resolve_deployment` and
    returned as ``(manifest.name, manifest.deployment_dir)``; inside a
    `deployment_binding_scope()` the name is also bound to that folder, so
    later lookups by name reach the same folder even when it sits outside the
    deployments roots. A path that names no manifest raises
    `DeploymentResolutionError` rather than passing the path on. A plain name
    is returned unchanged with ``None``: the caller resolves it as before.
    """
    if not _looks_like_manifest_path(ref):
        return ref, None
    manifest = resolve_deployment(ref)
    deployment_dir = Path(manifest.deployment_dir)
    if not bind_resolved_deployment(manifest.name, deployment_dir):
        # Later lookups by name would reach a different, same-named folder
        # (or none): refuse rather than drift. Binding needs a command scope
        # and a regular `deployment.yaml` in the folder.
        raise DeploymentResolutionError(
            f"cannot use {ref!r} as --deployment here: the deployment name "
            f"{manifest.name!r} can only be tied to a folder that holds a "
            "regular deployment.yaml; pass that folder (or rename the "
            "manifest to deployment.yaml)"
        )
    return manifest.name, deployment_dir


def deployment_name_lookup(
    name: str, deployment_dir: Path | str,
) -> dict[str, Any]:
    """Say whether a later `--deployment <name>` would reach `deployment_dir`.

    Uses the same roots, in the same order, as `find_deployment_dir`. The
    folder need not hold a manifest yet (bootstrap plans it before drafting
    one). Returns ``{"resolves": bool, "searched": [...], "found": path|None}``
    where ``found`` is the other folder the name reaches today, if any.
    """
    target = Path(deployment_dir).expanduser().resolve()
    try:
        roots = _deployment_search_roots()
    except DeploymentResolutionError as exc:
        return {"resolves": False, "searched": [], "found": None, "error": str(exc)}
    searched: list[str] = []
    for root in roots:
        for candidate_name in _deployment_name_candidates(name):
            candidate = (root / candidate_name).resolve()
            searched.append(str(candidate))
            if candidate == target:
                return {"resolves": True, "searched": searched, "found": str(target)}
            if candidate.is_dir() and _is_regular_manifest(candidate / "deployment.yaml"):
                return {"resolves": False, "searched": searched, "found": str(candidate)}
    return {"resolves": False, "searched": searched, "found": None}


def find_deployment_dir(
    name: str, *, deployment_dir_override: str | None = None,
) -> Optional[Path]:
    """Return ``<root>/<name>/`` from the first root that
    contains it, else None.

    Per `separate-framework-and-deployments` (2026-05-22): the
    search walks multiple candidate roots in priority order:

      1. Explicit `deployment_dir_override` (caller).
      2. `OKG_DEPLOYMENTS_DIR` env var (tests + CI).
      3. User-state XDG dir
         (`${XDG_DATA_HOME:-$HOME/.local/share}/okg/deployments/`).
      4. Repo-walked `<repo>/deployments/` (in-repo deployments
         like `okg-workspace`).
      5. In-tree fallback from this module's path.

    Returns the first root where `<root>/<name>/deployment.yaml`
    exists as a regular file. A record-only directory is not a
    deployment and is skipped so it cannot shadow a lower-priority
    valid root. None if not found anywhere. Callers decide whether
    absent is fatal.
    """
    from okg.substrate.distributions.packaged_release import verified_deployment_directory
    packaged_directory = verified_deployment_directory(name)
    if packaged_directory is not None:
        return packaged_directory
    active_environment = _active_release_environment(name)
    if active_environment is not None:
        return active_environment.manifest.deployment_dir
    bound = None if deployment_dir_override else _bound_deployment_dir(name)
    if bound is not None:
        return bound

    for root in _deployment_search_roots(
        deployment_dir_override=deployment_dir_override,
    ):
        for candidate_name in _deployment_name_candidates(name):
            candidate = root / candidate_name
            if candidate.is_dir() and _is_regular_manifest(candidate / "deployment.yaml"):
                return candidate

    return None


# ---------------------------------------------------------------------------
# sys.path injection (idempotent)
# ---------------------------------------------------------------------------

_DEPLOYMENT_LOCAL_MODULE_PREFIXES = (
    "agent_tools",
    "attestors",
    "checkpoint_attestors",
    "enrichers",
    "sources",
    "views",
    "views_demo",
    "workers",
)


# Process-wide lock for the evict + inject + deployment-module first-import
# sequence (make-migrate-idempotent, design ruling 5). Deployment-owned modules
# import under shared top-level names (`sources`, `enrichers`, ...) while
# `inject_deployment_path` mutates `sys.path` and `evict_deployment_modules`
# pops `sys.modules`. Python's per-module import lock does not cover that
# evict+inject+first-import window, and the ingest layer fans imports across a
# thread pool, so two threads can race a deployment module through a
# half-mutated `sys.path`/`sys.modules`. Every path that evicts, injects, or
# first-imports a deployment module holds this lock; the ingest fan-out
# additionally pre-imports sequentially in the main thread (see
# `preimport_source_modules`) so worker threads only ever hit the module cache.
_DEPLOYMENT_MODULE_LOCK = threading.RLock()


def deployment_module_import_lock() -> threading.RLock:
    """The process-wide lock guarding deployment-module load side effects."""
    return _DEPLOYMENT_MODULE_LOCK


def preimport_source_modules(module_names: "Iterable[str]") -> None:
    """Import each deployment source module once, sequentially, under the lock.

    Called from the MAIN thread during ingest layer setup, before the source
    fan-out spawns. Importing is a process-global side effect (it mutates
    `sys.modules` and runs package `__init__`), so it must not happen inside the
    thread pool. After this returns, every named module is in `sys.modules`, so
    the worker threads' ``importlib.import_module`` calls are pure cache hits
    and cannot race the evict/inject/first-import window.
    """
    with _DEPLOYMENT_MODULE_LOCK:
        for module_name in module_names:
            if not module_name:
                continue
            importlib.import_module(module_name)


def _module_origins(module: Any) -> list[Path]:
    origins: list[Path] = []
    file_attr = getattr(module, "__file__", None)
    if file_attr:
        origins.append(Path(file_attr).resolve())
    path_attr = getattr(module, "__path__", None)
    if path_attr:
        origins.extend(Path(raw).resolve() for raw in path_attr)
    return origins


def _module_belongs_to_deployment(
    module: Any,
    deployment_dir: Path,
) -> bool:
    dep_dir = deployment_dir.resolve()
    for origin in _module_origins(module):
        if origin == dep_dir or dep_dir in origin.parents:
            return True
    return False


def evict_deployment_modules(
    deployment_dir: Path,
    *,
    include_current: bool = False,
    prefixes: set[str] | None = None,
) -> None:
    """Evict deployment-local modules from ``sys.modules``.

    By default this only removes modules rooted under another deployment
    directory. Callers that reload editable deployment code in a
    long-lived process can pass ``include_current=True`` to force the
    next import to read the current deployment files from disk.
    """

    requested_prefixes = (
        _DEPLOYMENT_LOCAL_MODULE_PREFIXES
        if prefixes is None
        else tuple(
            prefix
            for prefix in _DEPLOYMENT_LOCAL_MODULE_PREFIXES
            if prefix in prefixes
        )
    )
    stale_prefixes = {
        prefix
        for prefix in requested_prefixes
        if (
            (module := sys.modules.get(prefix)) is not None
            and (
                include_current
                or not _module_belongs_to_deployment(module, deployment_dir)
            )
        )
    }
    if not stale_prefixes:
        return
    for name in list(sys.modules):
        if name in stale_prefixes or any(
            name.startswith(f"{prefix}.") for prefix in stale_prefixes
        ):
            sys.modules.pop(name, None)


def _evict_stale_deployment_modules(deployment_dir: Path) -> None:
    evict_deployment_modules(deployment_dir, include_current=False)


def inject_deployment_path(deployment_dir: Path) -> None:
    """Add ``deployment_dir`` AND its parent (the ``deployments/``
    root) to ``sys.path`` so both module path styles resolve:

      * ``workflows.X`` — relative, used by source_registry.yaml /
        enricher_registry.yaml / agent_tools entries that pre-date
        the SDK.
      * ``<deployment_name>.workflows.X`` — package-style, used by
        `runtime.workflows: [...]` declarations in SDK-aware
        manifests (Phase 2 §7).

    Idempotent — duplicates are skipped.

    Adding the parent depends on ``<deployment_dir>/__init__.py``
    existing so Python treats the deployment as a package. Every
    deployment under ``deployments/`` has one by convention; if a
    new deployment is added without it, the package-style imports
    silently fall back to the relative style and the operator sees
    a clear ImportError naming the missing module path.
    """
    dep_dir = Path(deployment_dir).resolve()
    dir_str = str(dep_dir)
    parent_str = str(dep_dir.parent)
    # Serialize the sys.path mutation + module eviction so a concurrent import
    # never observes a half-mutated path or a partially-evicted module set
    # (make-migrate-idempotent, design ruling 5).
    with _DEPLOYMENT_MODULE_LOCK:
        sys.path[:] = [
            entry for entry in sys.path
            if entry not in {dir_str, parent_str}
        ]
        sys.path.insert(0, dir_str)
        sys.path.insert(1, parent_str)
        _evict_stale_deployment_modules(dep_dir)


# ---------------------------------------------------------------------------
# Full resolution: discovery + manifest + sys.path
# ---------------------------------------------------------------------------

def resolve_deployment(
    name: str | None = None,
    *,
    deployment_dir_override: str | None = None,
) -> DeploymentManifest:
    """Resolve a deployment by name (or auto-detect) and return
    the parsed manifest with ``sys.path`` already injected.

    Parameters
    ----------
    name : str | None
        Explicit deployment name. May also be an absolute path to
        a ``deployment.yaml`` (or a directory containing one) for
        downstream repos that consume okg as a submodule and place
        their manifest outside the okg tree. If None, falls back
        to the ``OKG_DEPLOYMENT`` env var (canonical), then the
        deprecated ``PZ_OKG_DEPLOYMENT`` alias (which emits a
        ``DeprecationWarning``), then auto-detection (exactly one
        deployment in the resolved root).
    deployment_dir_override : str | None
        Explicit deployments root directory.

    Returns
    -------
    DeploymentManifest
        Fully-resolved manifest. ``sys.path[0]`` is the
        deployment directory by the time this returns.

    Raises
    ------
    DeploymentResolutionError
        If the deployment cannot be resolved, or an explicit
        override / env var points at a non-existent directory.
    """
    from okg.substrate.distributions.packaged_release import verified_deployment_directory
    packaged_directory = verified_deployment_directory(name)
    if packaged_directory is not None:
        return DeploymentManifest.from_yaml(packaged_directory / "deployment.yaml")
    if not name:
        name = os.environ.get("OKG_DEPLOYMENT")
    if not name:
        # Deprecated alias: every other reader uses OKG_DEPLOYMENT. Keep
        # PZ_OKG_DEPLOYMENT working but warn once so callers migrate.
        legacy = os.environ.get("PZ_OKG_DEPLOYMENT")
        if legacy:
            import warnings

            warnings.warn(
                "PZ_OKG_DEPLOYMENT is deprecated; set OKG_DEPLOYMENT instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            name = legacy

    active_environment = _active_release_environment(name)
    if active_environment is not None:
        manifest = active_environment.manifest
        inject_deployment_path(manifest.deployment_dir)
        return manifest

    # Path-shaped name: caller passed an explicit manifest path.
    if name and _looks_like_manifest_path(name):
        manifest_path = _resolve_manifest_path(name)
        if manifest_path is None:
            raise DeploymentResolutionError(
                f"Deployment manifest not found at path {name!r}"
            )
        manifest = DeploymentManifest.from_yaml(manifest_path)
        inject_deployment_path(manifest.deployment_dir)
        return manifest

    if not name:
        # Preserve the established auto-detect contract: an explicit
        # override/env root is authoritative, while named lookup below uses
        # the broader priority cascade.
        deployments_dir = find_deployments_root(
            deployment_dir_override=deployment_dir_override,
        )
        if deployments_dir is None:
            raise DeploymentResolutionError(
                "No deployments directory found. Set "
                "OKG_DEPLOYMENTS_DIR, run from inside a repo with a "
                "deployments/ sibling, or pass an explicit override."
            )
        candidates = [
            d / "deployment.yaml"
            for d in deployments_dir.iterdir()
            if d.is_dir() and _is_regular_manifest(d / "deployment.yaml")
        ]
        if len(candidates) == 1:
            manifest = DeploymentManifest.from_yaml(candidates[0])
            inject_deployment_path(manifest.deployment_dir)
            return manifest
        if len(candidates) == 0:
            raise DeploymentResolutionError(
                f"No deployments found in {deployments_dir}. "
                f"Create one with: okg-ingest init <name>"
            )
        names = sorted(c.parent.name for c in candidates)
        raise DeploymentResolutionError(
            f"Multiple deployments found: {names}. Specify with "
            f"--deployment or OKG_DEPLOYMENT."
        )

    bound = None if deployment_dir_override else _bound_deployment_dir(name)
    if bound is not None:
        manifest = DeploymentManifest.from_yaml(bound / "deployment.yaml")
        inject_deployment_path(manifest.deployment_dir)
        return manifest

    search_roots = _deployment_search_roots(
        deployment_dir_override=deployment_dir_override,
    )
    if not search_roots:
        raise DeploymentResolutionError(
            "No deployments directory found. Set "
            "OKG_DEPLOYMENTS_DIR, run from inside a repo with a "
            "deployments/ sibling, or pass an explicit override."
        )

    if name:
        deployment_dir = find_deployment_dir(
            name,
            deployment_dir_override=deployment_dir_override,
        )
        if deployment_dir is not None:
            manifest_path = deployment_dir / "deployment.yaml"
            if _is_regular_manifest(manifest_path):
                manifest = DeploymentManifest.from_yaml(manifest_path)
                inject_deployment_path(manifest.deployment_dir)
                return manifest
        searched = [
            root / candidate_name / "deployment.yaml"
            for root in search_roots
            for candidate_name in _deployment_name_candidates(name)
        ]
        searched_paths = ", ".join(str(path) for path in searched)
        raise DeploymentResolutionError(
            f"Deployment {name!r} not found. Searched: {searched_paths}"
        )



def active_effective_manifest_for_path(path: Path | str) -> dict[str, Any] | None:
    """Return the composed manifest when *path* names the active sealed manifest."""
    if not os.environ.get("OKG_ENVIRONMENT_DESCRIPTOR"):
        return None
    manifest = resolve_deployment(os.environ.get("OKG_DEPLOYMENT"))
    expected = manifest.deployment_dir / "deployment.yaml"
    if Path(path).resolve(strict=False) != expected.resolve(strict=False):
        return None
    return dict(manifest.effective_raw)
