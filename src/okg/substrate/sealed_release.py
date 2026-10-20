"""Build-once, content-addressed deployment release artifacts."""

from __future__ import annotations

import hashlib
import json
import os
import posixpath
import re
import resource
import select
import shutil
import signal
import stat
import subprocess
import tempfile
import threading
import time
import ctypes
from contextlib import contextmanager
from dataclasses import dataclass, field as dataclass_field, replace
from datetime import datetime, timezone
from pathlib import Path
from pathlib import PurePosixPath
from types import MappingProxyType
from typing import Any, Callable, Iterator, Mapping, Sequence

import fcntl

from okg.substrate.catalog.module_provenance import (
    BUILTIN_KIND,
    BUILTIN_OWNER,
    ReleaseModuleProvenanceMismatch,
    module_provenance_entries,
    module_provenance_from_composed,
)
from okg.substrate._sealed_release_integrity import (
    executable_tree_digest as _executable_tree_digest,
    is_bytecode_relative_name as _is_bytecode_relative_name,
    private_pycache_prefix as _private_pycache_prefix,
    remove_artifact_tree as _remove_artifact_tree,
    remove_runtime_caches as _remove_runtime_caches,
    seal_permissions as _seal_permissions,
    sealed_bytecode_refusal as _sealed_bytecode_refusal,
    tree_digest,
    validate_contained_symlinks as _validate_contained_symlinks,
)
from okg.substrate._sealed_release_runtime import (
    RUNTIME_BUILD_RECIPE_DIGEST,
    RUNTIME_CONSTRUCTION_DOCUMENT_SCHEMA,
    RUNTIME_CONSTRUCTION_FILENAME,
    RUNTIME_CONSTRUCTION_SCHEMA,
    RUNTIME_DISTRIBUTION_ENROLLMENT,
    RUNTIME_DISTRIBUTION_PREFIX_TREE_SCHEMA,
    RUNTIME_PREFIX_COPY_PROOF_SCHEMA,
    RUNTIME_PREFIX_COPY_STATUS,
    RUNTIME_RELEASE_ADDITIONS_SCHEMA,
    RUNTIME_RELOCATION_VERIFIER_MODULE,
    RUNTIME_RELOCATION_VERIFIER_SOURCE,
    _verify_authorized_runtime_distribution,
    runtime_prefix_copy_entries,
    sync_dependencies as _sync_dependencies,
)
from okg.substrate.deployments.environment import (
    DeploymentEnvironment,
    load_environment,
)
from okg.substrate.errors import SealedReleaseInvalid


SEALED_RELEASE_SCHEMA = "okg.sealed_release/v1"
SEALED_RELEASE_RUNTIME_AUTHORITY_SCHEMA = "okg.sealed_release.runtime_authority/v2"
SEALED_RELEASE_RUNTIME_DESCRIPTOR_PROOF_SCHEMA = (
    "okg.sealed_release.runtime_descriptor_startup_proof/v3"
)
_RELEASE_FILENAME = "release.json"
_PREPARING_FILENAME = ".preparing"
_INPUTS_DIRNAME = ".inputs"
_LOCKS_DIRNAME = ".locks"
_LOCK_FILENAMES = frozenset(
    {
        "Cargo.lock",
        "npm-shrinkwrap.json",
        "package-lock.json",
        "pnpm-lock.yaml",
        "poetry.lock",
        "uv.lock",
        "yarn.lock",
    }
)
_DEPLOYMENT_SNAPSHOT_DIR = ".okg-deployment-repository"


@dataclass(frozen=True)
class SealedReleaseInputs:
    code_revision: str
    dependency_lock_digest: str
    schema_checksum: str
    catalog_fingerprint: str
    definition_digest: str
    skill_bundle_digest: str | None
    postgres_capacity_digest: str | None = None
    deployment_revision: str | None = None
    deployment_tree_sha: str | None = None
    deployment_layout: str | None = None
    deployment_repo_path: str | None = None
    # Provider provenance for every non-built-in module the catalog was
    # composed from (#1179 slice 6): owner, provider kind, package release
    # digest, object digests. Empty for a release built from the in-tree
    # library alone, and omitted from the payload when empty, so such a
    # release keeps exactly the input identity it had before this field
    # existed.
    module_provenance: tuple[Mapping[str, Any], ...] = ()

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "code_revision": self.code_revision,
            "dependency_lock_digest": self.dependency_lock_digest,
            "schema_checksum": self.schema_checksum,
            "catalog_fingerprint": self.catalog_fingerprint,
            "definition_digest": self.definition_digest,
            "skill_bundle_digest": self.skill_bundle_digest,
            "deployment_revision": self.deployment_revision,
            "deployment_tree_sha": self.deployment_tree_sha,
            "deployment_layout": self.deployment_layout,
            "deployment_repo_path": self.deployment_repo_path,
        }
        if self.postgres_capacity_digest is not None:
            payload["postgres_capacity_digest"] = self.postgres_capacity_digest
        if self.module_provenance:
            payload["module_provenance"] = [
                dict(entry) for entry in self.module_provenance
            ]
        return payload

    @property
    def input_id(self) -> str:
        return _canonical_digest(self.to_dict())


def _sealed_release_inputs_hash(inputs: SealedReleaseInputs) -> int:
    """Hash by the input identity, which is the digest of the payload.

    A frozen dataclass hashes its fields, and `module_provenance` holds
    dicts, so the generated hash would raise on a release sealed from a
    package. The canonical digest is the identity every caller already
    uses, so hashing by it keeps hash and equality agreeing.
    """
    return hash(inputs.input_id)


SealedReleaseInputs.__hash__ = _sealed_release_inputs_hash  # type: ignore[assignment]


@dataclass(frozen=True)
class SealedRelease:
    artifact_id: str
    artifact_root: Path
    inputs: SealedReleaseInputs
    executable_tree_digest: str
    prepared_at: str
    reused: bool = False

    @property
    def repo_root(self) -> Path:
        return self.artifact_root / "repo"

    @property
    def deployment_repo_root(self) -> Path:
        path = self.inputs.deployment_repo_path or "."
        return (self.repo_root / path).resolve()

    @property
    def venv_root(self) -> Path:
        return self.artifact_root / "venv"

    @property
    def interpreter(self) -> Path:
        return self.venv_root / "bin" / "python3"

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": SEALED_RELEASE_SCHEMA,
            "artifact_id": self.artifact_id,
            "artifact_root": str(self.artifact_root),
            "input_id": self.inputs.input_id,
            "inputs": self.inputs.to_dict(),
            "executable_tree_digest": self.executable_tree_digest,
            "layout": {
                "repo_root": "repo",
                "venv_root": "venv",
                "interpreter": "venv/bin/python3",
            },
            "prepared_at": self.prepared_at,
            "reused": self.reused,
        }

    def extension_runtime_authority(self) -> "VerifiedRelocatableRuntimeAuthority":
        """Project a retained, path-free relocatable-runtime authority."""

        return _verified_relocatable_runtime_authority(self)


@dataclass(frozen=True, slots=True)
class RuntimeAuthorityMember:
    relative_name: str
    mode: int
    size: int
    raw_blob_digest: str

    def to_record(self) -> dict[str, object]:
        return {
            "relative_name": self.relative_name,
            "mode": self.mode,
            "size": self.size,
            "raw_blob": {
                "digest_domain": "raw-bytes/v1",
                "digest": self.raw_blob_digest,
                "size": self.size,
            },
        }


@dataclass(frozen=True, slots=True)
class VerifiedRelocatableRuntimeAuthority:
    """Verified release bytes retained by descriptor without exposing a locator."""

    _receipt: bytes = dataclass_field(repr=False)
    receipt_digest: str
    sealed_release_artifact_id: str
    sealed_release_tree_digest: str
    platform_identity: Mapping[str, str]
    python_abi: str
    _members: Mapping[str, RuntimeAuthorityMember] = dataclass_field(repr=False)
    _root_fd: int = dataclass_field(repr=False)
    _root_identity: tuple[int, int, int, int, int] = dataclass_field(repr=False)
    _closed: bool = dataclass_field(default=False, init=False, repr=False)
    _lock: threading.RLock = dataclass_field(
        default_factory=threading.RLock,
        init=False,
        repr=False,
    )

    @property
    def receipt(self) -> dict[str, object]:
        value = json.loads(self._receipt.decode("utf-8"))
        if not isinstance(value, dict):
            raise _runtime_invalid(
                "sealed_release_runtime_closure_mismatch",
                "runtime authority receipt is not a JSON object",
            )
        return value

    @property
    def members(self) -> tuple[RuntimeAuthorityMember, ...]:
        return tuple(self._members[name] for name in sorted(self._members))

    def member(self, relative_name: str) -> RuntimeAuthorityMember:
        safe = _runtime_relative_name(relative_name)
        try:
            return self._members[safe]
        except KeyError as exc:
            raise _runtime_invalid(
                "sealed_release_runtime_closure_mismatch",
                "requested release member is outside the verified tree",
            ) from exc

    def read_verified_member(
        self,
        relative_name: str,
        expected_raw_blob_digest: str,
    ) -> bytes:
        with self._lock:
            self._assert_open()
            member = self.member(relative_name)
            if member.raw_blob_digest != expected_raw_blob_digest:
                raise _runtime_invalid(
                    "sealed_release_runtime_closure_mismatch",
                    "release member identity differs from the requested identity",
                )
            payload, observed = _read_runtime_member_at(
                self._root_fd, member.relative_name
            )
            if observed != member or _raw_digest(payload) != member.raw_blob_digest:
                raise _runtime_invalid(
                    "sealed_release_runtime_closure_mismatch",
                    "release member changed after authority projection",
                )
            return payload

    def _duplicate_root_fd(self) -> int:
        with self._lock:
            self._assert_open()
            observed = os.fstat(self._root_fd)
            identity = (
                observed.st_dev,
                observed.st_ino,
                observed.st_mode,
                observed.st_mtime_ns,
                observed.st_ctime_ns,
            )
            if identity != self._root_identity:
                raise _runtime_invalid(
                    "sealed_release_runtime_closure_mismatch",
                    "retained release root changed",
                )
            return int(fcntl.fcntl(self._root_fd, fcntl.F_DUPFD_CLOEXEC, 64))

    def _assert_open(self) -> None:
        if self._closed or self._root_fd < 0:
            raise _runtime_invalid(
                "sealed_release_runtime_closure_mismatch",
                "release runtime authority is closed",
            )

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            root_fd = self._root_fd
            object.__setattr__(self, "_root_fd", -1)
            object.__setattr__(self, "_closed", True)
            try:
                os.close(root_fd)
            except OSError:
                # Ownership is already retired; retry could close a reused fd.
                pass

    def __enter__(self) -> "VerifiedRelocatableRuntimeAuthority":
        self._assert_open()
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def __del__(self) -> None:  # pragma: no cover
        if hasattr(self, "_lock"):
            self.close()


ReleaseInputResolver = Callable[[Path, str], SealedReleaseInputs]
DependencySync = Callable[[Path, Path, Mapping[str, str]], None]


def prepare_sealed_release(
    *,
    source_repo: str | Path,
    revision: str,
    artifact_store: str | Path,
    input_resolver: ReleaseInputResolver,
    dependency_sync: DependencySync | None = None,
    expected_inputs: SealedReleaseInputs | None = None,
    environ: Mapping[str, str] | None = None,
    deployment_source_repo: str | Path | None = None,
    deployment_revision: str | None = None,
) -> SealedRelease:
    """Prepare an exact commit once, then verify/reuse by input identity."""
    source = Path(source_repo).expanduser().resolve()
    store = Path(artifact_store).expanduser().resolve()
    if not (source / ".git").exists():
        raise SealedReleaseInvalid(
            f"source repository has no .git authority: {source}",
            data={"artifact_root": None, "command": "git rev-parse"},
        )
    store.mkdir(parents=True, exist_ok=True)
    commit = _git_output(source, "rev-parse", f"{revision}^{{commit}}")
    split_deployment = deployment_source_repo is not None
    deployment_source = (
        Path(deployment_source_repo).expanduser().resolve()
        if split_deployment
        else source
    )
    if not (deployment_source / ".git").exists():
        raise SealedReleaseInvalid(
            f"deployment repository has no .git authority: {deployment_source}",
        )
    deployment_commit = _git_output(
        deployment_source,
        "rev-parse",
        f"{deployment_revision or revision}^{{commit}}",
    )

    worktree_parent = Path(tempfile.mkdtemp(prefix=".sealed-worktree-", dir=store))
    worktree = worktree_parent / "repo"
    worktree_added = False
    deployment_worktree = worktree_parent / "deployment"
    deployment_worktree_added = False
    preparing_root: Path | None = None
    try:
        _git(source, "worktree", "add", "--detach", str(worktree), commit)
        worktree_added = True
        observed_commit = _git_output(worktree, "rev-parse", "HEAD")
        if observed_commit != commit:
            raise SealedReleaseInvalid(
                "detached release worktree resolved the wrong revision",
                data={"expected": commit, "observed": observed_commit},
            )
        dirty = _git_output(worktree, "status", "--porcelain")
        if dirty:
            raise SealedReleaseInvalid(
                "detached release worktree is not clean",
                data={"expected": "clean", "observed": dirty.splitlines()},
            )
        if split_deployment:
            snapshot_root = worktree / _DEPLOYMENT_SNAPSHOT_DIR
            if snapshot_root.exists():
                raise SealedReleaseInvalid(
                    "core repository collides with reserved deployment snapshot path",
                    data={"observed": str(snapshot_root)},
                )
            _git(
                deployment_source,
                "worktree",
                "add",
                "--detach",
                str(deployment_worktree),
                deployment_commit,
            )
            deployment_worktree_added = True
            observed_deployment_commit = _git_output(
                deployment_worktree, "rev-parse", "HEAD"
            )
            if observed_deployment_commit != deployment_commit:
                raise SealedReleaseInvalid(
                    "detached deployment worktree resolved the wrong revision",
                    data={
                        "expected": deployment_commit,
                        "observed": observed_deployment_commit,
                    },
                )
            deployment_dirty = _git_output(
                deployment_worktree, "status", "--porcelain"
            )
            if deployment_dirty:
                raise SealedReleaseInvalid(
                    "detached deployment worktree is not clean",
                    data={"observed": deployment_dirty.splitlines()},
                )
            shutil.copytree(
                deployment_worktree,
                snapshot_root,
                ignore=shutil.ignore_patterns(".git"),
                symlinks=True,
            )
        deployment_tree_sha = _git_output(
            deployment_worktree if split_deployment else worktree,
            "rev-parse",
            "HEAD^{tree}",
        )

        inputs = input_resolver(worktree, commit)
        _validate_inputs(
            inputs,
            expected_revision=commit,
            expected_deployment_revision=deployment_commit,
            expected_deployment_tree_sha=deployment_tree_sha,
            expected_deployment_layout="split" if split_deployment else "monorepo",
            expected_deployment_repo_path=(
                _DEPLOYMENT_SNAPSHOT_DIR if split_deployment else "."
            ),
        )
        if expected_inputs is not None and inputs != expected_inputs:
            raise SealedReleaseInvalid(
                "release inputs differ from the approved identity",
                data={
                    "expected": expected_inputs.to_dict(),
                    "observed": inputs.to_dict(),
                },
            )

        with _input_lock(store, inputs.input_id):
            reused = _load_indexed_release(store, inputs)
            if reused is not None:
                return reused

            preparing_root = Path(
                tempfile.mkdtemp(prefix=".preparing-", dir=store),
            )
            (preparing_root / _PREPARING_FILENAME).write_text(
                json.dumps({"input_id": inputs.input_id}) + "\n",
                encoding="utf-8",
            )
            shutil.copytree(
                worktree,
                preparing_root / "repo",
                ignore=shutil.ignore_patterns(".git"),
                symlinks=True,
            )
            _remove_worktree(source, worktree)
            worktree_added = False
            if deployment_worktree_added:
                _remove_worktree(deployment_source, deployment_worktree)
                deployment_worktree_added = False

            sync = dependency_sync or _sync_dependencies
            sync_env = dict(os.environ if environ is None else environ)
            sync_env.update(
                {
                    "PYTHONDONTWRITEBYTECODE": "1",
                    "UV_LINK_MODE": "copy",
                }
            )
            sync(
                preparing_root / "repo",
                preparing_root / "venv",
                sync_env,
            )
            _validate_contained_symlinks(preparing_root)
            for executable_root in (
                preparing_root / "repo",
                preparing_root / "venv",
            ):
                if not executable_root.is_dir():
                    raise SealedReleaseInvalid(
                        "dependency preparation omitted an executable tree",
                        data={"expected": str(executable_root)},
                    )
                _remove_runtime_caches(executable_root)
                _seal_permissions(executable_root)

            (preparing_root / _PREPARING_FILENAME).unlink()
            executable_tree_digest = _executable_tree_digest(preparing_root)
            artifact_id = _release_artifact_id(inputs, executable_tree_digest)
            release = SealedRelease(
                artifact_id=artifact_id,
                artifact_root=preparing_root,
                inputs=inputs,
                executable_tree_digest=executable_tree_digest,
                prepared_at=datetime.now(timezone.utc).isoformat(),
            )
            _write_release_metadata(release)

            final_root = store / artifact_id.removeprefix("sha256:")
            if final_root.exists():
                _remove_artifact_tree(preparing_root)
                preparing_root = None
                final_release = verify_sealed_release(
                    final_root,
                    expected_inputs=inputs,
                    expected_artifact_id=artifact_id,
                    reused=True,
                )
            else:
                os.replace(preparing_root, final_root)
                preparing_root = None
                _seal_permissions(final_root)
                final_release = SealedRelease(
                    artifact_id=artifact_id,
                    artifact_root=final_root,
                    inputs=inputs,
                    executable_tree_digest=executable_tree_digest,
                    prepared_at=release.prepared_at,
                )
            _write_input_index(store, final_release)
            return final_release
    except SealedReleaseInvalid:
        if preparing_root is not None:
            _remove_artifact_tree(preparing_root)
        raise
    except Exception as exc:
        if preparing_root is not None:
            _remove_artifact_tree(preparing_root)
        raise SealedReleaseInvalid(
            f"sealed release preparation failed: {type(exc).__name__}: {exc}",
            data={
                "artifact_root": (
                    str(preparing_root) if preparing_root is not None else None
                ),
                "command": "prepare_sealed_release",
            },
        ) from exc
    finally:
        if worktree_added:
            _remove_worktree(source, worktree)
        if deployment_worktree_added:
            _remove_worktree(deployment_source, deployment_worktree)
        shutil.rmtree(worktree_parent, ignore_errors=True)


class SealedReleaseRevisionMissing(SealedReleaseInvalid):
    """A sealed release was asked to record a null or empty code revision.

    A sealed release is identified by the exact Git commit it was built from.
    A missing revision (a standalone or packaged release artifact carries
    none) must refuse here rather than be stringified into the literal text
    ``"None"``, which would then read as a Git object id (#1983).
    """

    code = "sealed_release_revision_missing"


def _require_sealed_revision(value: object, *, field: str) -> str:
    """Return ``value`` as a revision string, or refuse a null or empty one."""
    if not isinstance(value, str) or not value.strip():
        raise SealedReleaseRevisionMissing(
            "sealed release requires an exact code revision",
            data={"field": field, "observed": value},
            fix_hint=(
                "seal a release only from a committed Git checkout; a "
                "standalone or packaged deployment records no code revision "
                "and cannot be sealed"
            ),
        )
    return value


def prepare_environment_release(
    *,
    environment: DeploymentEnvironment,
    source_repo: str | Path,
    revision: str,
    dependency_sync: DependencySync | None = None,
    environ: Mapping[str, str] | None = None,
    module_providers: Mapping[str, Any] | None = None,
) -> SealedRelease:
    """Prepare a release using definition/catalog inputs from the exact commit.

    `module_providers` is an owner-keyed
    :class:`~okg.substrate.catalog.module_providers.ModuleProvider` mapping,
    normally built from a deployment's install lock. Omitted, the catalog is
    composed from the release's own built-in ontology library exactly as
    before, and the sealed record carries no module provenance.
    """
    revision = _require_sealed_revision(revision, field="revision")
    source = Path(source_repo).expanduser().resolve()
    from okg.substrate.deployment_release import current_release_artifact
    from okg.substrate.deployment_upgrade import _resolve_repo_authorities

    pin = dict(environment.release.pin)
    explicit_code_repository = pin.get("code_repository")
    if explicit_code_repository is not None:
        authority_manifest = replace(environment.manifest, release={"pin": pin})
        release_artifact = current_release_artifact(authority_manifest)
        # Refuse before any further Git resolution: a null revision must never
        # be stringified into the text "None" and sealed as a commit id.
        core_revision = _require_sealed_revision(
            release_artifact.code_revision, field="code_revision"
        )
        code_root, deployment_root = _resolve_repo_authorities(
            Path(environment.manifest.deployment_dir),
            explicit_code_repository=str(explicit_code_repository),
            requested_code_revision=(
                str(pin["code_revision"]) if pin.get("code_revision") else None
            ),
        )
        if source != deployment_root:
            raise SealedReleaseInvalid(
                "sealed release source differs from deployment repository authority",
                data={"expected": str(deployment_root), "observed": str(source)},
            )
        split_layout = code_root != deployment_root
    else:
        code_root = source
        core_revision = revision
        split_layout = False
    deployment_commit = _git_output(source, "rev-parse", f"{revision}^{{commit}}")
    deployment_tree_sha = _git_output(source, "rev-parse", f"{revision}^{{tree}}")
    try:
        descriptor_relative = environment.descriptor_path.relative_to(source)
    except ValueError as exc:
        raise SealedReleaseInvalid(
            "environment descriptor is outside the source repository",
            data={
                "expected": str(source),
                "observed": str(environment.descriptor_path),
            },
        ) from exc

    values = dict(os.environ if environ is None else environ)
    values["OKG_SEALED_PYTHON_EXTRAS"] = json.dumps(
        list(environment.manifest.runtime.python_extras),
        separators=(",", ":"),
    )

    def resolve_inputs(candidate_repo: Path, commit: str) -> SealedReleaseInputs:
        candidate_deployment_root = (
            candidate_repo / _DEPLOYMENT_SNAPSHOT_DIR
            if split_layout
            else candidate_repo
        )
        candidate_environment = load_environment(
            candidate_deployment_root / descriptor_relative,
            environ=values,
        )
        validate_environment_release_paths(
            candidate_environment, candidate_deployment_root
        )
        candidate_pin = dict(candidate_environment.release.pin)
        if candidate_pin != pin:
            raise SealedReleaseInvalid(
                "staged deployment redirects framework repository authority"
            )
        for field, observed, expected in (
            (
                "descriptor_digest",
                candidate_environment.descriptor_digest,
                environment.descriptor_digest,
            ),
            (
                "definition_digest",
                candidate_environment.definition_digest,
                environment.definition_digest,
            ),
            (
                "postgres_capacity_digest",
                candidate_environment.postgres_capacity_digest,
                environment.postgres_capacity_digest,
            ),
        ):
            if observed != expected:
                raise SealedReleaseInvalid(
                    f"working descriptor/config differs from revision for {field}",
                    data={"expected": expected, "observed": observed},
                )

        from okg.substrate.catalog.modules_compose import (
            compose_catalog,
            providers_for_declared_deployment,
        )
        from okg.substrate.catalog.ownership import catalog_identity_for

        # The deployment's own `module_providers:` owners are added to the
        # base below, never substituted for it: sealing must compose the same
        # catalog the installed deployment composes (issue #1933).
        composed = compose_catalog(
            candidate_environment.manifest,
            providers=providers_for_declared_deployment(
                candidate_environment.manifest,
                **(
                    {"providers": dict(module_providers)}
                    if module_providers is not None
                    else {
                        "modules_root": (
                            candidate_repo / "src/okg/substrate/library/ontologies"
                        )
                    }
                ),
            ),
        )
        catalog = catalog_identity_for(
            composed=composed,
            deployment_dir=candidate_environment.manifest.deployment_dir,
            command="okg environment prepare-release",
        )
        skills = candidate_environment.manifest.skills_dir
        if skills is not None:
            try:
                skills.relative_to(candidate_deployment_root)
            except ValueError as exc:
                raise SealedReleaseInvalid(
                    "sealed release skills path is outside the candidate repository",
                    data={"observed": str(skills)},
                ) from exc
        return release_inputs_from_tree(
            candidate_repo,
            code_revision=core_revision,
            catalog_fingerprint=catalog.catalog_fingerprint,
            module_provenance=module_provenance_from_composed(composed),
            definition_digest=candidate_environment.definition_digest,
            skills_path=skills,
            postgres_capacity_digest=(candidate_environment.postgres_capacity_digest),
            deployment_revision=deployment_commit,
            deployment_tree_sha=deployment_tree_sha,
            deployment_layout="split" if split_layout else "monorepo",
            deployment_repo_path=(
                _DEPLOYMENT_SNAPSHOT_DIR if split_layout else "."
            ),
        )

    return prepare_sealed_release(
        source_repo=code_root,
        revision=core_revision,
        artifact_store=environment.release.artifact_store,
        input_resolver=resolve_inputs,
        dependency_sync=dependency_sync,
        environ=values,
        deployment_source_repo=source if split_layout else None,
        deployment_revision=deployment_commit if split_layout else None,
    )


def validate_environment_release_paths(
    environment: DeploymentEnvironment,
    repo_root: str | Path,
) -> None:
    """Require every definition-owned executable input to live in the bundle."""
    root = Path(repo_root).expanduser().resolve()
    manifest = environment.manifest
    paths = {
        "environment descriptor": environment.descriptor_path,
        "deployment definition": environment.definition_path,
        "definition root": manifest.deployment_dir,
        "schema directory": manifest.schema_dir,
        "source registry": manifest.source_registry,
        "enricher registry": manifest.enricher_registry,
        "taxonomy": manifest.taxonomy,
        "aliases": manifest.aliases,
        "MCP instructions": manifest.mcp_instructions,
        "skills directory": manifest.skills_dir,
        "audit questions": manifest.audit_questions,
        "tool description overrides": manifest.tool_description_overrides,
        "skill triggers": manifest.skill_triggers,
        "snapshot": manifest.snapshot_path,
        "viz asset": manifest.viz_path,
        "MCP pre-join tools": manifest.mcp_pre_join_tools,
        "checkpoint attestor root": manifest.checkpoint_attestor_root,
    }
    for label, path in paths.items():
        if path is None:
            continue
        candidate = Path(path).resolve()
        try:
            candidate.relative_to(root)
        except ValueError as exc:
            raise SealedReleaseInvalid(
                f"sealed release {label} is outside the candidate repository",
                data={"expected": str(root), "observed": str(candidate)},
            ) from exc


def release_inputs_from_tree(
    repo_root: str | Path,
    *,
    code_revision: str,
    catalog_fingerprint: str,
    definition_digest: str,
    module_provenance: Sequence[Mapping[str, Any]] = (),
    skills_path: str | Path | None = None,
    postgres_capacity_digest: str | None = None,
    deployment_revision: str | None = None,
    deployment_tree_sha: str | None = None,
    deployment_layout: str = "monorepo",
    deployment_repo_path: str = ".",
) -> SealedReleaseInputs:
    root = Path(repo_root).resolve()
    resolved_deployment_revision = deployment_revision or _git_output(
        root, "rev-parse", "HEAD"
    )
    resolved_deployment_tree_sha = deployment_tree_sha or _git_output(
        root, "rev-parse", "HEAD^{tree}"
    )
    return SealedReleaseInputs(
        code_revision=code_revision,
        dependency_lock_digest=dependency_lock_digest(root),
        schema_checksum=schema_manifest_digest(root),
        catalog_fingerprint=catalog_fingerprint,
        definition_digest=definition_digest,
        module_provenance=tuple(dict(entry) for entry in module_provenance),
        skill_bundle_digest=(
            tree_digest(Path(skills_path)) if skills_path is not None else None
        ),
        postgres_capacity_digest=postgres_capacity_digest,
        deployment_revision=resolved_deployment_revision,
        deployment_tree_sha=resolved_deployment_tree_sha,
        deployment_layout=deployment_layout,
        deployment_repo_path=deployment_repo_path,
    )


def dependency_lock_digest(repo_root: str | Path) -> str:
    root = Path(repo_root).resolve()
    lock_files = sorted(
        root / filename for filename in _LOCK_FILENAMES if (root / filename).is_file()
    )
    if not lock_files:
        raise SealedReleaseInvalid(
            "release repository has no supported dependency lock file",
            data={"observed": str(root), "expected": sorted(_LOCK_FILENAMES)},
        )
    return _paths_digest(root, lock_files)


def schema_manifest_digest(repo_root: str | Path) -> str:
    root = Path(repo_root).resolve()
    db_root = root / "src" / "okg" / "substrate" / "db"
    required = [db_root / "schema.sql", db_root / "views.sql"]
    missing = [str(path.relative_to(root)) for path in required if not path.is_file()]
    if missing:
        raise SealedReleaseInvalid(
            "release repository is missing schema snapshot inputs",
            data={"expected": ["schema.sql", "views.sql"], "observed": missing},
        )
    migrations = sorted(
        path for path in (db_root / "migrations").glob("*.sql") if path.is_file()
    )
    return _paths_digest(root, [*required, *migrations])


def load_sealed_release(artifact_root: str | Path) -> SealedRelease:
    root = Path(artifact_root).expanduser().resolve()
    metadata_path = root / _RELEASE_FILENAME
    if not metadata_path.is_file():
        raise SealedReleaseInvalid(
            "sealed release metadata is missing",
            data={"artifact_root": str(root), "expected": _RELEASE_FILENAME},
        )
    try:
        raw = json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise SealedReleaseInvalid(
            f"sealed release metadata is invalid: {exc}",
            data={"artifact_root": str(root)},
        ) from exc
    if not isinstance(raw, dict) or raw.get("schema") != SEALED_RELEASE_SCHEMA:
        raise SealedReleaseInvalid(
            "sealed release metadata has the wrong schema",
            data={"artifact_root": str(root), "observed": raw.get("schema")},
        )
    inputs_raw = raw.get("inputs")
    if not isinstance(inputs_raw, dict):
        raise SealedReleaseInvalid(
            "sealed release metadata has no input identity",
            data={"artifact_root": str(root)},
        )
    inputs = SealedReleaseInputs(
        code_revision=_required_metadata(inputs_raw, "code_revision", root),
        dependency_lock_digest=_required_metadata(
            inputs_raw,
            "dependency_lock_digest",
            root,
        ),
        schema_checksum=_required_metadata(inputs_raw, "schema_checksum", root),
        catalog_fingerprint=_required_metadata(
            inputs_raw,
            "catalog_fingerprint",
            root,
        ),
        definition_digest=_required_metadata(
            inputs_raw,
            "definition_digest",
            root,
        ),
        skill_bundle_digest=_optional_metadata(inputs_raw, "skill_bundle_digest"),
        postgres_capacity_digest=_optional_metadata(
            inputs_raw,
            "postgres_capacity_digest",
        ),
        deployment_revision=_optional_metadata(inputs_raw, "deployment_revision"),
        deployment_tree_sha=_optional_metadata(inputs_raw, "deployment_tree_sha"),
        deployment_layout=_optional_metadata(inputs_raw, "deployment_layout"),
        deployment_repo_path=_optional_metadata(inputs_raw, "deployment_repo_path"),
        module_provenance=module_provenance_entries(
            inputs_raw.get("module_provenance")
        ),
    )
    _validate_inputs(
        inputs,
        expected_revision=inputs.code_revision,
        expected_deployment_revision=inputs.deployment_revision,
        expected_deployment_tree_sha=inputs.deployment_tree_sha,
        expected_deployment_layout=inputs.deployment_layout,
        expected_deployment_repo_path=inputs.deployment_repo_path,
    )
    observed_input_id = _required_metadata(raw, "input_id", root)
    if observed_input_id != inputs.input_id:
        raise SealedReleaseInvalid(
            "sealed release input identity is inconsistent",
            data={"expected": inputs.input_id, "observed": observed_input_id},
        )
    layout = raw.get("layout")
    expected_layout = {
        "repo_root": "repo",
        "venv_root": "venv",
        "interpreter": "venv/bin/python3",
    }
    if layout != expected_layout:
        raise SealedReleaseInvalid(
            "sealed release layout is invalid",
            data={"expected": expected_layout, "observed": layout},
        )
    return SealedRelease(
        artifact_id=_required_metadata(raw, "artifact_id", root),
        artifact_root=root,
        inputs=inputs,
        executable_tree_digest=_required_metadata(
            raw,
            "executable_tree_digest",
            root,
        ),
        prepared_at=_required_metadata(raw, "prepared_at", root),
    )


def verify_sealed_release(
    artifact_root: str | Path,
    *,
    expected_inputs: SealedReleaseInputs | None = None,
    expected_artifact_id: str | None = None,
    reused: bool = False,
) -> SealedRelease:
    release = load_sealed_release(artifact_root)
    problems: list[dict[str, Any]] = []
    computed_artifact_id = _release_artifact_id(
        release.inputs,
        release.executable_tree_digest,
    )
    if release.artifact_id != computed_artifact_id:
        problems.append(
            {
                "field": "artifact_id",
                "expected": computed_artifact_id,
                "observed": release.artifact_id,
            }
        )
    if expected_artifact_id is not None and release.artifact_id != expected_artifact_id:
        problems.append(
            {
                "field": "expected_artifact_id",
                "expected": expected_artifact_id,
                "observed": release.artifact_id,
            }
        )
    if expected_inputs is not None and release.inputs != expected_inputs:
        problems.append(
            {
                "field": "inputs",
                "expected": expected_inputs.to_dict(),
                "observed": release.inputs.to_dict(),
            }
        )
    observed_tree = _executable_tree_digest(release.artifact_root)
    if observed_tree != release.executable_tree_digest:
        problems.append(
            {
                "field": "executable_tree_digest",
                "expected": release.executable_tree_digest,
                "observed": observed_tree,
            }
        )
    expected_dir = release.artifact_id.removeprefix("sha256:")
    if release.artifact_root.name != expected_dir:
        problems.append(
            {
                "field": "artifact_root",
                "expected": expected_dir,
                "observed": release.artifact_root.name,
            }
        )
    if problems:
        raise SealedReleaseInvalid(
            "sealed release identity or executable tree mismatch",
            data={
                "artifact_id": release.artifact_id,
                "artifact_root": str(release.artifact_root),
                "observed": problems,
            },
        )
    return SealedRelease(
        artifact_id=release.artifact_id,
        artifact_root=release.artifact_root,
        inputs=release.inputs,
        executable_tree_digest=release.executable_tree_digest,
        prepared_at=release.prepared_at,
        reused=reused,
    )


def _verified_relocatable_runtime_authority(
    release: SealedRelease,
) -> VerifiedRelocatableRuntimeAuthority:
    if not isinstance(release, SealedRelease):
        raise _runtime_invalid(
            "sealed_release_runtime_authority_missing",
            "runtime authority requires a verified sealed release",
        )
    verified = verify_sealed_release(
        release.artifact_root,
        expected_inputs=release.inputs,
        expected_artifact_id=release.artifact_id,
    )
    root_fd = _open_runtime_root(verified.artifact_root)
    try:
        root_stat = os.fstat(root_fd)
        root_identity = (
            root_stat.st_dev,
            root_stat.st_ino,
            root_stat.st_mode,
            root_stat.st_mtime_ns,
            root_stat.st_ctime_ns,
        )
        observed_tree, members, observed_runtime_records = _release_tree_snapshot_at(
            root_fd
        )
        if observed_tree != verified.executable_tree_digest:
            raise _runtime_invalid(
                "sealed_release_runtime_closure_mismatch",
                "retained release tree differs from its sealed identity",
            )
        construction_name = f"repo/{RUNTIME_CONSTRUCTION_FILENAME}"
        construction_member = members.get(construction_name)
        if construction_member is None:
            raise _runtime_invalid(
                "sealed_release_runtime_authority_missing",
                "sealed release has no runtime construction receipt",
            )
        construction_payload, observed_construction = _read_runtime_member_at(
            root_fd,
            construction_name,
        )
        if observed_construction != construction_member:
            raise _runtime_invalid(
                "sealed_release_runtime_closure_mismatch",
                "runtime construction receipt changed",
            )
        document = _strict_runtime_json(construction_payload)
        expected_document_keys = {
            "schema_version",
            "receipt",
            "receipt_digest",
            "runtime_members",
            "distribution_members",
            "distribution_proof",
            "prefix_copy_proof",
            "release_additions_manifest",
            "loader_graph",
            "loader_proof",
            "platform_loader_allow_list",
            "native_closure",
            "platform_identity",
            "python_abi",
        }
        if (
            set(document) != expected_document_keys
            or document.get("schema_version") != RUNTIME_CONSTRUCTION_DOCUMENT_SCHEMA
        ):
            raise _runtime_invalid(
                "sealed_release_runtime_authority_missing",
                "runtime construction document has the wrong schema",
            )
        receipt = _runtime_mapping(document.get("receipt"))
        if receipt.get(
            "schema_version"
        ) != RUNTIME_CONSTRUCTION_SCHEMA or _canonical_ref_digest(
            document.get("receipt_digest"), RUNTIME_CONSTRUCTION_SCHEMA
        ) != _runtime_canonical_digest(receipt):
            raise _runtime_invalid(
                "sealed_release_runtime_closure_mismatch",
                "runtime construction receipt digest differs",
            )
        runtime_member_values = document.get("runtime_members")
        if runtime_member_values != observed_runtime_records:
            raise _runtime_invalid(
                "sealed_release_runtime_closure_mismatch",
                "runtime complete prefix differs from the construction receipt",
            )
        runtime_members = _runtime_member_records(runtime_member_values)
        observed_runtime_members = {
            name.removeprefix("venv/"): RuntimeAuthorityMember(
                name.removeprefix("venv/"),
                member.mode,
                member.size,
                member.raw_blob_digest,
            )
            for name, member in members.items()
            if name.startswith("venv/")
        }
        if runtime_members != observed_runtime_members:
            raise _runtime_invalid(
                "sealed_release_runtime_closure_mismatch",
                "runtime prefix members differ from the construction receipt",
            )
        distribution_proof = _runtime_mapping(document.get("distribution_proof"))
        proof_profile = _runtime_string_mapping(
            distribution_proof.get("platform_profile")
        )
        verified_distribution = _verify_authorized_runtime_distribution(
            distribution_proof,
            None,
            expected_profile=proof_profile,
        )
        distribution_member_values = document.get("distribution_members")
        if not isinstance(
            distribution_member_values, list
        ) or distribution_member_values != list(
            verified_distribution.distribution_members
        ):
            raise _runtime_invalid(
                "sealed_release_runtime_closure_mismatch",
                "runtime distribution members differ from the signed proof",
            )
        distribution_prefix_record = {
            "schema_version": RUNTIME_DISTRIBUTION_PREFIX_TREE_SCHEMA,
            "entries": distribution_member_values,
        }
        distribution_prefix_digest = _runtime_canonical_digest(
            distribution_prefix_record
        )
        runtime_prefix_record = {
            "schema_version": "okg.sealed_release.runtime_prefix_tree/v1",
            "entries": runtime_member_values,
        }
        if receipt.get("runtime_prefix_tree_digest") != _runtime_canonical_digest(
            runtime_prefix_record
        ):
            raise _runtime_invalid(
                "sealed_release_runtime_closure_mismatch",
                "runtime prefix tree digest differs",
            )
        if not isinstance(runtime_member_values, list):
            raise _runtime_invalid(
                "sealed_release_runtime_closure_mismatch",
                "runtime prefix tree entries are invalid",
            )
        distribution_names = {
            str(_runtime_mapping(item).get("relative_name"))
            for item in distribution_member_values
        }
        additions = [
            item
            for item in runtime_member_values
            if str(_runtime_mapping(item).get("relative_name"))
            not in distribution_names
        ]
        additions_manifest = {
            "schema_version": RUNTIME_RELEASE_ADDITIONS_SCHEMA,
            "entries": additions,
        }
        if document.get("release_additions_manifest") != additions_manifest:
            raise _runtime_invalid(
                "sealed_release_runtime_closure_mismatch",
                "runtime release additions manifest differs",
            )
        copied_entries, excluded_bytecode_entries = runtime_prefix_copy_entries(
            [_runtime_mapping(value) for value in distribution_member_values]
        )
        prefix_copy_proof = {
            "schema_version": RUNTIME_PREFIX_COPY_PROOF_SCHEMA,
            "distribution_prefix_tree_digest": distribution_prefix_digest,
            "copied_entries": copied_entries,
            "excluded_bytecode_entries": excluded_bytecode_entries,
            "release_additions_manifest_digest": _runtime_canonical_digest(
                additions_manifest
            ),
            "runtime_prefix_tree_digest": _runtime_canonical_digest(
                runtime_prefix_record
            ),
            "status": RUNTIME_PREFIX_COPY_STATUS,
        }
        if (
            document.get("prefix_copy_proof") != prefix_copy_proof
            or receipt.get("prefix_copy_proof_digest")
            != _runtime_canonical_digest(prefix_copy_proof)
            or receipt.get("release_additions_manifest_digest")
            != _runtime_canonical_digest(additions_manifest)
            or receipt.get("distribution_prefix_tree_digest")
            != distribution_prefix_digest
            or receipt.get("distribution_enrollment_digest")
            != _runtime_canonical_digest(RUNTIME_DISTRIBUTION_ENROLLMENT)
            or receipt.get("source_manifest_digest")
            != verified_distribution.source_manifest_digest
            or receipt.get("build_recipe_digest") != RUNTIME_BUILD_RECIPE_DIGEST
            or receipt.get("reproducibility_proof_digest")
            != verified_distribution.reproducibility_proof_digest
            or receipt.get("distribution_acquisition_receipt_digest")
            != verified_distribution.distribution_acquisition_receipt_digest
            or receipt.get("distribution_archive")
            != verified_distribution.distribution_archive
            or not _is_sha256(receipt.get("dependency_wheelhouse_manifest_digest"))
        ):
            raise _runtime_invalid(
                "sealed_release_runtime_closure_mismatch",
                "runtime distribution or exact-copy proof differs",
            )
        loader_graph = _runtime_mapping(document.get("loader_graph"))
        loader_proof = _runtime_mapping(document.get("loader_proof"))
        allow_list = _runtime_mapping(document.get("platform_loader_allow_list"))
        native_closure = _runtime_mapping(document.get("native_closure"))
        if (
            loader_proof.get("loader_graph_digest")
            != _runtime_canonical_digest(loader_graph)
            or loader_proof.get("platform_loader_allow_list_digest")
            != _runtime_canonical_digest(allow_list)
            or receipt.get("loader_relocation_proof_digest")
            != _runtime_canonical_digest(loader_proof)
            or receipt.get("stdlib_native_closure_digest")
            != _runtime_canonical_digest(native_closure)
        ):
            raise _runtime_invalid(
                "sealed_release_runtime_closure_mismatch",
                "runtime native-closure proof differs",
            )
        _validate_loader_graph(loader_graph)
        platform_identity = _runtime_string_mapping(document.get("platform_identity"))
        if platform_identity != _runtime_string_mapping(
            loader_graph.get("platform_identity")
        ):
            raise _runtime_invalid(
                "sealed_release_runtime_closure_mismatch",
                "runtime platform identity differs",
            )
        if (
            platform_identity.get("os") != proof_profile.get("os")
            or platform_identity.get("architecture")
            != proof_profile.get("architecture")
            or platform_identity.get("libc") != proof_profile.get("libc")
            or platform_identity.get("kernel_contract")
            != proof_profile.get("kernel_contract")
            or receipt.get("platform_profile_digest")
            != _runtime_canonical_digest(proof_profile)
        ):
            raise _runtime_invalid(
                "sealed_release_runtime_distribution_unsupported",
                "runtime platform differs from the signed distribution profile",
            )
        python_abi = document.get("python_abi")
        if (
            not isinstance(python_abi, str)
            or not re.fullmatch(r"cp[0-9]{2,3}", python_abi)
            or python_abi != proof_profile.get("python_abi")
        ):
            raise _runtime_invalid(
                "sealed_release_runtime_closure_mismatch",
                "runtime Python ABI is invalid",
            )
        interpreter_record = _runtime_mapping(receipt.get("interpreter"))
        interpreter_name = interpreter_record.get("relative_name")
        if not isinstance(interpreter_name, str):
            raise _runtime_invalid(
                "sealed_release_runtime_authority_missing",
                "runtime receipt has no interpreter",
            )
        interpreter_member = runtime_members.get(interpreter_name)
        if (
            interpreter_member is None
            or _runtime_prefix_member_record(interpreter_member) != interpreter_record
        ):
            raise _runtime_invalid(
                "sealed_release_runtime_closure_mismatch",
                "runtime interpreter differs from the construction receipt",
            )
        prefix_configuration = _runtime_mapping(receipt.get("prefix_configuration"))
        if (
            prefix_configuration
            != {
                "profile": "standalone-base-prefix/v1",
                "pyvenv_config": None,
            }
            or "pyvenv.cfg" in runtime_members
        ):
            raise _runtime_invalid(
                "sealed_release_runtime_closure_mismatch",
                "runtime standalone base-prefix configuration differs",
            )
        verifier_name = _runtime_verifier_relative_name(python_abi)
        verifier_member = runtime_members.get(verifier_name)
        verifier_ref = _runtime_mapping(receipt.get("relocation_verifier_raw_blob"))
        if (
            verifier_member is None
            or verifier_member.raw_blob_digest != verifier_ref.get("digest")
            or verifier_member.size != verifier_ref.get("size")
            or verifier_member.raw_blob_digest
            != _raw_digest(RUNTIME_RELOCATION_VERIFIER_SOURCE)
        ):
            raise _runtime_invalid(
                "sealed_release_runtime_authority_missing",
                "runtime relocation verifier differs",
            )
        tk_native_names = _runtime_tk_native_members(loader_graph, runtime_members)
        descriptor_proof = _descriptor_startup_proof(
            root_fd,
            f"venv/{interpreter_name}",
            python_abi=python_abi,
            interpreter_member=interpreter_member,
            verifier_relative_name=f"venv/{verifier_name}",
            verifier_member=verifier_member,
            tk_native_members=tuple(
                (f"venv/{name}", runtime_members[name]) for name in tk_native_names
            ),
        )
        descriptor_proof_digest = _runtime_canonical_digest(descriptor_proof)
        public_receipt: dict[str, object] = {
            "schema_version": SEALED_RELEASE_RUNTIME_AUTHORITY_SCHEMA,
            "sealed_release": {
                "schema_version": SEALED_RELEASE_SCHEMA,
                "artifact_id": verified.artifact_id,
                "input_id": verified.inputs.input_id,
                "executable_tree_digest": verified.executable_tree_digest,
            },
            "construction_receipt_digest": _runtime_canonical_digest(receipt),
            "distribution_enrollment_digest": receipt["distribution_enrollment_digest"],
            "build_recipe_digest": receipt["build_recipe_digest"],
            "reproducibility_proof_digest": receipt["reproducibility_proof_digest"],
            "distribution_acquisition_receipt_digest": receipt[
                "distribution_acquisition_receipt_digest"
            ],
            "platform_identity": dict(platform_identity),
            "python_abi": python_abi,
            "distribution_archive": receipt["distribution_archive"],
            "distribution_prefix_tree_digest": receipt[
                "distribution_prefix_tree_digest"
            ],
            "prefix_copy_proof_digest": receipt["prefix_copy_proof_digest"],
            "runtime_prefix_tree_digest": receipt["runtime_prefix_tree_digest"],
            "interpreter": interpreter_record,
            "prefix_configuration": dict(prefix_configuration),
            "stdlib_native_closure_digest": receipt["stdlib_native_closure_digest"],
            "loader_relocation_proof_digest": receipt["loader_relocation_proof_digest"],
            "descriptor_startup_proof_digest": descriptor_proof_digest,
        }
        final_stat = os.fstat(root_fd)
        if (
            final_stat.st_dev,
            final_stat.st_ino,
            final_stat.st_mode,
            final_stat.st_mtime_ns,
            final_stat.st_ctime_ns,
        ) != root_identity:
            raise _runtime_invalid(
                "sealed_release_runtime_closure_mismatch",
                "release root changed during runtime projection",
            )
        public_receipt_bytes = _canonical_json_bytes(public_receipt)
        return VerifiedRelocatableRuntimeAuthority(
            public_receipt_bytes,
            f"sha256:{hashlib.sha256(public_receipt_bytes).hexdigest()}",
            verified.artifact_id,
            verified.executable_tree_digest,
            MappingProxyType(dict(platform_identity)),
            python_abi,
            MappingProxyType(members),
            root_fd,
            root_identity,
        )
    except BaseException:
        os.close(root_fd)
        raise


def _open_runtime_root(path: Path) -> int:
    if not path.is_absolute() or any(part == ".." for part in path.parts):
        raise _runtime_invalid(
            "sealed_release_runtime_authority_missing",
            "sealed release root is not absolute and normalized",
        )
    current = os.open(
        path.anchor,
        os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        for component in path.relative_to(path.anchor).parts:
            next_fd = os.open(
                component,
                os.O_RDONLY
                | os.O_DIRECTORY
                | os.O_CLOEXEC
                | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=current,
            )
            os.close(current)
            current = next_fd
        return current
    except OSError as exc:
        os.close(current)
        raise _runtime_invalid(
            "sealed_release_runtime_authority_missing",
            "sealed release root could not be retained without following links",
        ) from exc


def _release_tree_snapshot_at(
    root_fd: int,
) -> tuple[
    str,
    dict[str, RuntimeAuthorityMember],
    list[dict[str, object]],
]:
    digest_entries: dict[str, tuple[str, int, bytes]] = {}
    members: dict[str, RuntimeAuthorityMember] = {}
    root_device = os.fstat(root_fd).st_dev

    def walk(directory_fd: int, relative_root: str) -> None:
        try:
            names = sorted(os.listdir(directory_fd))
        except OSError as exc:
            raise _runtime_invalid(
                "sealed_release_runtime_closure_mismatch",
                "release directory could not be enumerated",
            ) from exc
        for name in names:
            relative_name = f"{relative_root}/{name}"
            if _is_bytecode_relative_name(
                relative_name
            ) and not _runtime_live_tree_name(relative_name):
                raise _sealed_bytecode_refusal([relative_name])
            if _ignored_runtime_tree_name(relative_name):
                continue
            metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            mode = stat.S_IMODE(metadata.st_mode)
            if stat.S_ISLNK(metadata.st_mode):
                target = os.readlink(name, dir_fd=directory_fd).encode("utf-8")
                digest_entries[relative_name] = ("symlink", mode, target)
                continue
            if stat.S_ISDIR(metadata.st_mode):
                digest_entries[relative_name] = ("directory", mode, b"")
                child_fd = os.open(
                    name,
                    os.O_RDONLY
                    | os.O_DIRECTORY
                    | os.O_CLOEXEC
                    | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=directory_fd,
                )
                try:
                    if os.fstat(child_fd).st_dev != root_device:
                        raise _runtime_invalid(
                            "sealed_release_runtime_closure_mismatch",
                            "release tree crosses a mount boundary",
                        )
                    walk(child_fd, relative_name)
                finally:
                    os.close(child_fd)
                continue
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
                raise _runtime_invalid(
                    "sealed_release_runtime_closure_mismatch",
                    "release tree contains an unsupported member",
                )
            payload, member = _read_runtime_member_at(root_fd, relative_name)
            digest_entries[relative_name] = ("file", mode, payload)
            members[relative_name] = member

    for root_name in ("repo", "venv"):
        root_child_fd = os.open(
            root_name,
            os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=root_fd,
        )
        try:
            info = os.fstat(root_child_fd)
            if info.st_dev != root_device:
                raise _runtime_invalid(
                    "sealed_release_runtime_closure_mismatch",
                    "release tree crosses a mount boundary",
                )
            digest_entries[root_name] = (
                "directory",
                stat.S_IMODE(info.st_mode),
                b"",
            )
            walk(root_child_fd, root_name)
        finally:
            os.close(root_child_fd)
    digest = hashlib.sha256(b"okg-executable-tree-v1\0")
    for relative_name, (kind, mode, content) in sorted(digest_entries.items()):
        digest.update(kind.encode("ascii"))
        digest.update(b"\0")
        digest.update(f"{mode:o}".encode("ascii"))
        digest.update(b"\0")
        digest.update(relative_name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(content)
        digest.update(b"\0")
    runtime_records: list[dict[str, object]] = []
    for release_name, (kind, mode, content) in sorted(digest_entries.items()):
        if not release_name.startswith("venv/"):
            continue
        relative_name = release_name.removeprefix("venv/")
        if kind == "directory":
            runtime_records.append(
                {
                    "relative_name": relative_name,
                    "kind": "directory",
                    "mode": mode,
                    "size": 0,
                    "raw_blob": None,
                }
            )
        elif kind == "symlink":
            runtime_records.append(
                {
                    "relative_name": relative_name,
                    "kind": "symlink",
                    "mode": mode,
                    "normalized_relative_target": content.decode("utf-8"),
                }
            )
        else:
            record = _runtime_prefix_member_record(members[release_name])
            record["relative_name"] = relative_name
            runtime_records.append(record)
    return f"sha256:{digest.hexdigest()}", members, runtime_records


def _ignored_runtime_tree_name(relative_name: str) -> bool:
    parts = PurePosixPath(relative_name).parts
    if any(
        part in {".git", ".mypy_cache", ".pytest_cache", ".ruff_cache", "__pycache__"}
        for part in parts
    ) or relative_name.endswith((".pyc", ".pyo")):
        return True
    return _runtime_live_tree_name(relative_name)


def _runtime_live_tree_name(relative_name: str) -> bool:
    parts = PurePosixPath(relative_name).parts
    if len(parts) < 2 or parts[:2] != ("repo", "profiles"):
        return False
    if len(parts) <= 3:
        return False
    return parts[2] != "_live" and parts[3] == "_live"


def _read_runtime_member_at(
    root_fd: int,
    relative_name: str,
) -> tuple[bytes, RuntimeAuthorityMember]:
    safe = _runtime_relative_name(relative_name)
    parts = PurePosixPath(safe).parts
    current = os.dup(root_fd)
    try:
        for component in parts[:-1]:
            next_fd = os.open(
                component,
                os.O_RDONLY
                | os.O_DIRECTORY
                | os.O_CLOEXEC
                | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=current,
            )
            os.close(current)
            current = next_fd
        member_fd = os.open(
            parts[-1],
            os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=current,
        )
        try:
            before = os.fstat(member_fd)
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_nlink != 1
                or before.st_dev != os.fstat(root_fd).st_dev
            ):
                raise _runtime_invalid(
                    "sealed_release_runtime_closure_mismatch",
                    "release authority member is not one regular file",
                )
            chunks: list[bytes] = []
            while True:
                chunk = os.read(member_fd, 1024 * 1024)
                if not chunk:
                    break
                chunks.append(chunk)
            after = os.fstat(member_fd)
            if _runtime_stat_token(before) != _runtime_stat_token(after):
                raise _runtime_invalid(
                    "sealed_release_runtime_closure_mismatch",
                    "release authority member changed while read",
                )
            payload = b"".join(chunks)
            member = RuntimeAuthorityMember(
                safe,
                stat.S_IMODE(after.st_mode),
                len(payload),
                _raw_digest(payload),
            )
            return payload, member
        finally:
            os.close(member_fd)
    except OSError as exc:
        raise _runtime_invalid(
            "sealed_release_runtime_closure_mismatch",
            "release authority member could not be read without following links",
        ) from exc
    finally:
        os.close(current)


def _runtime_member_records(value: object) -> dict[str, RuntimeAuthorityMember]:
    if not isinstance(value, list):
        raise _runtime_invalid(
            "sealed_release_runtime_closure_mismatch",
            "runtime member list is invalid",
        )
    result: dict[str, RuntimeAuthorityMember] = {}
    observed_names: list[str] = []
    for item in value:
        raw = _runtime_mapping(item)
        kind = raw.get("kind")
        expected_keys = (
            {"relative_name", "kind", "mode", "normalized_relative_target"}
            if kind == "symlink"
            else {"relative_name", "kind", "mode", "size", "raw_blob"}
        )
        if set(raw) != expected_keys:
            raise _runtime_invalid(
                "sealed_release_runtime_closure_mismatch",
                "runtime member record is not closed",
            )
        name = _runtime_relative_name(raw.get("relative_name"))
        observed_names.append(name)
        if kind == "directory":
            if (
                raw.get("mode") != 0o555
                or raw.get("size") != 0
                or raw.get("raw_blob") is not None
            ):
                raise _runtime_invalid(
                    "sealed_release_runtime_closure_mismatch",
                    "runtime directory member identity is invalid",
                )
            continue
        if kind == "symlink":
            target = raw.get("normalized_relative_target")
            resolved = posixpath.normpath(
                posixpath.join(posixpath.dirname(name), str(target))
            )
            if (
                raw.get("mode") != 0o777
                or not isinstance(target, str)
                or not target
                or target.startswith("/")
                or "\\" in target
                or target != posixpath.normpath(target)
                or resolved == ".."
                or resolved.startswith("../")
            ):
                raise _runtime_invalid(
                    "sealed_release_runtime_non_relocatable",
                    "runtime symbolic-link member identity is invalid",
                )
            continue
        blob = _runtime_mapping(raw.get("raw_blob"))
        mode = raw.get("mode")
        size = raw.get("size")
        digest = blob.get("digest")
        if (
            raw.get("kind") != "file"
            or blob.get("digest_domain") != "raw-bytes/v1"
            or blob.get("size") != size
            or not isinstance(mode, int)
            or isinstance(mode, bool)
            or mode not in {0o444, 0o555}
            or not isinstance(size, int)
            or isinstance(size, bool)
            or size < 0
            or not isinstance(digest, str)
            or not _is_sha256(digest)
        ):
            raise _runtime_invalid(
                "sealed_release_runtime_closure_mismatch",
                "runtime member raw identity is invalid",
            )
        member = RuntimeAuthorityMember(
            name,
            mode,
            size,
            digest,
        )
        if name in result:
            raise _runtime_invalid(
                "sealed_release_runtime_closure_mismatch",
                "runtime member names are not unique",
            )
        result[name] = member
    if observed_names != sorted(
        set(observed_names), key=lambda item: item.encode("utf-8")
    ):
        raise _runtime_invalid(
            "sealed_release_runtime_closure_mismatch",
            "runtime member names are not sorted and unique",
        )
    return result


def _runtime_prefix_member_record(member: RuntimeAuthorityMember) -> dict[str, object]:
    return {
        "relative_name": member.relative_name,
        "kind": "file",
        "mode": member.mode,
        "size": member.size,
        "raw_blob": {
            "digest_domain": "raw-bytes/v1",
            "digest": member.raw_blob_digest,
            "size": member.size,
        },
    }


def _validate_loader_graph(loader_graph: Mapping[str, object]) -> None:
    objects = loader_graph.get("objects")
    if not isinstance(objects, list):
        raise _runtime_invalid(
            "sealed_release_runtime_closure_mismatch",
            "runtime loader graph is invalid",
        )
    for value in objects:
        item = _runtime_mapping(value)
        searches = item.get("relative_search_tokens")
        resolutions = item.get("resolutions")
        if not isinstance(searches, list) or not isinstance(resolutions, list):
            raise _runtime_invalid(
                "sealed_release_runtime_closure_mismatch",
                "runtime loader graph is invalid",
            )
        if any(
            not isinstance(token, str)
            or not token
            or token.startswith("/")
            or (not token.startswith(("$ORIGIN", "@loader_path", "@executable_path")))
            for token in searches
        ):
            raise _runtime_invalid(
                "sealed_release_runtime_non_relocatable",
                "runtime loader graph contains host search authority",
            )
        for resolution_value in resolutions:
            resolution = _runtime_mapping(resolution_value).get("resolution")
            if not isinstance(resolution, str) or resolution.startswith("/"):
                raise _runtime_invalid(
                    "sealed_release_runtime_non_relocatable",
                    "runtime loader graph contains a host dependency locator",
                )


def _is_tk_library(basename: str) -> bool:
    """Tk 8 ships libtk8.6.so; Tk 9 ships libtcl9tk9.0.so."""

    return basename.startswith(("libtk", "libtcl9tk"))


def _runtime_tk_native_members(
    loader_graph: Mapping[str, object],
    runtime_members: Mapping[str, RuntimeAuthorityMember],
) -> tuple[str, ...]:
    """Select the exact _tkinter, Tcl, and Tk files that startup must map."""

    values = loader_graph.get("objects")
    if not isinstance(values, list):
        raise _runtime_invalid(
            "sealed_release_runtime_closure_mismatch",
            "runtime loader graph is invalid",
        )
    selected: set[str] = set()
    for value in values:
        item = _runtime_mapping(value)
        relative_name = item.get("relative_name")
        if not isinstance(relative_name, str) or not PurePosixPath(
            relative_name
        ).name.startswith("_tkinter"):
            continue
        selected.add(relative_name)
        resolutions = item.get("resolutions")
        if not isinstance(resolutions, list):
            raise _runtime_invalid(
                "sealed_release_runtime_closure_mismatch",
                "runtime _tkinter loader graph is invalid",
            )
        for resolution_value in resolutions:
            resolution = _runtime_mapping(resolution_value)
            dependency = resolution.get("dependency")
            target = resolution.get("resolution")
            if not isinstance(dependency, str) or not isinstance(target, str):
                raise _runtime_invalid(
                    "sealed_release_runtime_closure_mismatch",
                    "runtime _tkinter dependency is invalid",
                )
            if PurePosixPath(dependency).name.startswith(("libtcl", "libtk")):
                selected.add(target)
    names = tuple(sorted(selected, key=lambda value: value.encode("utf-8")))
    basenames = tuple(PurePosixPath(value).name for value in names)
    if (
        not any(value.startswith("_tkinter") for value in basenames)
        or not any(
            value.startswith("libtcl") and not _is_tk_library(value)
            for value in basenames
        )
        or not any(_is_tk_library(value) for value in basenames)
        or any(name not in runtime_members for name in names)
    ):
        raise _runtime_invalid(
            "sealed_release_runtime_closure_mismatch",
            "runtime Tcl/Tk native closure is incomplete",
        )
    return names


def _descriptor_startup_proof(
    root_fd: int,
    interpreter_relative_name: str,
    *,
    python_abi: str,
    interpreter_member: RuntimeAuthorityMember,
    verifier_relative_name: str,
    verifier_member: RuntimeAuthorityMember,
    tk_native_members: tuple[tuple[str, RuntimeAuthorityMember], ...],
) -> Mapping[str, object]:
    libc = ctypes.CDLL(None, use_errno=True)
    if not hasattr(libc, "fexecve") and not hasattr(libc, "execveat"):
        raise _runtime_invalid(
            "sealed_release_runtime_descriptor_exec_unsupported",
            "platform has no admitted descriptor execution syscall",
        )
    interpreter_fd = _open_runtime_regular_fd(root_fd, interpreter_relative_name)
    verifier_fd = -1
    native_handles: list[
        tuple[
            str,
            int,
            RuntimeAuthorityMember,
            tuple[int, int, int, int, int, int, int],
        ]
    ] = []
    try:
        interpreter_token = _verify_runtime_member_fd(
            root_fd, interpreter_relative_name, interpreter_fd, interpreter_member
        )
        verifier_fd = _open_runtime_regular_fd(root_fd, verifier_relative_name)
        verifier_token = _verify_runtime_member_fd(
            root_fd, verifier_relative_name, verifier_fd, verifier_member
        )
        for relative_name, member in tk_native_members:
            member_fd = _open_runtime_regular_fd(root_fd, relative_name)
            try:
                token = _verify_runtime_member_fd(
                    root_fd, relative_name, member_fd, member
                )
            except BaseException:
                os.close(member_fd)
                raise
            native_handles.append((relative_name, member_fd, member, token))
    except BaseException:
        for _name, member_fd, _member, _token in native_handles:
            os.close(member_fd)
        if verifier_fd >= 0:
            os.close(verifier_fd)
        os.close(interpreter_fd)
        raise
    request_read, request_write = os.pipe()
    result_read, result_write = os.pipe()
    acknowledgement_read, acknowledgement_write = os.pipe()
    sources: list[int] = []
    pid = -1
    reaped = False
    with (
        tempfile.TemporaryDirectory(prefix="okg-release-empty-cwd-") as cwd_name,
        _private_pycache_prefix(outside_fds=(root_fd,)) as pycache_flags,
    ):
        cwd_fd = _open_runtime_root(Path(cwd_name).resolve())
        try:
            root_source = int(fcntl.fcntl(root_fd, fcntl.F_DUPFD_CLOEXEC, 64))
            interpreter_source = int(
                fcntl.fcntl(interpreter_fd, fcntl.F_DUPFD_CLOEXEC, 64)
            )
            request_source = int(fcntl.fcntl(request_read, fcntl.F_DUPFD_CLOEXEC, 64))
            result_source = int(fcntl.fcntl(result_write, fcntl.F_DUPFD_CLOEXEC, 64))
            cwd_source = int(fcntl.fcntl(cwd_fd, fcntl.F_DUPFD_CLOEXEC, 64))
            acknowledgement_source = int(
                fcntl.fcntl(acknowledgement_read, fcntl.F_DUPFD_CLOEXEC, 64)
            )
            verifier_source = int(fcntl.fcntl(verifier_fd, fcntl.F_DUPFD_CLOEXEC, 64))
            native_sources = [
                int(fcntl.fcntl(member_fd, fcntl.F_DUPFD_CLOEXEC, 64))
                for _name, member_fd, _member, _token in native_handles
            ]
            sources.extend(
                [
                    root_source,
                    interpreter_source,
                    request_source,
                    result_source,
                    cwd_source,
                    acknowledgement_source,
                    verifier_source,
                    *native_sources,
                ]
            )
            pid = os.fork()
            if pid == 0:  # pragma: no cover - observed through the parent proof
                try:
                    targets = {
                        request_source: 3,
                        root_source: 4,
                        result_source: 5,
                        interpreter_source: 6,
                        cwd_source: 7,
                        acknowledgement_source: 8,
                        verifier_source: 9,
                    }
                    targets.update(
                        {
                            source: 10 + index
                            for index, source in enumerate(native_sources)
                        }
                    )
                    for source, target in targets.items():
                        if source != target:
                            os.dup2(source, target, inheritable=True)
                    keep = [
                        3,
                        4,
                        5,
                        6,
                        7,
                        8,
                        9,
                        *(10 + index for index in range(len(native_sources))),
                    ]
                    maximum = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
                    if maximum == resource.RLIM_INFINITY:
                        maximum = 65536
                    cursor = 0
                    for kept in keep:
                        if cursor < kept:
                            os.closerange(cursor, kept)
                        cursor = kept + 1
                    os.closerange(cursor, int(maximum))
                    os.fchdir(7)
                    os.close(7)
                    # argv[0] names the executed descriptor so CPython finds
                    # the copied prefix, not its build-time prefix (#2352).
                    _runtime_exec_fd(
                        6,
                        [
                            "/proc/self/fd/6",
                            "-I",
                            "-s",
                            "-E",
                            "-B",
                            *pycache_flags,
                            "/proc/self/fd/9",
                        ],
                        {
                            "LANG": "C.UTF-8",
                            "LC_ALL": "C.UTF-8",
                            "PYTHONDONTWRITEBYTECODE": "1",
                        },
                    )
                except Exception:
                    os._exit(127)
            for source in sources:
                os.close(source)
            sources.clear()
            os.close(request_read)
            request_read = -1
            os.close(acknowledgement_read)
            acknowledgement_read = -1
            request_payload = "\n".join(
                (
                    interpreter_relative_name,
                    verifier_relative_name,
                    *(name for name, _member in tk_native_members),
                )
            ).encode("utf-8")
            request_view = memoryview(request_payload)
            while request_view:
                written = os.write(request_write, request_view)
                if written <= 0:
                    raise OSError("short descriptor-startup request write")
                request_view = request_view[written:]
            os.close(request_write)
            request_write = -1
            os.close(result_write)
            result_write = -1
            os.set_blocking(result_read, False)
            deadline = time.monotonic() + 15.0
            chunks: list[bytes] = []
            status: int | None = None
            result_eof = False
            while not result_eof:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    os.kill(pid, signal.SIGKILL)
                    os.waitpid(pid, 0)
                    reaped = True
                    raise _runtime_invalid(
                        "sealed_release_runtime_non_relocatable",
                        "runtime descriptor startup timed out",
                    )
                readable, _, _ = select.select(
                    [result_read], [], [], min(remaining, 0.1)
                )
                if readable:
                    chunk = os.read(result_read, 65536)
                    if chunk:
                        chunks.append(chunk)
                        if sum(len(item) for item in chunks) > 1024 * 1024:
                            os.kill(pid, signal.SIGKILL)
                            os.waitpid(pid, 0)
                            reaped = True
                            raise _runtime_invalid(
                                "sealed_release_runtime_non_relocatable",
                                "runtime descriptor startup proof is too large",
                            )
                    else:
                        result_eof = True
                waited, raw_status = os.waitpid(pid, os.WNOHANG)
                if waited == pid:
                    status = raw_status
                    reaped = True
                    try:
                        while True:
                            chunk = os.read(result_read, 65536)
                            if not chunk:
                                break
                            chunks.append(chunk)
                    except BlockingIOError:
                        pass
                    result_eof = True
            if status is None:
                try:
                    _verify_runtime_member_fd(
                        root_fd,
                        interpreter_relative_name,
                        interpreter_fd,
                        interpreter_member,
                        expected_token=interpreter_token,
                    )
                    _verify_runtime_member_fd(
                        root_fd,
                        verifier_relative_name,
                        verifier_fd,
                        verifier_member,
                        expected_token=verifier_token,
                    )
                    for relative_name, member_fd, member, token in native_handles:
                        _verify_runtime_member_fd(
                            root_fd,
                            relative_name,
                            member_fd,
                            member,
                            expected_token=token,
                        )
                except BaseException:
                    os.kill(pid, signal.SIGKILL)
                    os.waitpid(pid, 0)
                    reaped = True
                    raise
                os.write(acknowledgement_write, b"1")
                os.close(acknowledgement_write)
                acknowledgement_write = -1
                while True:
                    waited, raw_status = os.waitpid(pid, os.WNOHANG)
                    if waited == pid:
                        status = raw_status
                        reaped = True
                        break
                    if time.monotonic() >= deadline:
                        os.kill(pid, signal.SIGKILL)
                        os.waitpid(pid, 0)
                        reaped = True
                        raise _runtime_invalid(
                            "sealed_release_runtime_non_relocatable",
                            "runtime descriptor startup did not exit after verification",
                        )
                    time.sleep(0.01)
            _verify_runtime_member_fd(
                root_fd,
                interpreter_relative_name,
                interpreter_fd,
                interpreter_member,
                expected_token=interpreter_token,
            )
            _verify_runtime_member_fd(
                root_fd,
                verifier_relative_name,
                verifier_fd,
                verifier_member,
                expected_token=verifier_token,
            )
            for relative_name, member_fd, member, token in native_handles:
                _verify_runtime_member_fd(
                    root_fd,
                    relative_name,
                    member_fd,
                    member,
                    expected_token=token,
                )
            if (
                status is None
                or not os.WIFEXITED(status)
                or os.WEXITSTATUS(status) != 0
            ):
                raise _runtime_invalid(
                    "sealed_release_runtime_non_relocatable",
                    "runtime refused descriptor startup",
                )
            proof = _descriptor_startup_wire_proof(
                b"".join(chunks),
                tk_native_members=tuple(name for name, _member in tk_native_members),
            )
            expected_keys = {
                "schema_version",
                "python_abi",
                "isolated",
                "ignore_environment",
                "no_user_site",
                "dont_write_bytecode",
                "executable_descriptor_matches_member",
                "verifier_descriptor_matches_member",
                "base_prefix_inside_prefix",
                "module_origins_inside_prefix",
                "cwd_empty",
                "tkinter_imported",
                "tk_native_members_match",
                "tk_native_members",
                "inherited_fds",
                "builtin_modules",
                "frozen_modules",
            }
            if (
                set(proof) != expected_keys
                or proof.get("schema_version")
                != SEALED_RELEASE_RUNTIME_DESCRIPTOR_PROOF_SCHEMA
                or proof.get("python_abi") != python_abi
                or any(
                    proof.get(field) is not True
                    for field in (
                        "isolated",
                        "ignore_environment",
                        "no_user_site",
                        "dont_write_bytecode",
                        "executable_descriptor_matches_member",
                        "verifier_descriptor_matches_member",
                        "base_prefix_inside_prefix",
                        "module_origins_inside_prefix",
                        "cwd_empty",
                        "tkinter_imported",
                        "tk_native_members_match",
                    )
                )
                or proof.get("inherited_fds")
                != [
                    3,
                    4,
                    5,
                    6,
                    8,
                    9,
                    *(10 + index for index in range(len(native_handles))),
                ]
                or proof.get("tk_native_members")
                != [name for name, _member in tk_native_members]
            ):
                raise _runtime_invalid(
                    "sealed_release_runtime_non_relocatable",
                    "runtime descriptor startup proof is invalid",
                )
            return proof
        finally:
            if pid > 0 and not reaped:
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                try:
                    os.waitpid(pid, 0)
                except ChildProcessError:
                    pass
            os.close(interpreter_fd)
            os.close(verifier_fd)
            for _name, member_fd, _member, _token in native_handles:
                os.close(member_fd)
            os.close(cwd_fd)
            for fd in sources:
                try:
                    os.close(fd)
                except OSError:
                    # Teardown must preserve the primary startup refusal if a
                    # partial setup path already retired this duplicate.
                    pass
            for fd in (
                request_read,
                request_write,
                result_read,
                result_write,
                acknowledgement_read,
                acknowledgement_write,
            ):
                if fd >= 0:
                    try:
                        os.close(fd)
                    except OSError:
                        # Pipe cleanup is best-effort after the proof outcome;
                        # an already-retired endpoint must not replace it.
                        pass


def _verify_runtime_member_fd(
    root_fd: int,
    relative_name: str,
    member_fd: int,
    expected: RuntimeAuthorityMember,
    *,
    expected_token: tuple[int, int, int, int, int, int, int] | None = None,
) -> tuple[int, int, int, int, int, int, int]:
    """Rebind an open executable/script to its exact sealed member and bytes."""

    current_fd = _open_runtime_regular_fd(root_fd, relative_name)
    try:
        current = os.fstat(current_fd)
        before = os.fstat(member_fd)
        chunks: list[bytes] = []
        offset = 0
        while True:
            chunk = os.pread(member_fd, 1024 * 1024, offset)
            if not chunk:
                break
            chunks.append(chunk)
            offset += len(chunk)
        after = os.fstat(member_fd)
        payload = b"".join(chunks)
        before_token = _runtime_stat_token(before)
        if (
            (current.st_dev, current.st_ino) != (before.st_dev, before.st_ino)
            or before_token != _runtime_stat_token(after)
            or (expected_token is not None and before_token != expected_token)
            or not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or stat.S_IMODE(before.st_mode) != expected.mode
            or before.st_size != expected.size
            or len(payload) != expected.size
            or _raw_digest(payload) != expected.raw_blob_digest
        ):
            raise _runtime_invalid(
                "sealed_release_runtime_closure_mismatch",
                "runtime startup member changed from its sealed authority",
            )
        return before_token
    finally:
        os.close(current_fd)


def _descriptor_startup_wire_proof(
    payload: bytes, *, tk_native_members: tuple[str, ...]
) -> Mapping[str, object]:
    try:
        fields = payload.decode("utf-8").split("\n")
    except UnicodeDecodeError as exc:
        raise _runtime_invalid(
            "sealed_release_runtime_closure_mismatch",
            "runtime descriptor startup wire is invalid",
        ) from exc
    if len(fields) != 17 or fields[0] != (
        "okg.sealed_release.runtime_descriptor_startup_wire/v1"
    ):
        raise _runtime_invalid(
            "sealed_release_runtime_closure_mismatch",
            "runtime descriptor startup wire is invalid",
        )

    def flag(index: int) -> bool:
        if fields[index] not in {"0", "1"}:
            raise _runtime_invalid(
                "sealed_release_runtime_closure_mismatch",
                "runtime descriptor startup flag is invalid",
            )
        return fields[index] == "1"

    def names(index: int) -> list[str]:
        values = [] if not fields[index] else fields[index].split("\0")
        if any(
            not value or "\x00" in value or "\n" in value for value in values
        ) or values != sorted(set(values), key=lambda value: value.encode("utf-8")):
            raise _runtime_invalid(
                "sealed_release_runtime_closure_mismatch",
                "runtime descriptor startup names are invalid",
            )
        return values

    try:
        inherited_fds = (
            [] if not fields[13] else [int(value) for value in fields[13].split(",")]
        )
    except ValueError as exc:
        raise _runtime_invalid(
            "sealed_release_runtime_closure_mismatch",
            "runtime descriptor startup descriptors are invalid",
        ) from exc
    if inherited_fds != sorted(set(inherited_fds)) or any(
        value < 0 for value in inherited_fds
    ):
        raise _runtime_invalid(
            "sealed_release_runtime_closure_mismatch",
            "runtime descriptor startup descriptors are invalid",
        )
    loaded_native = names(16)
    if loaded_native != list(tk_native_members):
        raise _runtime_invalid(
            "sealed_release_runtime_non_relocatable",
            "runtime startup did not map the exact Tcl/Tk closure",
        )
    return {
        "schema_version": SEALED_RELEASE_RUNTIME_DESCRIPTOR_PROOF_SCHEMA,
        "python_abi": fields[1],
        "isolated": flag(2),
        "ignore_environment": flag(3),
        "no_user_site": flag(4),
        "dont_write_bytecode": flag(5),
        "executable_descriptor_matches_member": flag(6),
        "verifier_descriptor_matches_member": flag(7),
        "base_prefix_inside_prefix": flag(8),
        "module_origins_inside_prefix": flag(9),
        "cwd_empty": flag(10),
        "tkinter_imported": flag(11),
        "tk_native_members_match": flag(12),
        "inherited_fds": inherited_fds,
        "builtin_modules": names(14),
        "frozen_modules": names(15),
        "tk_native_members": loaded_native,
    }


def _open_runtime_regular_fd(root_fd: int, relative_name: str) -> int:
    safe = _runtime_relative_name(relative_name)
    parts = PurePosixPath(safe).parts
    current = os.dup(root_fd)
    try:
        for component in parts[:-1]:
            next_fd = os.open(
                component,
                os.O_RDONLY
                | os.O_DIRECTORY
                | os.O_CLOEXEC
                | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=current,
            )
            os.close(current)
            current = next_fd
        fd = os.open(
            parts[-1],
            os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=current,
        )
        observed = os.fstat(fd)
        if not stat.S_ISREG(observed.st_mode) or observed.st_nlink != 1:
            os.close(fd)
            raise _runtime_invalid(
                "sealed_release_runtime_non_relocatable",
                "runtime interpreter is not one regular file",
            )
        return fd
    finally:
        os.close(current)


def _runtime_exec_fd(
    executable_fd: int,
    argv: list[str],
    environ: Mapping[str, str],
) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    encoded_argv = [item.encode("utf-8") for item in argv]
    encoded_env = [
        f"{key}={value}".encode("utf-8") for key, value in sorted(environ.items())
    ]
    argv_array = (ctypes.c_char_p * (len(encoded_argv) + 1))(*encoded_argv, None)
    env_array = (ctypes.c_char_p * (len(encoded_env) + 1))(*encoded_env, None)
    fexecve = getattr(libc, "fexecve", None)
    if fexecve is not None:
        fexecve(executable_fd, argv_array, env_array)
    execveat = getattr(libc, "execveat", None)
    if execveat is not None:
        execveat(executable_fd, ctypes.c_char_p(b""), argv_array, env_array, 0x1000)
    os._exit(126 if ctypes.get_errno() else 127)


def _runtime_verifier_relative_name(python_abi: str) -> str:
    match = re.fullmatch(r"cp(?P<major>[0-9])(?P<minor>[0-9]{1,2})", python_abi)
    if match is None:
        raise _runtime_invalid(
            "sealed_release_runtime_closure_mismatch",
            "runtime Python ABI is invalid",
        )
    return (
        f"lib/python{match.group('major')}.{match.group('minor')}/site-packages/"
        f"{RUNTIME_RELOCATION_VERIFIER_MODULE}.py"
    )


def _runtime_relative_name(value: object) -> str:
    if not isinstance(value, str) or not value or "\\" in value or "\x00" in value:
        raise _runtime_invalid(
            "sealed_release_runtime_closure_mismatch",
            "runtime relative member name is invalid",
        )
    parts = value.split("/")
    candidate = PurePosixPath(value)
    if (
        candidate.is_absolute()
        or candidate.as_posix() != value
        or any(part in {"", ".", ".."} for part in parts)
    ):
        raise _runtime_invalid(
            "sealed_release_runtime_closure_mismatch",
            "runtime relative member name escapes its authority",
        )
    return value


def _strict_runtime_json(payload: bytes) -> Mapping[str, object]:
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _runtime_invalid(
            "sealed_release_runtime_closure_mismatch",
            "runtime authority record is not strict JSON",
        ) from exc
    result = _runtime_mapping(value)
    try:
        canonical = _canonical_json_bytes(result)
    except (TypeError, ValueError) as exc:
        raise _runtime_invalid(
            "sealed_release_runtime_closure_mismatch",
            "runtime authority record is not canonical JSON",
        ) from exc
    if payload != canonical:
        raise _runtime_invalid(
            "sealed_release_runtime_closure_mismatch",
            "runtime authority record is not canonical JSON",
        )
    return result


def _runtime_mapping(value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise _runtime_invalid(
            "sealed_release_runtime_closure_mismatch",
            "runtime authority value is not a closed object",
        )
    return value


def _runtime_string_mapping(value: object) -> dict[str, str]:
    raw = _runtime_mapping(value)
    if any(not isinstance(item, str) or not item for item in raw.values()):
        raise _runtime_invalid(
            "sealed_release_runtime_closure_mismatch",
            "runtime platform identity is invalid",
        )
    return {key: str(item) for key, item in raw.items()}


def _canonical_ref_digest(value: object, schema: str) -> str:
    raw = _runtime_mapping(value)
    if (
        set(raw) != {"digest_domain", "schema_version", "digest"}
        or raw.get("digest_domain") != "canonical-json/v1"
        or raw.get("schema_version") != schema
        or not isinstance(raw.get("digest"), str)
    ):
        raise _runtime_invalid(
            "sealed_release_runtime_closure_mismatch",
            "runtime canonical digest reference is invalid",
        )
    return str(raw["digest"])


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _runtime_canonical_digest(value: object) -> str:
    return f"sha256:{hashlib.sha256(_canonical_json_bytes(value)).hexdigest()}"


def _raw_digest(payload: bytes) -> str:
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def _runtime_stat_token(
    value: os.stat_result,
) -> tuple[int, int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
        value.st_nlink,
    )


def _runtime_invalid(code: str, message: str) -> SealedReleaseInvalid:
    return SealedReleaseInvalid(message, data={"code": code})


def _git(cwd: Path, *args: str) -> None:
    proc = subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise SealedReleaseInvalid(
            "git command failed during sealed release preparation",
            data={
                "command": "git " + " ".join(args),
                "observed": (proc.stderr or proc.stdout)[-4000:],
            },
        )


def _git_output(cwd: Path, *args: str) -> str:
    proc = subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise SealedReleaseInvalid(
            "git command failed during sealed release preparation",
            data={
                "command": "git " + " ".join(args),
                "observed": (proc.stderr or proc.stdout)[-4000:],
            },
        )
    return proc.stdout.strip()


def _remove_worktree(source: Path, worktree: Path) -> None:
    if not worktree.exists():
        return
    proc = subprocess.run(
        ["git", "worktree", "remove", "--force", str(worktree)],
        cwd=source,
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        shutil.rmtree(worktree, ignore_errors=True)
        subprocess.run(
            ["git", "worktree", "prune"],
            cwd=source,
            check=False,
            capture_output=True,
            text=True,
        )


def _write_release_metadata(release: SealedRelease) -> None:
    payload = release.to_dict()
    payload.pop("artifact_root", None)
    payload.pop("reused", None)
    target = release.artifact_root / _RELEASE_FILENAME
    temporary = release.artifact_root / f".{_RELEASE_FILENAME}.tmp"
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, target)


def _release_artifact_id(
    inputs: SealedReleaseInputs,
    executable_tree_digest: str,
) -> str:
    return _canonical_digest(
        {
            "schema": SEALED_RELEASE_SCHEMA,
            "inputs": inputs.to_dict(),
            "executable_tree_digest": executable_tree_digest,
        }
    )


@contextmanager
def _input_lock(store: Path, input_id: str) -> Iterator[None]:
    locks = store / _LOCKS_DIRNAME
    locks.mkdir(exist_ok=True)
    lock_path = locks / f"{input_id.removeprefix('sha256:')}.lock"
    with lock_path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _input_index_path(store: Path, input_id: str) -> Path:
    return store / _INPUTS_DIRNAME / f"{input_id.removeprefix('sha256:')}.json"


def _load_indexed_release(
    store: Path,
    inputs: SealedReleaseInputs,
) -> SealedRelease | None:
    index_path = _input_index_path(store, inputs.input_id)
    if not index_path.exists():
        matches: list[SealedRelease] = []
        for candidate in store.iterdir():
            if (
                not candidate.is_dir()
                or re.fullmatch(
                    r"[0-9a-f]{64}",
                    candidate.name,
                )
                is None
            ):
                continue
            try:
                release = load_sealed_release(candidate)
            except SealedReleaseInvalid:
                continue
            if release.inputs == inputs:
                matches.append(release)
        if len(matches) > 1:
            raise SealedReleaseInvalid(
                "multiple sealed artifacts claim the same release inputs",
                data={
                    "expected": inputs.input_id,
                    "observed": sorted(item.artifact_id for item in matches),
                },
            )
        if not matches:
            return None
        release = verify_sealed_release(
            matches[0].artifact_root,
            expected_inputs=inputs,
            expected_artifact_id=matches[0].artifact_id,
            reused=True,
        )
        _write_input_index(store, release)
        return release
    try:
        raw = json.loads(index_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise SealedReleaseInvalid(
            "sealed release input index is invalid",
            data={"observed": str(index_path)},
        ) from exc
    if not isinstance(raw, dict) or raw.get("schema") != SEALED_RELEASE_SCHEMA:
        raise SealedReleaseInvalid(
            "sealed release input index is invalid",
            data={"observed": str(index_path)},
        )
    observed_input_id = raw.get("input_id")
    artifact_id = raw.get("artifact_id")
    if (
        observed_input_id != inputs.input_id
        or not isinstance(artifact_id, str)
        or not _is_sha256(artifact_id)
    ):
        raise SealedReleaseInvalid(
            "sealed release input index identity is inconsistent",
            data={
                "expected": inputs.input_id,
                "observed": observed_input_id,
                "artifact_id": artifact_id,
            },
        )
    return verify_sealed_release(
        store / artifact_id.removeprefix("sha256:"),
        expected_inputs=inputs,
        expected_artifact_id=artifact_id,
        reused=True,
    )


def _write_input_index(store: Path, release: SealedRelease) -> None:
    indexes = store / _INPUTS_DIRNAME
    indexes.mkdir(exist_ok=True)
    target = _input_index_path(store, release.inputs.input_id)
    temporary = indexes / f".{target.name}.{os.getpid()}.tmp"
    temporary.write_text(
        json.dumps(
            {
                "schema": SEALED_RELEASE_SCHEMA,
                "input_id": release.inputs.input_id,
                "artifact_id": release.artifact_id,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, target)


def _validate_inputs(
    inputs: SealedReleaseInputs,
    *,
    expected_revision: str,
    expected_deployment_revision: str | None = None,
    expected_deployment_tree_sha: str | None = None,
    expected_deployment_layout: str | None = None,
    expected_deployment_repo_path: str | None = None,
) -> None:
    for field in ("code_revision", "deployment_revision", "deployment_tree_sha"):
        value = getattr(inputs, field)
        if value is not None and re.fullmatch(
            r"(?:[0-9a-f]{40}|[0-9a-f]{64})", value
        ) is None:
            raise SealedReleaseInvalid(
                f"release input {field} must be a lowercase full Git object id",
                data={"observed": value},
            )
    if inputs.code_revision != expected_revision:
        raise SealedReleaseInvalid(
            "release input resolver returned the wrong revision for core authority",
            data={"expected": expected_revision, "observed": inputs.code_revision},
        )
    for field, observed, expected in (
        ("deployment_revision", inputs.deployment_revision, expected_deployment_revision),
        ("deployment_tree_sha", inputs.deployment_tree_sha, expected_deployment_tree_sha),
        ("deployment_layout", inputs.deployment_layout, expected_deployment_layout),
        ("deployment_repo_path", inputs.deployment_repo_path, expected_deployment_repo_path),
    ):
        if expected is not None and observed != expected:
            raise SealedReleaseInvalid(
                f"release input resolver returned the wrong {field}",
                data={"expected": expected, "observed": observed},
            )
    for field, value in inputs.to_dict().items():
        if field == "module_provenance":
            # A list of provider rows, not a digest string, so none of the
            # string checks below apply. It is examined here all the same:
            # skipping it outright let a resolver seal a row naming a module
            # and nothing else, which only failed later, at activation.
            try:
                for entry in module_provenance_entries(value):
                    owner = entry["owner"]
                    kind = entry["provider_kind"]
                    if not owner or not kind:
                        raise ReleaseModuleProvenanceMismatch(
                            f"module {entry['module_id']} names no owner and "
                            "provider kind",
                            data={"observed": repr(entry)[:200]},
                            fix_hint=(
                                "re-seal the release; every sealed module "
                                "records the provider that supplied it"
                            ),
                        )
                    if (owner == BUILTIN_OWNER) != (kind == BUILTIN_KIND):
                        raise ReleaseModuleProvenanceMismatch(
                            f"module {entry['module_id']} names owner "
                            f"{owner!r} with provider kind {kind!r}, which "
                            "disagree",
                            data={"observed": repr(entry)[:200]},
                            fix_hint=(
                                f"the {BUILTIN_OWNER!r} owner is the "
                                f"{BUILTIN_KIND!r} provider and no other "
                                "owner may claim that kind"
                            ),
                        )
            except ReleaseModuleProvenanceMismatch as exc:
                raise SealedReleaseInvalid(
                    f"release input module_provenance is malformed: {exc}",
                    data={"observed": inputs.to_dict()},
                ) from exc
            continue
        if field in {
            "skill_bundle_digest", "deployment_revision", "deployment_tree_sha",
            "deployment_layout", "deployment_repo_path",
        } and value is None:
            continue
        if not isinstance(value, str) or not value:
            raise SealedReleaseInvalid(
                f"release input {field} is missing",
                data={"observed": inputs.to_dict()},
            )
        if field in {"code_revision", "deployment_revision", "deployment_tree_sha"}:
            if re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", value) is None:
                raise SealedReleaseInvalid(
                    f"release input {field} must be a lowercase full Git object id",
                    data={"observed": value},
                )
        elif field == "deployment_layout":
            if value not in {"monorepo", "split"}:
                raise SealedReleaseInvalid("release deployment layout is invalid")
        elif field == "deployment_repo_path":
            if value not in {".", _DEPLOYMENT_SNAPSHOT_DIR}:
                raise SealedReleaseInvalid("release deployment path is invalid")
        elif not _is_sha256(value):
            raise SealedReleaseInvalid(
                f"release input {field} must be a sha256 digest",
                data={"observed": value},
            )


def _paths_digest(root: Path, paths: list[Path]) -> str:
    digest = hashlib.sha256()
    for path in paths:
        relative = path.relative_to(root).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return f"sha256:{digest.hexdigest()}"


def _canonical_digest(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _is_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and re.fullmatch(
            r"sha256:[0-9a-f]{64}",
            value,
        )
        is not None
    )


def _required_metadata(raw: Mapping[str, Any], field: str, root: Path) -> str:
    value = raw.get(field)
    if not isinstance(value, str) or not value:
        raise SealedReleaseInvalid(
            f"sealed release metadata field {field!r} is missing",
            data={"artifact_root": str(root)},
        )
    return value


def _optional_metadata(raw: Mapping[str, Any], field: str) -> str | None:
    value = raw.get(field)
    return value if isinstance(value, str) and value else None
