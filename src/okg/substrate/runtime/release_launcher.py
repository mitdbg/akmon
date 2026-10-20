"""Stdlib-only launcher for a pre-activated sealed OKG release.

This file is executed by path from a supervisor-owned installation. Keep it
free of ``okg`` and third-party imports: candidate code is not import authority
until the active pointer and complete executable tree have been verified here.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import secrets
import stat
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence


ACTIVE_RELEASE_SCHEMA = "okg.active_release/v1"
LAUNCH_DESCRIPTOR_SCHEMA = "okg.release_launcher/v1"
SEALED_RELEASE_SCHEMA = "okg.sealed_release/v1"
SERVICE_PYCACHE_ROOT_NAME = "service-pycache"
_IGNORED_TREE_PARTS = frozenset(
    {
        ".git",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        "__pycache__",
    }
)
class LauncherError(Exception):
    pass


def verify_launch(descriptor_path: str | Path) -> dict[str, Any]:
    descriptor_file = _absolute_without_symlink_resolution(descriptor_path)
    descriptor = _read_regular_json(
        descriptor_file,
        schema=LAUNCH_DESCRIPTOR_SCHEMA,
        label="release launcher descriptor",
    )
    environment_id = _required_string(descriptor, "environment_id")
    artifact_store = Path(_required_string(descriptor, "artifact_store")).resolve()
    active_path = _absolute_without_symlink_resolution(
        _required_string(descriptor, "active_pointer"),
    )
    active = _read_regular_json(
        active_path,
        schema=ACTIVE_RELEASE_SCHEMA,
        label="active release pointer",
    )
    if active.get("environment_id") != environment_id:
        raise LauncherError(
            "active release pointer belongs to a different environment",
        )
    service_authority_root = _absolute_without_symlink_resolution(
        _required_string(descriptor, "service_authority_root"),
    )
    if service_authority_root.is_symlink():
        raise LauncherError("service authority root must not be a symlink")

    artifact_id = _sha256_string(active, "artifact_id")
    artifact_root_input = _absolute_without_symlink_resolution(
        _required_string(active, "artifact_root"),
    )
    expected_root = artifact_store / artifact_id.removeprefix("sha256:")
    if artifact_root_input != expected_root or artifact_root_input.is_symlink():
        raise LauncherError(
            "active release root does not match the content-addressed store",
        )
    artifact_root = artifact_root_input.resolve()
    release = _read_regular_json(
        artifact_root / "release.json",
        schema=SEALED_RELEASE_SCHEMA,
        label="sealed release metadata",
    )
    _verify_release_metadata(release, artifact_root=artifact_root)
    for field in ("artifact_id", "input_id", "executable_tree_digest"):
        if active.get(field) != release.get(field):
            raise LauncherError(
                f"active release pointer disagrees with release metadata: {field}",
            )
    inputs = _required_mapping(release, "inputs")
    if active.get("code_revision") != inputs.get("code_revision"):
        raise LauncherError(
            "active release pointer disagrees with release input: code_revision",
        )
    release_definition_digest = active.get(
        "release_definition_digest", active.get("definition_digest")
    )
    if release_definition_digest != inputs.get("definition_digest"):
        raise LauncherError(
            "active release pointer disagrees with release input: "
            "release_definition_digest",
        )
    capacity_digest = inputs.get("postgres_capacity_digest")
    if capacity_digest is not None:
        if not _is_sha256(capacity_digest):
            raise LauncherError(
                "sealed release Postgres capacity digest is invalid"
            )
        capacity = _required_mapping(active, "postgres_capacity")
        if capacity.get("schema") != "okg.postgres-capacity/v1":
            raise LauncherError(
                "active release Postgres capacity schema is invalid"
            )
        if capacity.get("digest") != capacity_digest:
            raise LauncherError(
                "active release pointer disagrees with release input: "
                "postgres_capacity_digest"
            )
        capacity_identity = {
            "api_version": capacity["schema"],
            "max_worker_processes": _positive_int(
                capacity,
                "max_worker_processes",
            ),
            "max_parallel_workers": _positive_int(
                capacity,
                "max_parallel_workers",
            ),
            "timescaledb_max_background_workers": _positive_int(
                capacity,
                "timescaledb_max_background_workers",
            ),
            "timescaledb_job_worker_reserve": _positive_int(
                capacity,
                "timescaledb_job_worker_reserve",
            ),
        }
        if _canonical_digest(capacity_identity) != capacity_digest:
            raise LauncherError(
                "active release Postgres capacity values disagree with its digest"
            )

    observed_tree = _executable_tree_digest(artifact_root)
    expected_tree = _sha256_string(release, "executable_tree_digest")
    if observed_tree != expected_tree:
        raise LauncherError("sealed release executable tree digest mismatch")

    repo_root = artifact_root / "repo"
    layout = _required_mapping(release, "layout")
    if layout != {
        "repo_root": "repo",
        "venv_root": "venv",
        "interpreter": "venv/bin/python3",
    }:
        raise LauncherError("sealed release layout is not launchable")
    interpreter = artifact_root / _required_string(layout, "interpreter")
    interpreter_resolved = interpreter.resolve()
    _require_within(interpreter_resolved, artifact_root, "release interpreter")
    if not interpreter.is_file() or not os.access(interpreter, os.X_OK):
        raise LauncherError("sealed release interpreter is missing or not executable")

    descriptor_relative = Path(_required_string(active, "descriptor_relative"))
    if descriptor_relative.is_absolute() or ".." in descriptor_relative.parts:
        raise LauncherError("active environment descriptor escapes the release repo")
    candidate_descriptor_input = repo_root / descriptor_relative
    if candidate_descriptor_input.is_symlink():
        raise LauncherError("active environment descriptor must not be a symlink")
    candidate_descriptor = candidate_descriptor_input.resolve()
    _require_within(candidate_descriptor, repo_root, "environment descriptor")
    if not candidate_descriptor.is_file():
        raise LauncherError("active environment descriptor is missing or not regular")
    descriptor_digest = (
        f"sha256:{hashlib.sha256(candidate_descriptor.read_bytes()).hexdigest()}"
    )
    if descriptor_digest != _sha256_string(active, "descriptor_file_digest"):
        raise LauncherError(
            "active environment descriptor bytes changed after activation"
        )

    database = _required_mapping(active, "database")
    dsn_env = _required_string(database, "dsn_env")
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", dsn_env) is None:
        raise LauncherError("active release database dsn_env is invalid")
    plan = {
        "schema": "okg.release_launch_plan/v1",
        "environment_id": environment_id,
        "tier": _required_string(active, "tier"),
        "graph_deployment_name": _required_string(
            active,
            "graph_deployment_name",
        ),
        "descriptor_path": str(candidate_descriptor),
        "descriptor_digest": _sha256_string(active, "descriptor_digest"),
        "definition_digest": _sha256_string(active, "definition_digest"),
        "data_root": str(
            active.get("data_root")
            or (Path(_required_string(active, "artifact_root")).parent / "data")
        ),
        "database_name": _required_string(database, "name"),
        "database_identity": _required_string(database, "dbos_identity"),
        "database_dsn_env": dsn_env,
        "credential_refs": active.get("credential_refs") or {},
        "path_environment": active.get("path_environment") or {},
        "artifact_id": artifact_id,
        "artifact_root": str(artifact_root),
        "input_id": _sha256_string(release, "input_id"),
        "executable_tree_digest": expected_tree,
        "code_revision": _required_string(inputs, "code_revision"),
        "repo_root": str(repo_root),
        "interpreter": str(interpreter),
        "active_pointer": str(active_path),
        "launcher_descriptor": str(descriptor_file),
        "service_authority_root": str(service_authority_root),
    }
    if capacity_digest is not None:
        plan["postgres_capacity_digest"] = capacity_digest
    return plan


def build_exec_environment(
    plan: Mapping[str, Any],
    *,
    environ: Mapping[str, str] | None = None,
) -> dict[str, str]:
    ambient = dict(os.environ if environ is None else environ)
    # Start from a small execution allowlist. Credential-name heuristics are not
    # an authority boundary: AWS_ACCESS_KEY_ID, SSH_AUTH_SOCK, KUBECONFIG, and
    # credential-file variables do not necessarily contain TOKEN/SECRET/PASSWORD.
    base_names = {
        "HOME", "LANG", "LC_ALL", "LOGNAME", "PATH", "SHELL", "TMPDIR",
        "TZ", "USER", "INVOCATION_ID", "OKG_SERVICE_ID",
        "OKG_SERVICE_LIFECYCLE_NONCE",
    }
    source = {name: ambient[name] for name in base_names if name in ambient}
    credential_refs = plan.get("credential_refs") or {}
    if not isinstance(credential_refs, dict):
        raise LauncherError("active credential references are invalid")
    path_environment = plan.get("path_environment") or {}
    if not isinstance(path_environment, dict) or any(
        not isinstance(name, str) or not isinstance(value, str)
        for name, value in path_environment.items()
    ):
        raise LauncherError("active path environment is invalid")
    service_id = source.get("OKG_SERVICE_ID", "")
    allowed_refs = credential_refs.get(service_id) or []
    if not isinstance(allowed_refs, list) or any(
        not isinstance(ref, str) or not ref.startswith("env:") for ref in allowed_refs
    ):
        raise LauncherError("service credential references are invalid")
    allowed_names = {ref.removeprefix("env:") for ref in allowed_refs}
    credential_values = {
        name: ambient[name]
        for name in allowed_names
        if isinstance(ambient.get(name), str) and ambient[name]
    }
    if credential_values.keys() != allowed_names:
        raise LauncherError("declared service credential is unavailable")
    dsn_env = _required_string(plan, "database_dsn_env")
    dsn = ambient.get(dsn_env)
    if not isinstance(dsn, str) or not dsn.strip():
        raise LauncherError(
            f"required database secret environment variable {dsn_env} is unavailable",
        )
    source.update(path_environment)
    data_root = Path(
        str(
            plan.get("data_root")
            or (Path(_required_string(plan, "active_pointer")).parent / "data")
        )
    )
    source.update(
        {
            "OKG_ACTIVE_RELEASE_POINTER": _required_string(
                plan,
                "active_pointer",
            ),
            "OKG_DATABASE_IDENTITY": _required_string(
                plan,
                "database_identity",
            ),
            "OKG_DEFINITION_DIGEST": _required_string(
                plan,
                "definition_digest",
            ),
            "OKG_DEPLOYMENT": _required_string(
                plan,
                "graph_deployment_name",
            ),
            dsn_env: dsn,
            "OKG_DSN": dsn,
            "OKG_ENVIRONMENT_DESCRIPTOR": _required_string(
                plan,
                "descriptor_path",
            ),
            "OKG_ENVIRONMENT_DESCRIPTOR_DIGEST": _required_string(
                plan,
                "descriptor_digest",
            ),
            "OKG_ENVIRONMENT_ID": _required_string(plan, "environment_id"),
            "OKG_ENVIRONMENT_DATA_ROOT": str(data_root),
            "OKG_ENVIRONMENT_TIER": _required_string(plan, "tier"),
            "OKG_PROFILE_DIR": str(data_root / "profiles"),
            "OKG_RELEASE_ARTIFACT_ID": _required_string(plan, "artifact_id"),
            "OKG_RELEASE_INPUT_ID": _required_string(plan, "input_id"),
            "OKG_RELEASE_ROOT": _required_string(plan, "artifact_root"),
            "OKG_SERVICE_AUTHORITY_ROOT": _required_string(
                plan,
                "service_authority_root",
            ),
            "PYTHONNOUSERSITE": "1",
        }
    )
    capacity_digest = plan.get("postgres_capacity_digest")
    if isinstance(capacity_digest, str):
        source["OKG_POSTGRES_CAPACITY_DIGEST"] = capacity_digest
    source.update(credential_values)
    return source


def execute_launch(
    descriptor_path: str | Path,
    *,
    command: Sequence[str] = (),
    environ: Mapping[str, str] | None = None,
) -> None:
    plan = verify_launch(descriptor_path)
    _execute_plan(plan, command=command, environ=environ)


def _execute_plan(
    plan: Mapping[str, Any],
    *,
    command: Sequence[str],
    environ: Mapping[str, str] | None,
) -> None:
    arguments = list(command) or [
        "-m",
        "okg.substrate.runtime.worker",
        "--deployment",
        _required_string(plan, "graph_deployment_name"),
    ]
    if arguments and arguments[0] == "--":
        arguments = arguments[1:]
    if not arguments:
        raise LauncherError("release launcher command is empty")
    if arguments[0] != "-I":
        arguments.insert(0, "-I")
    _refuse_role_bytecode_options(arguments)
    interpreter = _required_string(plan, "interpreter")
    exec_environ = build_exec_environment(plan, environ=environ)
    # #2861: -B writes no bytecode, and a fresh private cache prefix means the
    # service never reads a __pycache__ entry from the sealed tree. The receipt
    # records the prefix apart from the role arguments so status checks can
    # rebuild this exact argv.
    pycache_prefix = _private_pycache_prefix(plan)
    _write_service_authority(
        plan,
        arguments=arguments,
        environ=exec_environ,
        pycache_prefix=pycache_prefix,
    )
    os.chdir(_required_string(plan, "repo_root"))
    os.execve(
        interpreter,
        [
            interpreter,
            "-I",
            "-B",
            "-X",
            f"pycache_prefix={pycache_prefix}",
            *arguments[1:],
        ],
        exec_environ,
    )


def _private_pycache_prefix(plan: Mapping[str, Any]) -> str:
    """Create a fresh, owner-only bytecode cache directory for one launch.

    The root is opened once with ``O_NOFOLLOW | O_DIRECTORY`` and every later
    step (the owner and mode check, cleanup, the new directory) acts on that
    descriptor, so swapping the root for a symlink after the check changes
    nothing.
    """
    authority_root = _absolute_without_symlink_resolution(
        _required_string(plan, "service_authority_root"),
    )
    root = authority_root.parent / SERVICE_PYCACHE_ROOT_NAME
    root.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.mkdir(root, 0o700)
    except FileExistsError:
        pass
    try:
        fd = os.open(
            root,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
        )
    except OSError as exc:
        raise LauncherError(
            "service bytecode cache root must be a real directory, "
            f"not a symlink: {root}"
        ) from exc
    try:
        metadata = os.fstat(fd)
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or stat.S_IMODE(metadata.st_mode) & 0o077
        ):
            raise LauncherError(
                "service bytecode cache root must be a private directory "
                "owned by the service user"
            )
        for name in os.listdir(fd):
            # Every launch directory stays empty under -B, including those of
            # services that are running now. Removing an empty one is
            # harmless: Python finds no cache there and compiles from source.
            # rmdir refuses a non-empty directory or a symlink, which then
            # stays for an operator to inspect.
            try:
                os.rmdir(name, dir_fd=fd)
            except OSError:
                continue
        for _attempt in range(16):
            name = f"launch-{secrets.token_hex(16)}"
            try:
                os.mkdir(name, 0o700, dir_fd=fd)
            except FileExistsError:
                continue
            return str(root / name)
        raise LauncherError("could not create a unique bytecode cache directory")
    finally:
        os.close(fd)


# Interpreter long options that take the next argument as their value.
_VALUED_LONG_OPTIONS = frozenset({"--check-hash-based-pycs"})


def _refuse_role_bytecode_options(arguments: Sequence[str]) -> None:
    """Refuse role arguments that already set the launcher's bytecode flags.

    Parse the interpreter options as CPython does: a short-option token is a
    cluster (``-sB``); ``-W`` and ``-X`` take the rest of the token or, when
    that is empty, the next argument; ``-m`` and ``-c`` (attached value or
    not), ``-``, ``--`` and the first non-option end the interpreter options,
    so the program's own arguments are never read as flags.
    """
    index = 1
    while index < len(arguments):
        token = arguments[index]
        index += 1
        if token in {"-", "--"} or not token.startswith("-"):
            return
        if token.startswith("--"):
            if token.split("=", 1)[0] in _VALUED_LONG_OPTIONS and "=" not in token:
                index += 1
            continue
        position = 1
        while position < len(token):
            flag = token[position]
            position += 1
            if flag in "mc":
                return
            if flag in "WX":
                value = token[position:]
                if not value:
                    value = arguments[index] if index < len(arguments) else ""
                    index += 1
                if flag == "X" and value.split("=", 1)[0] == "pycache_prefix":
                    raise LauncherError(
                        "service role arguments must not set -X pycache_prefix; "
                        "the launcher sets it"
                    )
                break
            if flag == "B":
                raise LauncherError(
                    "service role arguments must not set -B; the launcher sets it"
                )


def _verify_release_metadata(
    release: Mapping[str, Any],
    *,
    artifact_root: Path,
) -> None:
    inputs = _required_mapping(release, "inputs")
    expected_input_fields = {
        "code_revision",
        "dependency_lock_digest",
        "schema_checksum",
        "catalog_fingerprint",
        "definition_digest",
        "skill_bundle_digest",
        "deployment_revision",
        "deployment_tree_sha",
        "deployment_layout",
        "deployment_repo_path",
    }
    optional_input_fields = {"postgres_capacity_digest"}
    if (
        not expected_input_fields.issubset(inputs)
        or set(inputs) - expected_input_fields - optional_input_fields
    ):
        raise LauncherError("sealed release input identity fields are incomplete")
    if not _required_string(inputs, "code_revision"):
        raise LauncherError("sealed release code revision is missing")
    for field in expected_input_fields - {
        "code_revision", "skill_bundle_digest", "deployment_revision",
        "deployment_tree_sha",
        "deployment_layout",
        "deployment_repo_path",
    }:
        _sha256_string(inputs, field)
    for field in ("code_revision", "deployment_revision", "deployment_tree_sha"):
        if re.fullmatch(
            r"(?:[0-9a-f]{40}|[0-9a-f]{64})",
            _required_string(inputs, field),
        ) is None:
            raise LauncherError(f"sealed release {field} is invalid")
    layout = _required_string(inputs, "deployment_layout")
    repo_path = _required_string(inputs, "deployment_repo_path")
    if (layout, repo_path) not in {
        ("monorepo", "."),
        ("split", ".okg-deployment-repository"),
    }:
        raise LauncherError("sealed release deployment layout is invalid")
    skill_digest = inputs.get("skill_bundle_digest")
    if skill_digest is not None and not _is_sha256(skill_digest):
        raise LauncherError("sealed release skill bundle digest is invalid")
    capacity_digest = inputs.get("postgres_capacity_digest")
    if capacity_digest is not None and not _is_sha256(capacity_digest):
        raise LauncherError("sealed release Postgres capacity digest is invalid")

    input_id = _canonical_digest(inputs)
    if release.get("input_id") != input_id:
        raise LauncherError("sealed release input identity is inconsistent")
    executable_tree_digest = _sha256_string(release, "executable_tree_digest")
    artifact_id = _canonical_digest(
        {
            "schema": SEALED_RELEASE_SCHEMA,
            "inputs": dict(inputs),
            "executable_tree_digest": executable_tree_digest,
        }
    )
    if release.get("artifact_id") != artifact_id:
        raise LauncherError("sealed release artifact identity is inconsistent")
    if artifact_root.name != artifact_id.removeprefix("sha256:"):
        raise LauncherError("sealed release directory does not match artifact id")


def _write_service_authority(
    plan: Mapping[str, Any],
    *,
    arguments: Sequence[str],
    environ: Mapping[str, str],
    pycache_prefix: str,
) -> None:
    service_id = environ.get("OKG_SERVICE_ID")
    if not service_id:
        return
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", service_id) is None:
        raise LauncherError("OKG_SERVICE_ID is invalid")
    root = _absolute_without_symlink_resolution(
        _required_string(plan, "service_authority_root"),
    )
    root.mkdir(parents=True, exist_ok=True)
    if root.is_symlink():
        raise LauncherError("service authority root must not be a symlink")
    pid = os.getpid()
    payload = {
        "schema": "okg.service_release_authority/v1",
        "verified_at": datetime.now(timezone.utc).isoformat(),
        "service_id": service_id,
        "environment_id": _required_string(plan, "environment_id"),
        "artifact_id": _required_string(plan, "artifact_id"),
        "artifact_root": _required_string(plan, "artifact_root"),
        "definition_digest": _required_string(plan, "definition_digest"),
        "active_pointer": _required_string(plan, "active_pointer"),
        "interpreter": _required_string(plan, "interpreter"),
        "repo_root": _required_string(plan, "repo_root"),
        "isolated": bool(arguments and arguments[0] == "-I"),
        "arguments": list(arguments),
        "pycache_prefix": pycache_prefix,
        "pid": pid,
        "process_start_identity": _process_start_identity(pid),
    }
    lifecycle_nonce = environ.get("OKG_SERVICE_LIFECYCLE_NONCE")
    if lifecycle_nonce is not None:
        if (
            not 32 <= len(lifecycle_nonce) <= 128
            or any(
                not (char.isascii() and (char.isalnum() or char in "_-") )
                for char in lifecycle_nonce
            )
        ):
            raise LauncherError("service lifecycle nonce is invalid")
        payload["lifecycle_nonce"] = lifecycle_nonce
    systemd_invocation_id = environ.get("INVOCATION_ID")
    if systemd_invocation_id is not None:
        if re.fullmatch(r"[0-9a-f]{32}", systemd_invocation_id) is None:
            raise LauncherError("systemd invocation identity is invalid")
        payload["systemd_invocation_id"] = systemd_invocation_id
    _atomic_write_json(root / f"{service_id}.json", payload)


def _executable_tree_digest(artifact_root: Path) -> str:
    roots = [artifact_root / "repo", artifact_root / "venv"]
    if any(not root.is_dir() for root in roots):
        raise LauncherError("sealed release executable layout is incomplete")
    resolved_root = artifact_root.resolve()
    entries: list[Path] = []
    bytecode: list[str] = []
    for root in roots:
        entries.append(root)
        for child in root.rglob("*"):
            relative_name = child.relative_to(artifact_root).as_posix()
            if _is_bytecode_relative_name(relative_name) and not _live_profile_path(
                child, artifact_root=artifact_root
            ):
                bytecode.append(relative_name)
            elif not _ignored_executable_tree_path(child, artifact_root=artifact_root):
                entries.append(child)
    if bytecode:
        # The digest never covers bytecode, so a planted .pyc would run
        # unverified; a sealed tree holds none (#2857).
        raise LauncherError(
            "sealed_runtime_bytecode_present: sealed release contains Python "
            f"bytecode: {', '.join(sorted(bytecode)[:20])}"
        )
    digest = hashlib.sha256(b"okg-executable-tree-v1\0")
    for path in sorted(
        entries,
        key=lambda item: item.relative_to(artifact_root).as_posix(),
    ):
        relative = path.relative_to(artifact_root).as_posix()
        metadata = path.lstat()
        mode = stat.S_IMODE(metadata.st_mode)
        if stat.S_ISLNK(metadata.st_mode):
            try:
                resolved = path.resolve(strict=True)
                resolved.relative_to(resolved_root)
            except (OSError, ValueError) as exc:
                raise LauncherError(
                    f"sealed release symlink escapes the artifact: {relative}",
                ) from exc
            kind = "symlink"
            content = os.readlink(path).encode("utf-8")
        elif stat.S_ISDIR(metadata.st_mode):
            kind = "directory"
            content = b""
        elif stat.S_ISREG(metadata.st_mode):
            kind = "file"
            content = path.read_bytes()
        else:
            raise LauncherError(
                f"sealed release contains unsupported entry: {relative}",
            )
        for token in (
            kind.encode("ascii"),
            f"{mode:o}".encode("ascii"),
            relative.encode("utf-8"),
            content,
        ):
            digest.update(token)
            digest.update(b"\0")
    return f"sha256:{digest.hexdigest()}"


def _read_regular_json(
    path: Path,
    *,
    schema: str,
    label: str,
) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise LauncherError(f"{label} is missing or not a regular file")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise LauncherError(f"{label} is invalid: {exc}") from exc
    if not isinstance(raw, dict) or raw.get("schema") != schema:
        raise LauncherError(f"{label} has the wrong schema")
    return raw


def _absolute_without_symlink_resolution(path: str | Path) -> Path:
    return Path(os.path.abspath(Path(path).expanduser()))


def _required_mapping(raw: Mapping[str, Any], field: str) -> Mapping[str, Any]:
    value = raw.get(field)
    if not isinstance(value, Mapping):
        raise LauncherError(f"required mapping {field!r} is missing")
    return value


def _required_string(raw: Mapping[str, Any], field: str) -> str:
    value = raw.get(field)
    if not isinstance(value, str) or not value:
        raise LauncherError(f"required field {field!r} is missing")
    return value


def _sha256_string(raw: Mapping[str, Any], field: str) -> str:
    value = _required_string(raw, field)
    if not _is_sha256(value):
        raise LauncherError(f"required digest {field!r} is invalid")
    return value


def _positive_int(raw: Mapping[str, Any], field: str) -> int:
    value = raw.get(field)
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise LauncherError(f"required positive integer {field!r} is invalid")
    return value


def _is_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and re.fullmatch(
            r"sha256:[0-9a-f]{64}",
            value,
        )
        is not None
    )


def _canonical_digest(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _is_bytecode_relative_name(relative_name: str) -> bool:
    parts = relative_name.split("/")
    return any(part.casefold() == "__pycache__" for part in parts) or (
        relative_name.casefold().endswith((".pyc", ".pyo"))
    )


def _ignored_tree_path(relative: Path) -> bool:
    if any(part in _IGNORED_TREE_PARTS for part in relative.parts):
        return True
    return relative.suffix in {".pyc", ".pyo"}


def _ignored_executable_tree_path(path: Path, *, artifact_root: Path) -> bool:
    relative = path.relative_to(artifact_root)
    if _ignored_tree_path(relative):
        return True
    return _live_profile_path(path, artifact_root=artifact_root)


def _live_profile_path(path: Path, *, artifact_root: Path) -> bool:
    parts = path.relative_to(artifact_root).parts
    if len(parts) < 2 or parts[:2] != ("repo", "profiles"):
        return False
    if len(parts) <= 3:
        return path.is_dir() and not path.is_symlink()
    if parts[2] == "_live" or parts[3] != "_live":
        return False
    live_root = artifact_root.joinpath(*parts[:4])
    return live_root.is_dir() and not live_root.is_symlink()


def _require_within(path: Path, root: Path, label: str) -> None:
    try:
        path.relative_to(root.resolve())
    except ValueError as exc:
        raise LauncherError(f"{label} escapes the sealed release") from exc


def _process_start_identity(pid: int) -> str | None:
    stat_path = Path(f"/proc/{pid}/stat")
    if stat_path.is_file():
        try:
            remainder = stat_path.read_text(encoding="utf-8").rsplit(")", 1)[1]
            fields = remainder.split()
            return f"proc:{fields[19]}"
        except (OSError, IndexError):
            return None
    proc = subprocess.run(
        ["ps", "-p", str(pid), "-o", "lstart="],
        check=False,
        capture_output=True,
        text=True,
    )
    value = proc.stdout.strip()
    return f"ps:{value}" if proc.returncode == 0 and value else None


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        temporary.chmod(0o600)
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="okg-release-launcher")
    parser.add_argument("--descriptor", required=True)
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        plan = verify_launch(args.descriptor)
        build_exec_environment(plan)
        if args.verify_only:
            if args.json:
                print(json.dumps({"ok": True, **plan}, indent=2))
            else:
                print(
                    f"verified {plan['environment_id']} artifact={plan['artifact_id']}",
                )
            return 0
        _execute_plan(
            plan,
            command=args.command,
            environ=None,
        )
        return 0
    except LauncherError as exc:
        if args.json:
            print(
                json.dumps(
                    {
                        "ok": False,
                        "error": {
                            "code": "sealed_release_invalid",
                            "message": str(exc),
                        },
                    },
                    indent=2,
                ),
                file=sys.stderr,
            )
        else:
            print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
