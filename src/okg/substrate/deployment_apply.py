"""Digest-bound, resumable deployment-state reconciliation.

``okg deployment apply`` is intentionally narrower than a release upgrade:
it converges catalog ownership/content, Nomos baseline, source wakeups, and
the running worker's configuration.  Dry-run is read-only.  Apply requires the
exact plan id and records each observed postcondition in Postgres.
"""
from __future__ import annotations

import hashlib
import json
import os
import platform
import plistlib
import shlex
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

import psycopg
from okg.substrate.db.connection import connect as db_connect
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from okg.substrate import fastyaml
from okg.substrate.cli.child_argv import running_okg_argv
from okg.substrate.errors import SubstrateError
from okg.substrate.cli.command_reference import okg_command
from okg.substrate.support.user_hints import hint_command, okg_prefix


SCHEMA = "okg.deployment_apply/v1"
STAGES = (
    "catalog_ownership",
    "catalog_apply",
    "nomos_baseline",
    "source_supervision",
    "worker_reload",
)


@dataclass(frozen=True)
class ApplyCommandResult:
    returncode: int
    stdout: str = ""
    stderr: str = ""

    def json_payload(self) -> Any | None:
        try:
            return json.loads(self.stdout)
        except (TypeError, ValueError):
            return None


CommandRunner = Callable[[list[str], Mapping[str, str]], ApplyCommandResult]


def _source_enqueue_failure(source: str, result: ApplyCommandResult) -> SubstrateError:
    """The refusal for a failed `okg jobs enqueue-source` child (#2480).

    The child runs with `--json`, so its own cause is usually the `error`
    object on stdout, not stderr; the message carries it so an operator sees
    why, not only which source. Every text is DSN-masked: the child's error
    can quote its connection string.
    """
    from okg.substrate.db.dsn import redact_text

    payload = result.json_payload()
    error = payload.get("error") if isinstance(payload, Mapping) else None
    if isinstance(error, Mapping):
        cause = str(error.get("message") or "")
    else:
        cause = str(error or "")
    if not cause:
        lines = [line for line in (result.stderr or "").splitlines() if line.strip()]
        cause = lines[-1].strip() if lines else ""
    if not cause:
        cause = f"exit {result.returncode} with no error output"
    return SubstrateError(
        f"source supervision enqueue failed for {source}: {redact_text(cause, dsn_flags=())}",
        data={
            "returncode": result.returncode,
            "stderr": redact_text(result.stderr[-2000:], dsn_flags=()),
            "stdout": redact_text((result.stdout or "")[-2000:], dsn_flags=()),
        },
        fix_hint=(
            redact_text(str(error["fix_hint"]), dsn_flags=())
            if isinstance(error, Mapping) and error.get("fix_hint") else None
        ),
    )
SupervisorInventory = Callable[[str], list[tuple[str, tuple[str, ...]]]]


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _digest(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value).encode()).hexdigest()


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(child) for key, child in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(child) for child in value]
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _target_identity(dsn: str) -> dict[str, Any]:
    info = conninfo_to_dict(dsn)
    return {
        "host": info.get("host"),
        "port": info.get("port"),
        "dbname": info.get("dbname"),
        "user": info.get("user"),
    }


def _enclosing_checkout(deployment_dir: Path) -> Path | None:
    """The Git checkout holding the deployment folder, or None."""
    try:
        root = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=str(deployment_dir),
            capture_output=True,
            text=True,
            timeout=5,
            check=True,
        ).stdout.strip()
    except Exception:  # noqa: BLE001 - non-git deployment roots are supported.
        root = ""
    return Path(root).resolve() if root else None


def _runtime_working_directory(
    *, deployment_dir: Path, sync_root: Path | None,
) -> Path:
    """Return the checkout root the compatibility worker must execute from."""
    if sync_root is not None:
        return sync_root.expanduser().resolve()
    return _enclosing_checkout(deployment_dir) or deployment_dir


_UNSET = object()

SUPERVISOR_ADAPTERS = ("launchd", "systemd", "container")

# WORKDIR in the runtime image; `uv sync` puts the environment at .venv beneath
# it. Deterministic from the Dockerfile, which is what lets the container
# adapter carry exact identity fields rather than relaxed ones.
CONTAINER_ROOT = Path("/opt/okg")

# The interpreter the ACKING process reports, and the spelling is load-bearing:
# the gate compares by raw string equality, so getting it wrong is a permanent
# refusal on a correct deployment.
#
# `sys.executable` preserves the name used to exec rather than resolving the
# symlink, so the value depends on HOW the process was started -- and BOTH
# spellings occur in this image:
#
#   command: ["okg"]        -> shebang /opt/okg/.venv/bin/python3 -> ".../python3"
#   command: ["python","-c"] -> PATH                              -> ".../python"
#
# The process that records the ack is the `worker` container, which runs
# `command: ["okg"]` (ops/kubernetes/okg/templates/runtime-statefulset.yaml:248).
# Hence python3. If that `command:` is ever changed to invoke the interpreter
# directly, this declaration silently stops matching and the gate refuses with
# no other signal -- which is why the coupling is named here rather than left to
# be rediscovered from a failed rollout.
#
# Do NOT "fix" a mismatch by normalising with .resolve(). Measured in this image,
# /opt/okg/.venv/bin/python and .../python3 BOTH resolve to /usr/local/bin/python3,
# which is outside the venv -- so resolving would erase the venv distinction and
# make a process running the SYSTEM python indistinguishable from the venv one.
# That is strictly worse than the mismatch it would fix.
CONTAINER_INTERPRETER = CONTAINER_ROOT / ".venv" / "bin" / "python3"

# `ARG VCS_REF=unknown` in the Dockerfile: a build that omits the build-arg bakes
# this literal. It must never be treated as a revision.
UNKNOWN_BUILD_REVISION = "unknown"

# Manifests at or above this contract version must declare runtime.supervisor
# when runtime is enabled. Below it, the adapter is still inferred from the host
# OS so a live host worker cannot be broken by a mid-upgrade raise.
SUPERVISOR_REQUIRED_CONTRACT_VERSION = 2


SUPERVISOR_OVERLAY_ENV = "OKG_RUNTIME_SUPERVISOR"


def supervisor_overlay() -> str | None:
    """The target-scoped supervisor declaration, if the harness set one.

    The deployment manifest is baked into the image (`COPY . .`), so the pod
    reads the SAME deployment.yaml as the host checkout. A static declaration in
    that file therefore cannot be truthful for both targets. The chart supplies
    the in-cluster instantiation's declaration here.

    This is a DECLARATION, not inference: the chart's values are the deployment
    configuration for that target, and setting it is a visible, explicit act at
    the layer that knows the environment. It is emphatically not /.dockerenv or
    platform.system() sniffing, which remain forbidden -- the difference is that
    someone wrote this down for this target, rather than the code guessing from
    ambient facts.
    """

    value = (os.environ.get(SUPERVISOR_OVERLAY_ENV) or "").strip()
    return value or None


def resolve_supervisor_adapter(
    *,
    declared: Any,
    deployment: str,
    runtime_enabled: bool,
    contract_version: Any = None,
    overlay: Any = _UNSET,
) -> str | None:
    """The declared adapter, or None to fall back to host inference.

    The adapter is a property of the deployment TARGET, not of the machine
    planning the change: the same deployment runs as a host launchd worker and
    as an in-cluster container, so it must be declared per instantiation. Never
    derived from platform.system() -- doing that inside a pod selects systemd and
    rebuilds the bug this exists to remove.

    Precedence is overlay, then manifest. The overlay wins because it is the
    narrower, target-scoped declaration: the manifest describes the deployment,
    the overlay describes THIS instantiation of it.
    """

    if overlay is _UNSET:
        overlay = supervisor_overlay()
    if overlay is not None:
        declared = overlay
    if declared is not None:
        value = str(declared).strip()
        if value not in SUPERVISOR_ADAPTERS:
            raise SubstrateError(
                "deployment declares an unsupported runtime supervisor",
                data={
                    "deployment": deployment,
                    "declared": declared,
                    "supported": list(SUPERVISOR_ADAPTERS),
                },
                fix_hint="set runtime.supervisor to one of "
                + ", ".join(SUPERVISOR_ADAPTERS),
            )
        return value
    try:
        version = int(contract_version) if contract_version is not None else 0
    except (TypeError, ValueError):
        version = 0
    if runtime_enabled and version >= SUPERVISOR_REQUIRED_CONTRACT_VERSION:
        raise SubstrateError(
            "runtime-enabled deployment does not declare runtime.supervisor",
            data={
                "deployment": deployment,
                "contract_version": contract_version,
                "supported": list(SUPERVISOR_ADAPTERS),
            },
            fix_hint=(
                "add `runtime.supervisor` to deployment.yaml (one of "
                + ", ".join(SUPERVISOR_ADAPTERS)
                + "); required from contract version "
                + str(SUPERVISOR_REQUIRED_CONTRACT_VERSION)
            ),
        )
    return None


def _tracking_service_authority(
    *,
    deployment: str,
    deployment_dir: Path,
    sync_root: Path | None,
    target: Mapping[str, Any],
    supervisor: str | None = None,
) -> dict[str, Any]:
    """Describe the exact compatibility supervisor definition to install."""
    if supervisor == "container":
        return _container_service_authority(
            deployment=deployment, target=target,
        )
    if (
        supervisor is None
        and sync_root is None
        and _enclosing_checkout(deployment_dir) is None
    ):
        # A plain install: the deployment folder is not inside a checkout, so
        # every path the host adapters derive from it names no okg at all.
        return _process_service_authority(
            deployment=deployment, target=target,
        )
    if supervisor == "launchd":
        adapter = "launchd"
        service_id = f"org.okg.runtime-worker.{deployment}"
    elif supervisor == "systemd":
        adapter = "systemd"
        service_id = f"okg-runtime-worker@{deployment}.service"
    else:
        # Legacy path: manifests below the contract version that requires a
        # declaration keep host inference so an in-place worker keeps running.
        system = platform.system()
        if system == "Darwin":
            adapter = "launchd"
            service_id = f"org.okg.runtime-worker.{deployment}"
        elif system == "Linux":
            adapter = "systemd"
            service_id = f"okg-runtime-worker@{deployment}.service"
        else:
            raise SubstrateError(
                "deployment apply cannot reconcile a worker service on this host",
                data={"platform": system, "deployment": deployment},
                fix_hint="use a supported launchd or systemd supervisor host",
            )
    working_directory = _runtime_working_directory(
        deployment_dir=deployment_dir,
        sync_root=sync_root,
    )
    try:
        code_revision = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(working_directory),
            capture_output=True,
            text=True,
            timeout=5,
            check=True,
        ).stdout.strip() or None
    except Exception:  # noqa: BLE001 - non-git tracking deployments are legal.
        code_revision = None
    process_executable = working_directory / ".venv" / "bin" / "python3"
    return {
        "authority_source": "deployment_manifest_and_selected_dsn",
        "adapter": adapter,
        "service_id": service_id,
        "working_directory": str(working_directory),
        "command": "uv run okg runtime worker",
        "launcher_executable": shutil.which("uv") or "uv",
        "process_executable": str(process_executable),
        "interpreter": str(process_executable),
        "import_root": str(working_directory / "src"),
        "code_revision": code_revision,
        "database": {
            key: target.get(key)
            for key in ("host", "port", "dbname")
        },
    }


def container_code_revision() -> str | None:
    """The image's build revision, or None.

    The image carries no .git (`.dockerignore` drops it while the Dockerfile
    does `COPY . .`), so `git rev-parse` cannot answer inside the runtime.
    The build already knows the revision -- it is passed as `--build-arg
    VCS_REF` and stamped as `org.opencontainers.image.revision` -- so the
    container adapter reads it from the environment the image bakes rather than
    having the field relaxed. Absent, this returns None and the gate refuses:
    an unknown revision is never treated as agreement.
    """

    value = (os.environ.get("OKG_CODE_REVISION") or "").strip()
    if not value or value == UNKNOWN_BUILD_REVISION:
        # The Dockerfile's `ARG VCS_REF=unknown` default reaches the env when a
        # build path forgets --build-arg. Both sides would then read the literal
        # "unknown" and COMPARE EQUAL -- a false convergence that would let the
        # gate pass having verified nothing. An honest None refuses instead.
        return None
    return value


def _imported_package_root() -> Path:
    """The directory the running ``okg`` package is imported from.

    The same expression the worker's observed authority reports
    (``_environment_authority_for_apply``). In the runtime image okg is a
    wheel installed in /opt/okg/.venv, so this is that environment's
    site-packages; a pod that puts /opt/okg/src on PYTHONPATH (the lifecycle
    lane) imports from there instead. Either way it is measured, not assumed:
    a worker importing okg from anywhere else still mismatches.
    """
    return Path(__file__).resolve().parents[2]


def _container_service_authority(
    *, deployment: str, target: Mapping[str, Any],
) -> dict[str, Any]:
    """Identity for an in-cluster runtime, remapped rather than relaxed.

    Every field the host adapters carry is carried here too, rooted at the
    image's deterministic WORKDIR instead of a checkout. No field is dropped:
    exact equality is achievable because the paths come from the Dockerfile.
    ``import_root`` is the one measured path: the image installs okg from a
    wheel into its environment, so the package is not under /opt/okg/src.
    """

    process_executable = CONTAINER_INTERPRETER
    return {
        "authority_source": "deployment_manifest_and_selected_dsn",
        "adapter": "container",
        "service_id": f"okg-runtime-worker.{deployment}",
        "working_directory": str(CONTAINER_ROOT),
        "command": "okg runtime worker",
        "launcher_executable": str(CONTAINER_ROOT / ".venv" / "bin" / "okg"),
        "process_executable": str(process_executable),
        "interpreter": str(process_executable),
        "import_root": str(_imported_package_root()),
        "code_revision": container_code_revision(),
        "database": {
            key: target.get(key)
            for key in ("host", "port", "dbname")
        },
    }


PROCESS_ADAPTER = "process"


def deployment_uses_process_worker(deployment_dir: Path | str) -> bool:
    """Whether the deployment's worker is one the operator starts by hand.

    True for a plain install: the folder is outside any Git checkout, names no
    sync root, and neither it nor the target overlay declares
    `runtime.supervisor`. Such a worker gets the `process` identity, and no
    command installs or restarts a service for it. Reads only the manifest,
    so it needs no database.
    """
    from okg.substrate.deployments import DeploymentManifest

    root = Path(deployment_dir).expanduser().resolve()
    manifest_path = root / "deployment.yaml"
    raw = fastyaml.safe_load(manifest_path.read_text()) or {}
    runtime = raw.get("runtime") if isinstance(raw, Mapping) else None
    declared = runtime.get("supervisor") if isinstance(runtime, Mapping) else None
    if supervisor_overlay() is not None or declared is not None:
        return False
    manifest = DeploymentManifest.from_yaml_lenient(manifest_path)
    return manifest.sync_root is None and _enclosing_checkout(root) is None

# What a `process` worker must match: the okg installation the checking
# command runs, never paths derived from the deployment folder. A plain
# install's worker can be started from any directory, so the working
# directory is not part of its identity; the interpreter is compared by its
# environment (sys.prefix), which `python` and `python3` spellings share.
PROCESS_IDENTITY_FIELDS = (
    "python_prefix",
    "import_root",
    "installed_code_revision",
    "database",
)


def _process_service_authority(
    *, deployment: str, target: Mapping[str, Any],
) -> dict[str, Any]:
    """Identity for a worker the operator starts with `okg runtime worker`.

    No supervisor definition exists for okg to install, so the only claim
    worth checking is that the worker runs the same okg as this command: the
    same environment, the same imported package, the same commit.
    """
    from okg.substrate.runtime.code_identity import installed_code_revision

    revision = installed_code_revision()
    return {
        "authority_source": "deployment_manifest_and_selected_dsn",
        "adapter": PROCESS_ADAPTER,
        "service_id": f"okg-runtime-worker.{deployment}",
        "command": str(okg_command("runtime worker", deployment=deployment)),
        "python_prefix": str(Path(sys.prefix).resolve()),
        "import_root": str(_imported_package_root()),
        "installed_code_revision": revision.revision,
        "installed_code_revision_reason": revision.reason,
        "database": {
            key: target.get(key)
            for key in ("host", "port", "dbname")
        },
    }


def _environment_authority_for_apply(
    *, deployment: str, deployment_dir: Path | str, dsn: str,
) -> dict[str, str] | None:
    """Resolve the sealed environment that owns this deployment target."""
    from okg.substrate.deployments.environment import load_environment
    from okg.substrate.runtime.release_authority import (
        _RELEASE_ENVIRONMENT_KEYS,
        _SEALED_MARKERS,
    )

    values = dict(os.environ)
    markers = sorted(key for key in _SEALED_MARKERS if values.get(key))
    if markers:
        missing = sorted(key for key in _RELEASE_ENVIRONMENT_KEYS if not values.get(key))
        if missing:
            raise SubstrateError(
                "deployment apply refused incomplete sealed environment authority",
                data={
                    "deployment": deployment,
                    "executable": False,
                    "observed_markers": markers,
                    "missing": missing,
                },
                fix_hint="repair the sealed launcher environment before service control",
            )
        environment = load_environment(
            values["OKG_ENVIRONMENT_DESCRIPTOR"], environ=values,
        )
        if (
            environment.id != values["OKG_ENVIRONMENT_ID"]
            or environment.graph_deployment_name != deployment
            or environment.release.mode != "sealed"
        ):
            raise SubstrateError(
                "deployment apply refused invalid sealed environment authority",
                data={
                    "deployment": deployment,
                    "executable": False,
                    "environment_id": environment.id,
                    "graph_deployment_name": environment.graph_deployment_name,
                    "release_mode": environment.release.mode,
                },
            )
        return {
            "environment_id": environment.id,
            "service_id": environment.runtime.worker_service,
            "adapter": "launchd" if sys.platform == "darwin" else "systemd",
        }

    root = Path(deployment_dir).expanduser().resolve()
    environments_root = next(
        (
            parent / "ops" / "environments"
            for parent in (root, *root.parents)
            if (parent / "ops" / "environments").is_dir()
        ),
        None,
    )
    if environments_root is None:
        return None
    target = _target_identity(dsn)
    target_port = int(target.get("port") or 5432)
    matches: list[tuple[Path, Mapping[str, Any]]] = []
    for descriptor in sorted(environments_root.glob("*/environment.yaml")):
        raw = fastyaml.safe_load(descriptor.read_text(encoding="utf-8")) or {}
        if not isinstance(raw, Mapping):
            raise SubstrateError(
                "deployment apply found a malformed environment descriptor",
                data={"path": str(descriptor), "executable": False},
            )
        release = raw.get("release")
        if (
            str(raw.get("graph_deployment_name") or "") != deployment
            or not isinstance(release, Mapping)
            or str(release.get("mode") or "") != "sealed"
        ):
            continue
        database = raw.get("database")
        if not isinstance(database, Mapping):
            raise SubstrateError(
                "deployment apply found incomplete sealed environment authority",
                data={
                    "path": str(descriptor),
                    "deployment": deployment,
                    "executable": False,
                    "missing": ["database"],
                },
            )
        try:
            descriptor_port = int(database["host_port"])
            descriptor_name = str(database["name"]).strip()
            if descriptor_port <= 0 or not descriptor_name:
                raise ValueError
        except (KeyError, TypeError, ValueError) as exc:
            raise SubstrateError(
                "deployment apply found incomplete sealed environment authority",
                data={
                    "path": str(descriptor),
                    "deployment": deployment,
                    "executable": False,
                    "missing_or_invalid": [
                        "database.name",
                        "database.host_port",
                    ],
                },
            ) from exc
        if (
            descriptor_name == str(target.get("dbname") or "")
            and descriptor_port == target_port
        ):
            matches.append((descriptor, raw))
    if not matches:
        return None
    if len(matches) != 1:
        raise SubstrateError(
            "deployment apply found ambiguous sealed environment authority",
            data={
                "deployment": deployment,
                "executable": False,
                "environment_descriptors": [str(path) for path, _ in matches],
            },
        )
    descriptor, raw = matches[0]
    environment_values = dict(values)
    secret_ref = str(raw["database"].get("dsn_secret_ref") or "")
    if secret_ref.startswith("env:"):
        environment_values[secret_ref.removeprefix("env:")] = dsn
    environment = load_environment(descriptor, environ=environment_values)
    return {
        "environment_id": environment.id,
        "service_id": environment.runtime.worker_service,
        "adapter": "launchd" if sys.platform == "darwin" else "systemd",
    }


def _require_tracking_apply_authority(
    *, deployment: str, deployment_dir: Path | str, dsn: str,
) -> None:
    authority = _environment_authority_for_apply(
        deployment=deployment, deployment_dir=deployment_dir, dsn=dsn,
    )
    if authority is None:
        return
    remedy = (
        f"{okg_prefix()} environment service restart "
        f"{authority['environment_id']} --adapter {authority['adapter']} "
        "--apply --json"
    )
    raise SubstrateError(
        "deployment apply refused an environment-managed sealed worker",
        data={
            "deployment": deployment,
            "executable": False,
            "authority": {
                "kind": "sealed_environment",
                "environment_id": authority["environment_id"],
                "service_id": authority["service_id"],
            },
            "remedy": remedy,
        },
        fix_hint=remedy,
    )


def _default_supervisor_inventory(adapter: str) -> list[tuple[str, tuple[str, ...]]]:
    """Enumerate installed definitions, including inactive definitions."""
    if adapter == "launchd":
        roots = (
            Path.home() / "Library" / "LaunchAgents",
            Path("/Library/LaunchAgents"),
            Path("/Library/LaunchDaemons"),
        )
        definitions: list[tuple[str, tuple[str, ...]]] = []
        for root in roots:
            for path in sorted(root.glob("*.plist")):
                try:
                    raw = plistlib.loads(path.read_bytes())
                except Exception as exc:
                    if path.name.startswith("org.okg.runtime-worker."):
                        raise SubstrateError(
                            "deployment apply could not read a worker service definition",
                            data={"path": str(path), "error": str(exc)},
                        ) from exc
                    continue
                label = raw.get("Label") if isinstance(raw, Mapping) else None
                argv = raw.get("ProgramArguments") if isinstance(raw, Mapping) else None
                if isinstance(label, str) and isinstance(argv, list) and all(
                    isinstance(arg, str) for arg in argv
                ):
                    definitions.append((label, tuple(argv)))
                elif isinstance(label, str) and label.startswith("org.okg.runtime-worker."):
                    raise SubstrateError(
                        "deployment apply found an incomplete worker service definition",
                        data={"path": str(path), "service_id": label},
                    )
        return sorted(set(definitions))
    if adapter == "systemd":
        roots = (
            Path.home() / ".config" / "systemd" / "user",
            Path("/etc/systemd/system"),
            Path("/usr/local/lib/systemd/system"),
            Path("/usr/lib/systemd/system"),
        )
        definitions: list[tuple[str, tuple[str, ...]]] = []
        for root in roots:
            for path in root.rglob("*.service"):
                try:
                    lines = path.read_text(encoding="utf-8").splitlines()
                except OSError as exc:
                    if path.name.startswith("okg-runtime-worker@"):
                        raise SubstrateError(
                            "deployment apply could not read a worker service definition",
                            data={"path": str(path), "error": str(exc)},
                        ) from exc
                    continue
                for line in lines:
                    if line.strip().startswith("ExecStart="):
                        command = line.strip().removeprefix("ExecStart=")
                        try:
                            argv = tuple(shlex.split(command))
                        except ValueError as exc:
                            if path.name.startswith("okg-runtime-worker@"):
                                raise SubstrateError(
                                    "deployment apply found an incomplete worker service definition",
                                    data={"path": str(path), "service_id": path.name},
                                ) from exc
                            break
                        instance = path.name.partition("@")[2].removesuffix(".service")
                        if instance:
                            argv = tuple(instance if arg == "%i" else arg for arg in argv)
                        definitions.append((path.name, argv))
                        break
        return sorted(set(definitions))
    return []


def _command_targets_deployment(argv: tuple[str, ...], deployment: str) -> bool:
    is_worker = False
    for index, arg in enumerate(argv):
        if arg in {"-m", "--module"} and index + 1 < len(argv):
            module = argv[index + 1]
            if module == "okg.substrate.runtime.worker":
                is_worker = True
            elif module == "okg.substrate.cli.main" and tuple(
                argv[index + 2 : index + 4]
            ) == ("runtime", "worker"):
                is_worker = True
        if Path(arg).name == "okg" and tuple(argv[index + 1 : index + 3]) == (
            "runtime",
            "worker",
        ):
            is_worker = True
    if not is_worker:
        return False
    for index, arg in enumerate(argv):
        if arg == "--deployment" and index + 1 < len(argv):
            return argv[index + 1] == deployment
        if arg.startswith("--deployment="):
            return arg.removeprefix("--deployment=") == deployment
    return False


def _require_single_supervisor_definition(
    *, desired: Mapping[str, Any], deployment: str,
    inventory: SupervisorInventory | None,
) -> None:
    service = desired["service_authority"]
    adapter = str(service["adapter"])
    if adapter not in {"launchd", "systemd"}:
        return
    intended = str(service["service_id"])
    conflicts = [
        service_id
        for service_id, argv in (inventory or _default_supervisor_inventory)(adapter)
        if service_id != intended and _command_targets_deployment(argv, deployment)
    ]
    if conflicts:
        raise SubstrateError(
            "deployment apply refused conflicting worker supervisor definitions",
            data={
                "deployment": deployment,
                "executable": False,
                "adapter": adapter,
                "intended_service_id": intended,
                "conflicting_service_ids": conflicts,
            },
            fix_hint="inspect and retire the conflicting supervisor definition explicitly",
        )


def desired_deployment_state(
    *, deployment: str, deployment_dir: Path | str, dsn: str,
) -> dict[str, Any]:
    """Build the immutable desired-state closure used by plan and worker."""
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.catalog.ownership import catalog_identity_for
    from okg.substrate.deployments import (
        DeploymentManifest,
        resolve_source_registry_path,
    )
    from okg.substrate.deployments.sync_root import require_resolved_sync_root
    from okg.substrate.nomos.baseline import deployment_baseline_spec
    from okg.substrate.runtime.source_poll_schedule import source_poll_schedule_ids
    from okg.substrate.sources.registry import load_source_registry

    root = Path(deployment_dir).expanduser().resolve()
    manifest_path = root / "deployment.yaml"
    raw_manifest = fastyaml.safe_load(manifest_path.read_text()) or {}
    if not isinstance(raw_manifest, Mapping):
        raise SubstrateError("deployment manifest must be a mapping")
    manifest = DeploymentManifest.from_yaml_lenient(manifest_path)
    # The manifest may name an authoritative checkout this machine cannot
    # resolve. Continuing would hand `sync_root=None` to both
    # `_runtime_working_directory` -- which derives the worker's working
    # directory, import_root, and code_revision from whatever checkout this
    # process happens to run in, and folds that into desired_state_digest --
    # and to the source registry's repo_root_override below (issue #1014).
    require_resolved_sync_root(manifest, command="deployment apply")
    # Prefer the manifest's own resolved path: it has been through placeholder
    # substitution (${manifest_dir}, ${repo_root}, ...), which the raw YAML
    # read above has not.  Resolving from raw YAML leaves a placeholder as a
    # literal path that never exists, which would silently fingerprint an
    # empty registry and converge on the wrong desired state.
    declared = raw_manifest.get("source_registry")
    registry_path = Path(
        getattr(manifest, "source_registry", None)
        or resolve_source_registry_path(root, dict(raw_manifest))
    )
    if registry_path.is_file():
        raw_registry = fastyaml.safe_load(registry_path.read_text()) or {}
    elif declared:
        # A declared-but-absent registry is a configuration error, never an
        # empty desired state.  Silent-fallback-to-defaults is a P0 here.
        raise SubstrateError(
            "declared source registry does not exist",
            data={"path": str(registry_path), "declared": str(declared)},
        )
    else:
        raw_registry = {}
    if not isinstance(raw_registry, Mapping):
        raise SubstrateError("source registry must be a mapping")
    composed = compose_catalog(root)
    catalog_identity = catalog_identity_for(
        composed=composed,
        deployment_dir=root,
        command="okg deployment apply",
    )
    nomos = deployment_baseline_spec(
        deployment=deployment,
        deployment_dir=root,
    )
    runtime = raw_manifest.get("runtime") or {}
    if not isinstance(runtime, Mapping):
        raise SubstrateError("deployment runtime block must be a mapping")
    typed_registry = load_source_registry(
        registry_path,
        **(
            {"repo_root_override": manifest.sync_root}
            if manifest.sync_root is not None
            else {}
        ),
    )
    source_registry_fingerprint = _digest(raw_registry)
    source_fingerprints = {
        str(name): _digest(entry.raw)
        for name, entry in sorted(typed_registry.items())
    }
    runtime_fingerprint = _digest(runtime)
    source_sync = runtime.get("source_sync") or {}
    poll = source_sync.get("poll") if isinstance(source_sync, Mapping) else {}
    poll_enabled = not isinstance(poll, Mapping) or bool(poll.get("enabled", True))
    schedules = runtime.get("schedules") or {}
    schedule_overrides = dict(schedules) if isinstance(schedules, Mapping) else {}
    expected_schedules = (
        source_poll_schedule_ids(
            typed_registry,
            schedule_overrides=schedule_overrides,
        )
        if bool(runtime.get("enabled", False)) and poll_enabled
        else []
    )
    target = _target_identity(dsn)
    contract_block = raw_manifest.get("contract")
    supervisor = resolve_supervisor_adapter(
        declared=runtime.get("supervisor"),
        deployment=deployment,
        runtime_enabled=bool(runtime.get("enabled", False)),
        contract_version=(
            contract_block.get("version")
            if isinstance(contract_block, Mapping)
            else None
        ),
    )
    service_authority = _tracking_service_authority(
        deployment=deployment,
        deployment_dir=root,
        sync_root=manifest.sync_root,
        target=target,
        supervisor=supervisor,
    )
    schedule_fingerprint = _digest(expected_schedules)
    service_authority["release"] = {
        "deployment_root_hash": catalog_identity.deployment_root_hash,
        "manifest_fingerprint": catalog_identity.manifest_fingerprint,
        "catalog_fingerprint": catalog_identity.catalog_fingerprint,
        "schema_fingerprint": catalog_identity.schema_fingerprint,
        "writer_fingerprint": catalog_identity.writer_fingerprint,
        "source_registry_fingerprint": source_registry_fingerprint,
        "schedule_fingerprint": schedule_fingerprint,
    }
    closure = {
        "deployment": deployment,
        "runtime_enabled": bool(runtime.get("enabled", False)),
        "target": target,
        "service_authority": service_authority,
        "deployment_root_hash": catalog_identity.deployment_root_hash,
        "manifest_fingerprint": catalog_identity.manifest_fingerprint,
        "catalog_fingerprint": catalog_identity.catalog_fingerprint,
        "schema_fingerprint": catalog_identity.schema_fingerprint,
        "writer_fingerprint": catalog_identity.writer_fingerprint,
        "source_registry_fingerprint": source_registry_fingerprint,
        "source_fingerprints": source_fingerprints,
        "source_names": sorted(typed_registry),
        "source_schedule_ids": expected_schedules,
        "schedule_fingerprint": schedule_fingerprint,
        "runtime_fingerprint": runtime_fingerprint,
        "worker_config_fingerprint": _digest({
            "source_registry_fingerprint": source_registry_fingerprint,
            "source_fingerprints": source_fingerprints,
            "runtime_fingerprint": runtime_fingerprint,
            "source_names": sorted(typed_registry),
            "source_schedule_ids": expected_schedules,
        }),
        "nomos_policy_fingerprint": nomos.get("policy_fingerprint"),
        "nomos_config_fingerprint": nomos.get("config_fingerprint"),
    }
    return {
        **closure,
        "desired_state_digest": _digest(closure),
        "deployment_dir": str(root),
        # Outside the digest: lets the ownership read recognize this
        # writer's pre-#2229 (schema-derived) owner key (#2229).
        "worktree_root_hash": catalog_identity.worktree_root_hash,
    }


def _table_exists(conn: psycopg.Connection, name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (name,))
        return cur.fetchone()[0] is not None


def ensure_deployment_apply_schema(conn: psycopg.Connection) -> None:
    """Install the additive receipt/ack tables on explicit apply/worker boot."""
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS okg")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS okg.deployment_apply_runs (
              deployment_name TEXT NOT NULL,
              desired_state_digest TEXT NOT NULL,
              plan_id TEXT NOT NULL,
              status TEXT NOT NULL CHECK (
                status IN ('applying','failed','complete')
              ),
              current_stage TEXT,
              desired_state JSONB NOT NULL,
              completed_stages JSONB NOT NULL DEFAULT '[]'::jsonb,
              stage_receipts JSONB NOT NULL DEFAULT '{}'::jsonb,
              error JSONB,
              started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              completed_at TIMESTAMPTZ,
              PRIMARY KEY (deployment_name, desired_state_digest)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS okg.deployment_worker_ack (
              deployment_name TEXT PRIMARY KEY,
              desired_state_digest TEXT NOT NULL,
              source_registry_fingerprint TEXT NOT NULL,
              source_fingerprints JSONB,
              runtime_fingerprint TEXT,
              worker_config_fingerprint TEXT,
              service_authority JSONB,
              observed_runtime_authority JSONB,
              manifest_fingerprint TEXT,
              catalog_fingerprint TEXT,
              schema_fingerprint TEXT,
              writer_fingerprint TEXT,
              schedule_fingerprint TEXT,
              active_sources JSONB NOT NULL,
              active_schedules JSONB NOT NULL,
              acknowledged_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        cur.execute(
            "ALTER TABLE okg.deployment_worker_ack "
            "ADD COLUMN IF NOT EXISTS source_fingerprints JSONB"
        )
        cur.execute(
            "ALTER TABLE okg.deployment_worker_ack "
            "ADD COLUMN IF NOT EXISTS runtime_fingerprint TEXT"
        )
        cur.execute(
            "ALTER TABLE okg.deployment_worker_ack "
            "ADD COLUMN IF NOT EXISTS worker_config_fingerprint TEXT"
        )
        cur.execute(
            "ALTER TABLE okg.deployment_worker_ack "
            "ADD COLUMN IF NOT EXISTS service_authority JSONB"
        )
        cur.execute(
            "ALTER TABLE okg.deployment_worker_ack "
            "ADD COLUMN IF NOT EXISTS observed_runtime_authority JSONB"
        )
        for column in (
            "manifest_fingerprint",
            "catalog_fingerprint",
            "schema_fingerprint",
            "writer_fingerprint",
            "schedule_fingerprint",
        ):
            cur.execute(
                "ALTER TABLE okg.deployment_worker_ack "
                f"ADD COLUMN IF NOT EXISTS {column} TEXT"
            )


def _read_receipt(conn: psycopg.Connection, deployment: str) -> dict[str, Any] | None:
    if not _table_exists(conn, "okg.deployment_apply_runs"):
        return None
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT * FROM okg.deployment_apply_runs
             WHERE deployment_name = %s
             ORDER BY updated_at DESC LIMIT 1
            """,
            (deployment,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def _read_worker_ack(conn: psycopg.Connection, deployment: str) -> dict[str, Any] | None:
    if not _table_exists(conn, "okg.deployment_worker_ack"):
        return None
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT * FROM okg.deployment_worker_ack WHERE deployment_name = %s",
            (deployment,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def _observed_runtime_authority(
    *,
    deployment: str,
    deployment_dir: Path | str,
    dsn: str,
) -> dict[str, Any]:
    """Measure the process that is emitting a worker acknowledgment.

    ``deployment_dir`` is accepted so tests can replace this probe with a
    deterministic process fixture. The production probe deliberately does not
    derive path or revision fields from it: those fields must describe this
    running interpreter and its imported checkout, not the desired manifest.
    """
    del deployment_dir
    working_directory = Path.cwd().resolve()
    imported_file = Path(__file__).resolve()
    try:
        code_revision = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(working_directory),
            capture_output=True,
            text=True,
            timeout=5,
            check=True,
        ).stdout.strip() or None
    except Exception:  # noqa: BLE001 - the mismatch gate will fail closed.
        code_revision = None
    if code_revision is None:
        # No .git here. Inside the runtime image that is expected, and the build
        # stamped the revision into the environment; this reads the same fact
        # from the source that has it, and stays None when neither does so the
        # gate still refuses.
        code_revision = container_code_revision()
    from okg.substrate.runtime.code_identity import installed_code_revision

    return {
        "authority_source": "running_worker_process",
        "deployment": deployment,
        "working_directory": str(working_directory),
        "process_executable": str(Path(sys.executable).absolute()),
        "interpreter": str(Path(sys.executable).absolute()),
        "python_prefix": str(Path(sys.prefix).resolve()),
        "import_root": str(imported_file.parents[2]),
        "code_revision": code_revision,
        "installed_code_revision": installed_code_revision().revision,
        "database": {
            key: _target_identity(dsn).get(key)
            for key in ("host", "port", "dbname")
        },
    }


def _worker_ack_matches(
    desired: Mapping[str, Any],
    ack: Mapping[str, Any] | None,
) -> bool:
    """Require one exact desired-state and observed-process acknowledgment."""
    return not _worker_ack_mismatches(desired, ack)


def _worker_ack_mismatches(
    desired: Mapping[str, Any],
    ack: Mapping[str, Any] | None,
) -> list[str]:
    """Every reason the ack does not match, in the gate's own order.

    The gate is right to fail closed, but a bare ``False`` reaches the operator
    ten minutes later as ``helm --wait`` deadline exceeded with no cause. That
    is a diagnosis gap, not a policy one: a containerised runtime cannot satisfy
    a host-supervisor identity (working_directory, process_executable,
    interpreter and import_root all describe a launchd/systemd checkout, and
    code_revision is None because the image carries no .git), and nothing in the
    failure said so. Naming the disagreeing fields costs nothing and turns a
    multi-hour trace into one line.
    """

    if not ack:
        return ["no worker acknowledgment recorded"]
    reasons: list[str] = []
    fingerprint_fields = (
        "desired_state_digest",
        "source_registry_fingerprint",
        "runtime_fingerprint",
        "worker_config_fingerprint",
        "manifest_fingerprint",
        "catalog_fingerprint",
        "schema_fingerprint",
        "writer_fingerprint",
        "schedule_fingerprint",
    )
    reasons.extend(
        f"fingerprint {field} differs"
        for field in fingerprint_fields
        if ack.get(field) != desired.get(field)
    )
    if dict(ack.get("source_fingerprints") or {}) != dict(
        desired.get("source_fingerprints") or {}
    ):
        reasons.append("source_fingerprints differ")
    if sorted(ack.get("active_sources") or []) != list(
        desired.get("source_names") or []
    ):
        reasons.append("active_sources differ")
    if sorted(ack.get("active_schedules") or []) != list(
        desired.get("source_schedule_ids") or []
    ):
        reasons.append("active_schedules differ")
    if reasons:
        return reasons

    expected = dict(desired.get("service_authority") or {})
    observed = dict(ack.get("observed_runtime_authority") or {})
    identity_fields = (
        PROCESS_IDENTITY_FIELDS
        if expected.get("adapter") == PROCESS_ADAPTER
        else (
            "working_directory",
            "process_executable",
            "interpreter",
            "import_root",
            "code_revision",
            "database",
        )
    )
    if bool(desired.get("runtime_enabled")):
        for field in identity_fields:
            if not expected.get(field):
                reasons.append(
                    f"identity {field} missing from desired authority"
                    + (
                        f" ({expected['installed_code_revision_reason']})"
                        if field == "installed_code_revision"
                        and expected.get("installed_code_revision_reason")
                        else ""
                    )
                )
            elif not observed.get(field):
                reasons.append(
                    f"identity {field} missing from the observed process"
                    + (
                        " (no .git in the image?)"
                        if field == "code_revision"
                        else ""
                    )
                )
        if reasons:
            return reasons

    reasons.extend(
        f"identity {field} differs: desired={expected.get(field)!r} "
        f"observed={observed.get(field)!r}"
        for field in identity_fields
        if observed.get(field) != expected.get(field)
    )
    return reasons


def _ownership_state(
    conn: psycopg.Connection, *, deployment: str, desired: Mapping[str, Any],
) -> dict[str, Any]:
    if not _table_exists(conn, "okg.deployment_write_ownership"):
        return {"status": "unclaimed", "owner": None}
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT deployment_name, owner_fingerprint, schema_fingerprint,
                   catalog_fingerprint, manifest_fingerprint, status
              FROM okg.deployment_write_ownership
             WHERE deployment_name = %s
            """,
            (deployment,),
        )
        owner = cur.fetchone()
    if owner is None:
        return {"status": "unclaimed", "owner": None}
    owner = dict(owner)
    from okg.substrate.catalog.ownership import (
        WRITER_SCOPE_DEPLOYMENT_ROOT,
        legacy_writer_fingerprint,
    )

    # #2229: deployment apply writes under deployment_root scope. A row
    # still keyed by this writer's v1 (schema-derived) key is the same
    # writer; the ownership stage's claim re-keys it.
    legacy_key = legacy_writer_fingerprint(
        deployment_name=deployment,
        schema_fingerprint=owner.get("schema_fingerprint"),
        writer_scope=WRITER_SCOPE_DEPLOYMENT_ROOT,
        environment_id=None,
        deployment_root_hash=desired.get("deployment_root_hash"),
        worktree_root_hash=desired.get("worktree_root_hash"),
    )
    if (
        legacy_key is not None
        and "worktree_root_hash" in desired
        and owner.get("owner_fingerprint") == legacy_key
    ):
        return {
            "status": "refresh_needed",
            "owner": owner,
            "reason": "same_writer_legacy_key",
        }
    if owner.get("owner_fingerprint") != desired["writer_fingerprint"]:
        return {
            "status": "owner_mismatch",
            "owner": owner,
            "recovery_command": (
                f"{okg_prefix()} catalog ownership diagnose --deployment "
                f"{desired['deployment_dir']} --json"
            ),
        }
    if (
        owner.get("schema_fingerprint") != desired["schema_fingerprint"]
        or owner.get("catalog_fingerprint") != desired["catalog_fingerprint"]
        or owner.get("manifest_fingerprint") != desired["manifest_fingerprint"]
    ):
        return {
            "status": "refresh_needed",
            "owner": owner,
            "reason": "same_writer_desired_state_changed",
        }
    return {"status": "owned", "owner": owner}


def _latest_catalog_fingerprint(conn: psycopg.Connection, deployment: str) -> str | None:
    if not _table_exists(conn, "okg.catalog_versions"):
        return None
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT catalog_fingerprint FROM okg.catalog_versions
             WHERE deployment_name = %s AND status = 'applied'
             ORDER BY catalog_version_id DESC LIMIT 1
            """,
            (deployment,),
        )
        row = cur.fetchone()
    return str(row[0]) if row and row[0] else None


def _nomos_live_state(
    *, deployment: str, deployment_dir: str, dsn: str,
) -> dict[str, Any]:
    from okg.substrate.nomos.baseline import reconcile_deployment_baseline

    return reconcile_deployment_baseline(
        deployment=deployment,
        deployment_dir=deployment_dir,
        dsn=dsn,
        apply=False,
    )


def plan_deployment_apply(
    *, deployment: str, deployment_dir: Path | str, dsn: str,
    supervisor_inventory: SupervisorInventory | None = None,
) -> dict[str, Any]:
    """Return a read-only ordered plan.  No schema or receipt writes occur."""
    from okg.substrate.deployments.admission import inactive_plan

    archived = inactive_plan(deployment_dir)
    if archived is not None:
        return archived
    _require_tracking_apply_authority(
        deployment=deployment, deployment_dir=deployment_dir, dsn=dsn,
    )
    desired = desired_deployment_state(
        deployment=deployment, deployment_dir=deployment_dir, dsn=dsn,
    )
    _require_single_supervisor_definition(
        desired=desired, deployment=deployment, inventory=supervisor_inventory,
    )
    try:
        with db_connect(dsn, autocommit=True) as conn:
            ownership = _ownership_state(
                conn, deployment=deployment, desired=desired,
            )
            catalog_fingerprint = _latest_catalog_fingerprint(conn, deployment)
            receipt = _read_receipt(conn, deployment)
            ack = _read_worker_ack(conn, deployment)
    except psycopg.Error as exc:
        raise SubstrateError(
            "deployment apply could not inspect live state",
            data={"deployment": deployment, "error": str(exc)},
            fix_hint="fix the deployment DSN/database before applying",
        ) from exc
    if ownership["status"] == "owner_mismatch":
        raise SubstrateError(
            "deployment apply refused: catalog ownership belongs to another writer",
            data=ownership,
            fix_hint=str(ownership["recovery_command"]),
        )
    nomos = _nomos_live_state(
        deployment=deployment,
        deployment_dir=desired["deployment_dir"],
        dsn=dsn,
    )
    ack_source_fingerprints = dict(
        (ack or {}).get("source_fingerprints") or {}
    )
    runtime_enabled = bool(desired.get("runtime_enabled"))
    enqueue_sources = sorted(
        name
        for name, fingerprint in desired["source_fingerprints"].items()
        if ack_source_fingerprints.get(name) != fingerprint
    )
    removed_sources = sorted(
        set((ack or {}).get("active_sources") or [])
        - set(desired["source_names"])
    )
    source_ack_same = bool(
        ack
        and ack_source_fingerprints == desired["source_fingerprints"]
        and sorted(ack.get("active_sources") or []) == desired["source_names"]
    )
    worker_ack_same = bool(source_ack_same and _worker_ack_matches(desired, ack))
    worker_ack_mismatches = (
        [] if ack is None or worker_ack_same
        else _worker_ack_mismatches(desired, ack)
    )
    # Issue #2502: when every fingerprint matches and only the process
    # identity differs or is missing (a worker started by hand rather than by
    # the supervisor unit apply installs, or a checkout with no code
    # revision), say so instead of calling it fingerprint drift.
    worker_ack_identity_only = bool(
        worker_ack_mismatches
        and all(item.startswith("identity ") for item in worker_ack_mismatches)
    )
    actions = [
        {
            "stage": "catalog_ownership",
            "needed": ownership["status"] in {"unclaimed", "refresh_needed"},
            "reason": ownership["status"],
        },
        {
            "stage": "catalog_apply",
            "needed": catalog_fingerprint != desired["catalog_fingerprint"],
            "reason": "catalog_fingerprint_drift" if catalog_fingerprint != desired["catalog_fingerprint"] else "converged",
        },
        {
            "stage": "nomos_baseline",
            "needed": bool(nomos.get("required") and not nomos.get("converged")),
            "reason": "nomos_fingerprint_drift" if nomos.get("required") and not nomos.get("converged") else "converged_or_disabled",
        },
        # Issue #3628: source supervision enqueues each changed source into
        # the runtime's job queue (`okg jobs enqueue-source`). With the
        # runtime off there is no queue -- the tables do not exist and
        # `okg ingest` runs sources inline -- so the stage is not needed,
        # rather than failing `--apply` after the catalog stage succeeded.
        {
            "stage": "source_supervision",
            "needed": bool(enqueue_sources) and runtime_enabled,
            "sources": enqueue_sources if runtime_enabled else [],
            "removed_sources": removed_sources,
            "reason": (
                "not_needed_runtime_disabled" if not runtime_enabled else
                "source_added_or_changed" if enqueue_sources else (
                    "removal_deferred_to_worker_reload"
                    if removed_sources else "converged"
                )
            ),
        },
        # Same issue: `okg runtime worker` refuses to start with the runtime
        # off, so installing or restarting its service cannot converge
        # anything (Jason, 2026-10-09, `u3runtimeoff A`).
        {
            "stage": "worker_reload",
            "needed": not worker_ack_same and runtime_enabled,
            # An absent ack is unknown: no worker has booted under this code
            # yet. Runtime-enabled deployments fail closed on that missing
            # authority receipt; runtime-disabled deployments demote it to a
            # migration-safe warning in `deployment_apply_health`.
            "reason": (
                "not_needed_runtime_disabled" if not runtime_enabled
                else "worker_ack_absent" if ack is None
                else "converged" if worker_ack_same
                else "worker_ack_identity_mismatch" if worker_ack_identity_only
                else "worker_ack_fingerprint_drift"
            ),
            "mismatches": worker_ack_mismatches,
        },
    ]
    plan_payload = {
        "deployment": deployment,
        "target": desired["target"],
        "desired_state_digest": desired["desired_state_digest"],
        "actions": actions,
    }
    plan_id = _digest(plan_payload)
    pending = [row["stage"] for row in actions if row["needed"]]
    # Issue #3580: a dry run printed `ok: True` and nothing else an operator
    # could act on, so a needed catalog reload read as done and the next
    # ingest refused. `ok` still means the plan was computed; these say what
    # was NOT done and the exact command that does it.
    next_command = (
        hint_command(okg_command(
            "deployment apply", deployment, apply=True, confirm=plan_id,
        ))
        if pending else None
    )
    summary = (
        f"dry run: nothing was applied; {len(pending)} stage(s) pending "
        f"({', '.join(pending)}). Apply them with `{next_command}`"
        if pending else "dry run: the deployment is converged; nothing to apply"
    )
    return {
        "schema": SCHEMA,
        "summary": summary,
        "ok": True,
        "mode": "dry-run",
        "applied": False,
        "pending_stages": pending,
        "next_command": next_command,
        "deployment": deployment,
        "deployment_dir": desired["deployment_dir"],
        "desired_state": desired,
        "plan_id": plan_id,
        "actions": actions,
        "no_op": not pending,
        "live": {
            "ownership": ownership,
            "catalog_fingerprint": catalog_fingerprint,
            "nomos": nomos,
            "receipt": receipt,
            "worker_ack": ack,
        },
    }


def _default_runner(argv: list[str], env: Mapping[str, str]) -> ApplyCommandResult:
    proc = subprocess.run(
        argv,
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, **env},
    )
    return ApplyCommandResult(proc.returncode, proc.stdout, proc.stderr)


def _record_run(
    conn: psycopg.Connection,
    *,
    plan: Mapping[str, Any],
    status: str,
    current_stage: str | None,
    completed_stages: list[str],
    receipts: Mapping[str, Any],
    error: Mapping[str, Any] | None = None,
) -> None:
    desired = plan["desired_state"]
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.deployment_apply_runs (
              deployment_name, desired_state_digest, plan_id, status,
              current_stage, desired_state, completed_stages, stage_receipts,
              error, updated_at, completed_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),
                      CASE WHEN %s = 'complete' THEN now() ELSE NULL END)
            ON CONFLICT (deployment_name, desired_state_digest) DO UPDATE SET
              plan_id = EXCLUDED.plan_id, status = EXCLUDED.status,
              current_stage = EXCLUDED.current_stage,
              desired_state = EXCLUDED.desired_state,
              completed_stages = EXCLUDED.completed_stages,
              stage_receipts = EXCLUDED.stage_receipts,
              error = EXCLUDED.error, updated_at = now(),
              completed_at = EXCLUDED.completed_at
            """,
            (
                plan["deployment"], desired["desired_state_digest"],
                plan["plan_id"], status, current_stage, Jsonb(dict(desired)),
                Jsonb(completed_stages), Jsonb(_json_safe(dict(receipts))),
                Jsonb(dict(error)) if error else None, status,
            ),
        )


def _recheck_digest(plan: Mapping[str, Any], dsn: str) -> None:
    current = desired_deployment_state(
        deployment=str(plan["deployment"]),
        deployment_dir=str(plan["deployment_dir"]),
        dsn=dsn,
    )
    if current["desired_state_digest"] != plan["desired_state"]["desired_state_digest"]:
        raise SubstrateError(
            "deployment apply plan became stale during execution",
            data={
                "planned": plan["desired_state"]["desired_state_digest"],
                "current": current["desired_state_digest"],
            },
            fix_hint="run dry-run again and confirm the new plan id",
        )


def _apply_catalog(
    conn: psycopg.Connection, plan: Mapping[str, Any], *, claim: bool,
) -> dict[str, Any]:
    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.catalog.ownership import (
        catalog_identity_for,
        claim_catalog_ownership,
        ensure_catalog_write_allowed,
    )

    root = Path(str(plan["deployment_dir"]))
    composed = compose_catalog(root)
    identity = catalog_identity_for(
        composed=composed,
        deployment_dir=root,
        command="okg deployment apply",
    )
    if claim:
        report = claim_catalog_ownership(conn, identity=identity, force=False)
        conn.commit()
        return {"claimed": True, "status": report.get("status")}
    ensure_catalog_write_allowed(conn, identity=identity)
    cv = apply_composed_catalog(conn, composed, identity=identity)
    conn.commit()
    return {"catalog_version_id": cv, "catalog_fingerprint": identity.catalog_fingerprint}


def record_worker_acknowledgment(
    *,
    dsn: str,
    deployment: str,
    deployment_dir: Path | str,
    active_sources: list[str],
    active_schedules: list[str],
) -> dict[str, Any]:
    """Persist desired state plus the replacement worker's observed identity."""
    desired = desired_deployment_state(
        deployment=deployment, deployment_dir=deployment_dir, dsn=dsn,
    )
    observed_runtime_authority = _observed_runtime_authority(
        deployment=deployment,
        deployment_dir=deployment_dir,
        dsn=dsn,
    )
    if sorted(active_sources) != desired["source_names"]:
        raise SubstrateError(
            "worker acknowledgment source set does not match desired registry",
            data={"desired": desired["source_names"], "active": sorted(active_sources)},
        )
    if sorted(active_schedules) != desired["source_schedule_ids"]:
        raise SubstrateError(
            "worker acknowledgment schedule set does not match desired registry",
            data={
                "desired": desired["source_schedule_ids"],
                "active": sorted(active_schedules),
            },
        )
    with db_connect(dsn) as conn:
        ensure_deployment_apply_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.deployment_worker_ack (
                  deployment_name, desired_state_digest,
                  source_registry_fingerprint, source_fingerprints,
                  runtime_fingerprint,
                  worker_config_fingerprint, service_authority,
                  observed_runtime_authority,
                  manifest_fingerprint, catalog_fingerprint,
                  schema_fingerprint, writer_fingerprint,
                  schedule_fingerprint, active_sources,
                  active_schedules, acknowledged_at
                ) VALUES (
                  %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now()
                )
                ON CONFLICT (deployment_name) DO UPDATE SET
                  desired_state_digest = EXCLUDED.desired_state_digest,
                  source_registry_fingerprint = EXCLUDED.source_registry_fingerprint,
                  source_fingerprints = EXCLUDED.source_fingerprints,
                  runtime_fingerprint = EXCLUDED.runtime_fingerprint,
                  worker_config_fingerprint = EXCLUDED.worker_config_fingerprint,
                  service_authority = EXCLUDED.service_authority,
                  observed_runtime_authority =
                    EXCLUDED.observed_runtime_authority,
                  manifest_fingerprint = EXCLUDED.manifest_fingerprint,
                  catalog_fingerprint = EXCLUDED.catalog_fingerprint,
                  schema_fingerprint = EXCLUDED.schema_fingerprint,
                  writer_fingerprint = EXCLUDED.writer_fingerprint,
                  schedule_fingerprint = EXCLUDED.schedule_fingerprint,
                  active_sources = EXCLUDED.active_sources,
                  active_schedules = EXCLUDED.active_schedules,
                  acknowledged_at = now()
                """,
                (
                    deployment, desired["desired_state_digest"],
                    desired["source_registry_fingerprint"],
                    Jsonb(desired["source_fingerprints"]),
                    desired["runtime_fingerprint"],
                    desired["worker_config_fingerprint"],
                    Jsonb(desired["service_authority"]),
                    Jsonb(observed_runtime_authority),
                    desired["manifest_fingerprint"],
                    desired["catalog_fingerprint"],
                    desired["schema_fingerprint"],
                    desired["writer_fingerprint"],
                    desired["schedule_fingerprint"],
                    Jsonb(sorted(active_sources)), Jsonb(sorted(active_schedules)),
                ),
            )
        conn.commit()
    return {
        "ok": True,
        "desired_state_digest": desired["desired_state_digest"],
        "observed_runtime_authority": observed_runtime_authority,
    }


def clear_worker_acknowledgment(*, dsn: str, deployment: str) -> None:
    """Invalidate any prior worker ack before a replacement boot begins.

    Deliberately does NOT install the schema.  A worker boot must not run
    DDL outside the migration system; doing so on every boot turns any
    missing CREATE grant, lock contention, or read-only standby into a
    startup failure.  When the table is absent there is no prior ack to
    invalidate, so the clear is a no-op.
    """
    with db_connect(dsn) as conn:
        if not _table_exists(conn, "okg.deployment_worker_ack"):
            return
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM okg.deployment_worker_ack "
                "WHERE deployment_name = %s",
                (deployment,),
            )
        conn.commit()


def _wait_for_ack(
    *, dsn: str, plan: Mapping[str, Any], timeout_seconds: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while True:
        with db_connect(dsn, autocommit=True) as conn:
            ack = _read_worker_ack(conn, str(plan["deployment"]))
        mismatches = _worker_ack_mismatches(plan["desired_state"], ack)
        if not mismatches:
            return ack
        if time.monotonic() >= deadline:
            raise SubstrateError(
                "replacement worker did not acknowledge the applied configuration: "
                + "; ".join(mismatches),
                data={
                    "mismatches": mismatches,
                    "last_ack": ack,
                    "desired_service_authority": dict(
                        (plan["desired_state"] or {}).get("service_authority") or {}
                    ),
                    "timeout_seconds": timeout_seconds,
                },
                fix_hint=(
                    hint_command(okg_command(
                        "deployment apply", plan["deployment"], json=True,
                    ))
                ),
            )
        time.sleep(0.25)


def _validate_source_enqueue(
    *, dsn: str, deployment: str, source: str, run_key: str, payload: Any,
) -> dict[str, Any]:
    from okg.substrate.runtime.partition_queue import source_sync_queue_name

    expected_queue = source_sync_queue_name(deployment)
    if not isinstance(payload, Mapping) or payload.get("ok") is not True:
        raise SubstrateError(
            f"source supervision enqueue returned invalid success for {source}",
            data={"payload": payload},
        )
    workflow_id = str(payload.get("dbos_workflow_id") or "").strip()
    if (
        payload.get("schema") != "okg.source_sync.enqueue.v1"
        or payload.get("execution_authority") != "dbos.workflow_status"
        or payload.get("runtime_deployment") != deployment
        or payload.get("source") != source
        or payload.get("deduplication_id") != run_key
        or not workflow_id
    ):
        raise SubstrateError(
            f"source supervision enqueue postcondition is malformed for {source}",
            data={"payload": payload, "expected_run_key": run_key},
        )
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('dbos.workflow_status')")
        if cur.fetchone()[0] is None:
            raise SubstrateError(
                "source supervision cannot observe DBOS workflow state",
                data={"source": source, "workflow_id": workflow_id},
            )
        cur.execute(
            "SELECT status, name, deduplication_id, queue_name "
            "FROM dbos.workflow_status WHERE workflow_uuid = %s",
            (workflow_id,),
        )
        row = cur.fetchone()
    if (
        row is None
        or str(row[1] or "") != "okg_source_sync"
        or str(row[2] or "") != run_key
        or str(row[3] or "") != expected_queue
    ):
        raise SubstrateError(
            f"source supervision workflow was not durably observed for {source}",
            data={
                "workflow_id": workflow_id,
                "row": row,
                "run_key": run_key,
                "expected_name": "okg_source_sync",
                "expected_queue": expected_queue,
            },
        )
    return {
        "payload": dict(payload),
        "observed_workflow": {
            "workflow_id": workflow_id,
            "status": str(row[0]),
            "name": str(row[1]),
            "deduplication_id": str(row[2]),
            "queue_name": str(row[3]),
        },
    }


def apply_deployment_plan(
    *,
    deployment: str,
    deployment_dir: Path | str,
    dsn: str,
    confirm: str,
    command_runner: CommandRunner | None = None,
    worker_ack_timeout_seconds: float = 30.0,
    supervisor_inventory: SupervisorInventory | None = None,
) -> dict[str, Any]:
    """Apply exactly one fresh plan under a deployment advisory lock."""
    from okg.substrate.deployments.admission import require_active_deployment
    require_active_deployment(deployment_dir)
    _require_tracking_apply_authority(
        deployment=deployment, deployment_dir=deployment_dir, dsn=dsn,
    )
    plan = plan_deployment_apply(
        deployment=deployment, deployment_dir=deployment_dir, dsn=dsn,
        supervisor_inventory=supervisor_inventory,
    )
    if confirm != plan["plan_id"]:
        raise SubstrateError(
            "deployment apply confirmation does not match the current plan",
            data={"expected_plan_id": plan["plan_id"], "confirmed": confirm},
            fix_hint="run the dry-run again and pass --confirm <plan-id> --apply",
        )
    runner = command_runner or _default_runner
    completed: list[str] = []
    receipts: dict[str, Any] = {}
    with db_connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_try_advisory_lock(hashtext(%s), hashtext(%s))",
                ("okg.deployment.apply", deployment),
            )
            locked = bool(cur.fetchone()[0])
        if not locked:
            raise SubstrateError(
                "another deployment apply holds the deployment lock",
                data={"deployment": deployment},
                fix_hint="wait for the active apply or inspect its receipt",
            )
        run_started = False
        try:
            # A competing apply can converge after the initial plan but before
            # this caller gets the deployment lock. Re-plan while holding the
            # lock so a stale confirmation can never authorize old actions.
            locked_plan = plan_deployment_apply(
                deployment=deployment,
                deployment_dir=deployment_dir,
                dsn=dsn,
                supervisor_inventory=supervisor_inventory,
            )
            if confirm != locked_plan["plan_id"]:
                raise SubstrateError(
                    "deployment apply confirmation became stale before the "
                    "deployment lock was acquired",
                    data={
                        "initial_plan_id": plan["plan_id"],
                        "expected_plan_id": locked_plan["plan_id"],
                        "confirmed": confirm,
                    },
                    fix_hint=(
                        "run the dry-run again and pass its new plan id to "
                        "--confirm with --apply"
                    ),
                )
            plan = locked_plan
            if plan["no_op"]:
                return {
                    **plan,
                    "summary": "applied nothing: the deployment was already converged",
                    "mode": "apply",
                    "applied": False,
                    "write_free": True,
                }

            ensure_deployment_apply_schema(conn)
            conn.commit()
            latest = _read_receipt(conn, deployment)
            if (
                latest
                and latest.get("desired_state_digest")
                == plan["desired_state"]["desired_state_digest"]
                and latest.get("status") in {"applying", "failed"}
            ):
                completed = list(latest.get("completed_stages") or [])
                receipts = dict(latest.get("stage_receipts") or {})
            run_started = True
            _record_run(
                conn, plan=plan, status="applying", current_stage=None,
                completed_stages=completed, receipts=receipts,
            )
            conn.commit()
            process_reload: Mapping[str, Any] | None = None
            for action in plan["actions"]:
                stage = str(action["stage"])
                # A receipt proves the postcondition was observed when the
                # stage finished; it does not authorize skipping a stage that
                # has drifted again before this resume.  The fresh plan is the
                # live authority, so rerun an idempotent action whenever it is
                # currently needed even if an older receipt exists.
                if not action["needed"]:
                    continue
                _recheck_digest(plan, dsn)
                _record_run(
                    conn, plan=plan, status="applying", current_stage=stage,
                    completed_stages=completed, receipts=receipts,
                )
                conn.commit()
                if stage == "catalog_ownership":
                    receipt = _apply_catalog(conn, plan, claim=True)
                elif stage == "catalog_apply":
                    receipt = _apply_catalog(conn, plan, claim=False)
                elif stage == "nomos_baseline":
                    from okg.substrate.nomos.baseline import reconcile_deployment_baseline
                    receipt = reconcile_deployment_baseline(
                        deployment=deployment,
                        deployment_dir=deployment_dir,
                        dsn=dsn,
                        apply=True,
                        desired_state_digest=plan["desired_state"]["desired_state_digest"],
                    )
                    if not receipt.get("converged"):
                        raise SubstrateError("Nomos baseline did not converge")
                elif stage == "source_supervision":
                    rows = []
                    for source in action.get("sources") or []:
                        run_key = (
                            "deployment-apply:"
                            f"{deployment}:"
                            f"{plan['desired_state']['source_fingerprints'][source]}:"
                            f"{source}"
                        )
                        argv = [
                            *running_okg_argv(), "jobs", "enqueue-source",
                            "--dsn", dsn,
                            "--runtime-deployment", deployment,
                            "--source", source,
                            "--run-key", run_key,
                            "--apply", "--json",
                        ]
                        result = runner(argv, {})
                        if result.returncode != 0:
                            raise _source_enqueue_failure(source, result)
                        rows.append({
                            "source": source,
                            "result": _validate_source_enqueue(
                                dsn=dsn,
                                deployment=deployment,
                                source=source,
                                run_key=run_key,
                                payload=result.json_payload(),
                            ),
                        })
                    receipt = {"sources": rows, "deduplicated": True}
                elif stage == "worker_reload":
                    service_authority = plan["desired_state"][
                        "service_authority"
                    ]
                    if service_authority.get("adapter") == PROCESS_ADAPTER:
                        # Nothing supervises this worker, so nothing here can
                        # reload it. Finish the other stages; the operator
                        # restarts the worker (see after the loop).
                        process_reload = action
                        continue
                    # An acknowledgment predating this replacement is not
                    # replacement proof, even when its fingerprints happen to
                    # match. Clear it before any fallible service work so a
                    # failed install remains resumable and visibly blocked.
                    clear_worker_acknowledgment(
                        dsn=dsn,
                        deployment=deployment,
                    )
                    install_argv = [
                        *running_okg_argv(), "dogfood", "service", "install",
                        "--deployment", deployment,
                        "--adapter", service_authority["adapter"],
                        "--repo-root", service_authority["working_directory"],
                        "--dsn", "${OKG_DEPLOYMENT_APPLY_DSN}",
                        "--apply", "--json",
                    ]
                    install_result = runner(
                        install_argv,
                        {"OKG_DEPLOYMENT_APPLY_DSN": dsn},
                    )
                    install_payload = install_result.json_payload()
                    if (
                        install_result.returncode != 0
                        or not isinstance(install_payload, Mapping)
                        or install_payload.get("ok") is not True
                        or install_payload.get("deployment") != deployment
                        or install_payload.get("adapter")
                        != service_authority["adapter"]
                        or not install_payload.get("written")
                    ):
                        from okg.substrate.db.dsn import redact_text

                        raise SubstrateError(
                            "supervisor definition install failed",
                            data={
                                "adapter": service_authority["adapter"],
                                # Line by line: stderr is many lines, and
                                # a one-DSN masker would drop the lines
                                # after a password or a later `@`.
                                "stderr": redact_text(
                                    install_result.stderr[-2000:],
                                    dsn_flags=(),
                                ),
                            },
                            fix_hint=hint_command(okg_command(
                                "deployment apply", deployment, json=True,
                            )),
                        )
                    argv = [
                        *running_okg_argv(), "dogfood", "restart",
                        "--deployment", deployment,
                        "--adapter", service_authority["adapter"],
                        "--apply", "--json",
                    ]
                    result = runner(argv, {})
                    if result.returncode != 0:
                        raise SubstrateError(
                            "portable worker reload failed",
                            data={"stderr": result.stderr[-2000:]},
                        )
                    ack = _wait_for_ack(
                        dsn=dsn, plan=plan,
                        timeout_seconds=worker_ack_timeout_seconds,
                    )
                    receipt = {
                        "service_install": {
                            "schema": install_payload.get("schema"),
                            "deployment": install_payload.get("deployment"),
                            "adapter": install_payload.get("adapter"),
                            "destination": install_payload.get("destination"),
                            "written": install_payload.get("written"),
                            "content_sha256": install_payload.get(
                                "content_sha256"
                            ),
                        },
                        "control": result.json_payload(),
                        "ack": ack,
                    }
                else:  # pragma: no cover - STAGES is closed above.
                    raise AssertionError(stage)
                _recheck_digest(plan, dsn)
                receipts[stage] = {
                    "desired_state_digest": plan["desired_state"]["desired_state_digest"],
                    "postcondition": receipt,
                }
                if stage not in completed:
                    completed.append(stage)
                _record_run(
                    conn, plan=plan, status="applying", current_stage=None,
                    completed_stages=completed, receipts=receipts,
                )
                conn.commit()
            final = plan_deployment_apply(
                deployment=deployment, deployment_dir=deployment_dir, dsn=dsn,
                supervisor_inventory=supervisor_inventory,
            )
            if process_reload is not None and {
                row["stage"] for row in final["actions"] if row["needed"]
            } <= {"worker_reload", "source_supervision"}:
                # Everything apply can do is done. Record it complete so no
                # failed receipt sends doctor back here, then name the restart.
                _record_run(
                    conn, plan=plan, status="complete", current_stage=None,
                    completed_stages=completed, receipts=receipts,
                )
                conn.commit()
                run_started = False
                raise SubstrateError(
                    "deployment apply cannot restart a worker it does not "
                    "supervise",
                    data={
                        "adapter": PROCESS_ADAPTER,
                        "completed_stages": completed,
                        "reason": process_reload.get("reason"),
                        "mismatches": process_reload.get("mismatches") or [],
                    },
                    fix_hint=_process_worker_fix(deployment, process_reload),
                )
            if not final["no_op"]:
                raise SubstrateError(
                    "deployment apply finished stages but live state is not converged",
                    data={"actions": final["actions"]},
                )
            _record_run(
                conn, plan=plan, status="complete", current_stage=None,
                completed_stages=completed, receipts=receipts,
            )
            conn.commit()
            return {
                **final,
                "summary": (
                    f"applied {len(completed)} stage(s) "
                    f"({', '.join(completed)}); the deployment is converged"
                ),
                "mode": "apply",
                "applied": True,
                "completed_stages": completed,
                "stage_receipts": receipts,
            }
        except Exception as exc:
            if run_started:
                _record_run(
                    conn, plan=plan, status="failed", current_stage=(
                        stage if "stage" in locals() else None
                    ), completed_stages=completed, receipts=receipts,
                    error={"type": type(exc).__name__, "message": str(exc)},
                )
                conn.commit()
            raise
        finally:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_unlock(hashtext(%s), hashtext(%s))",
                    ("okg.deployment.apply", deployment),
                )
            conn.commit()


def deployment_apply_health(
    *, deployment: str, deployment_dir: Path | str, dsn: str,
) -> dict[str, Any]:
    """Return a readiness-compatible finding for apply drift/failure."""
    try:
        with db_connect(dsn, autocommit=True) as conn:
            receipt = _read_receipt(conn, deployment)
            ack = _read_worker_ack(conn, deployment)
    except Exception as exc:  # noqa: BLE001 - the main DB probe owns availability.
        return {
            "ok": False,
            "status": "unavailable",
            "managed": True,
            "reason": str(exc),
            "next_command": hint_command(
            okg_command("deployment apply", deployment, json=True)
        ),
        }
    try:
        plan = plan_deployment_apply(
            deployment=deployment, deployment_dir=deployment_dir, dsn=dsn,
        )
    except Exception as exc:  # noqa: BLE001 - an existing apply must fail closed.
        return {
            "ok": False,
            "status": "unavailable",
            "managed": True,
            "reason": str(exc),
            "receipt": receipt,
            "next_command": hint_command(
            okg_command("deployment apply", deployment, json=True)
        ),
        }
    problems: list[str] = []
    warnings: list[str] = []
    if receipt is not None and receipt.get("status") != "complete":
        problems.append(f"apply_{receipt.get('status')}")
    # The worker-ack stages are never needed with the runtime off (issue
    # #3628): no worker runs there, so nothing waits for one to acknowledge.
    # With the runtime on, a needed stage -- an absent ack included -- is a
    # problem. `warnings` stays in the payload for its readers; it is empty.
    for row in plan["actions"]:
        if row["needed"]:
            problems.append(f"stage_needed:{row['stage']}")
    status = "blocked" if problems else "ready"
    next_command = hint_command(
        okg_command("deployment apply", deployment, json=True)
    )
    message = None
    worker_problems = {
        "stage_needed:worker_reload", "stage_needed:source_supervision",
    }
    if (
        problems
        and set(problems) <= worker_problems
        and plan["desired_state"]["service_authority"].get("adapter")
        == PROCESS_ADAPTER
    ):
        # `deployment apply` cannot fix this: nothing supervises the worker.
        reload = next(
            row for row in plan["actions"] if row["stage"] == "worker_reload"
        )
        next_command = _process_worker_fix(deployment, reload)
        if any(
            "missing from desired authority" in item
            for item in reload.get("mismatches") or []
        ):
            message = (
                "the worker cannot be checked: the okg running this command "
                "does not know its own code revision"
            )
        elif ack is None:
            message = "no runtime worker has started for this deployment yet"
        else:
            message = (
                "the worker last started under older deployment settings or "
                "okg code; restart it"
            )
    return {
        "ok": not problems,
        "status": status,
        "warnings": warnings,
        "managed": True,
        "deployment": deployment,
        "desired_state_digest": plan["desired_state"]["desired_state_digest"],
        "plan_id": plan["plan_id"],
        "actions": plan["actions"],
        "receipt": receipt,
        "worker_ack": ack,
        "problems": problems,
        "failed_stage": receipt.get("current_stage") if receipt else None,
        "message": message,
        "next_command": next_command,
    }


def _process_worker_fix(deployment: str, reload: Mapping[str, Any]) -> str:
    """What fixes a needed `process` worker reload, from its mismatches."""
    unknown = [
        item for item in reload.get("mismatches") or []
        if "missing from desired authority" in item
    ]
    if unknown:
        # A restart cannot fix this: the expected identity itself is unknown.
        return (
            "the okg running this command cannot name its own code revision ("
            + "; ".join(unknown)
            + "); run okg from a Git checkout, or from a wheel built from a "
            "clean commit, then restart the worker: "
            + hint_command(okg_command("runtime worker", deployment=deployment))
        )
    return _process_worker_restart_hint(
        deployment, started=reload.get("reason") != "worker_ack_absent",
    )


def _process_worker_restart_hint(deployment: str, *, started: bool = True) -> str:
    """The fix for a `process` worker: the operator (re)starts it."""
    command = hint_command(okg_command("runtime worker", deployment=deployment))
    if not started:
        return f"start the worker: {command}"
    return (
        "stop the running `okg runtime worker` for this deployment and "
        f"start it again: {command}"
    )


__all__ = [
    "ApplyCommandResult",
    "apply_deployment_plan",
    "clear_worker_acknowledgment",
    "deployment_apply_health",
    "deployment_uses_process_worker",
    "desired_deployment_state",
    "ensure_deployment_apply_schema",
    "plan_deployment_apply",
    "record_worker_acknowledgment",
]
