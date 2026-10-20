"""Metadata-only extension planning and out-of-process capability brokerage.

The planner never imports extension code.  Executable code receives only the
public ``okg.deployment`` data API in a sanitized child process and returns a
declarative batch for substrate-owned publication.
"""

from __future__ import annotations

import ast
import hashlib
import json
import os
import re
import subprocess
import sys
import sysconfig
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from okg.deployment import EXTENSION_SDK_VERSION, ExtensionBatch, ExtensionEdge, ExtensionNode
from okg.substrate.cli.child_argv import inherited_bytecode_flags


EXTENSION_PACKAGE_SCHEMA = "okg.extension_package/v1"
_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_EXTENSION_KINDS = frozenset(
    {"connector", "extractor", "enricher", "verifier", "materializer", "tool", "resolver"}
)
_FORBIDDEN_IMPORT_PREFIXES = (
    "okg.substrate",
    "okg.deployment._internal",
    "psycopg",
    "psycopg2",
    "sqlalchemy",
    "subprocess",
    "importlib",
    "builtins",
    "sys",
)
_AMBIENT_NAMES = frozenset(
    {"OKG_DSN", "DATABASE_URL", "PGPASSWORD", "AWS_SECRET_ACCESS_KEY", "API_KEY"}
)


class ExtensionBoundaryError(ValueError):
    """Stable extension refusal, normally raised before executable launch."""

    def __init__(self, code: str, message: str, *, path: str = "$") -> None:
        super().__init__(message)
        self.code = code
        self.path = path


@dataclass(frozen=True, slots=True)
class ExtensionPlanEntry:
    extension_id: str
    kind: str
    target: str
    capabilities: tuple[str, ...]
    requirements: Mapping[str, Any]
    requirements_digest: str
    source_path: str


@dataclass(frozen=True, slots=True)
class ExtensionPackagePlan:
    package_name: str
    package_version: str
    package_digest: str
    python_abi: str
    sdk_version: str
    dependency_digest: str
    entrypoint_digest: str
    provenance_digest: str
    sbom_digest: str
    execution_tier: Literal["out-of-process"]
    entries: tuple[ExtensionPlanEntry, ...]
    plan_digest: str
    package_root: Path


@dataclass(frozen=True, slots=True)
class ExtensionBrokerResult:
    batch: ExtensionBatch
    publication: Mapping[str, Any] | None
    stderr: str


def plan_extension_package(package_root: str | Path) -> ExtensionPackagePlan:
    """Inspect descriptor, pins, entry points, and source without importing code."""

    root = Path(package_root).resolve()
    descriptor_path = root / "okg-extension.json"
    if not root.is_dir() or not descriptor_path.is_file():
        raise ExtensionBoundaryError(
            "extension_metadata_missing",
            "extension package must contain okg-extension.json",
            path="okg-extension.json",
        )
    raw = _load_json_object(descriptor_path)
    _exact_keys(
        raw,
        {
            "schema_version",
            "package",
            "environment",
            "provenance",
            "sbom",
            "execution_tier",
            "entries",
        },
        path="$",
    )
    if raw["schema_version"] != EXTENSION_PACKAGE_SCHEMA:
        raise ExtensionBoundaryError(
            "extension_schema_unsupported",
            f"schema_version must be {EXTENSION_PACKAGE_SCHEMA}",
            path="schema_version",
        )
    if raw["execution_tier"] != "out-of-process":
        raise ExtensionBoundaryError(
            "extension_in_process_unapproved",
            "extension packages require the out-of-process execution tier",
            path="execution_tier",
        )

    package = _object(raw["package"], path="package")
    _exact_keys(package, {"name", "version"}, path="package")
    environment = _object(raw["environment"], path="environment")
    _exact_keys(
        environment,
        {"python_abi", "sdk_version", "dependency_lock", "dependency_digest"},
        path="environment",
    )
    provenance = _object(raw["provenance"], path="provenance")
    _exact_keys(provenance, {"source", "revision", "builder"}, path="provenance")
    sbom = _object(raw["sbom"], path="sbom")
    _exact_keys(sbom, {"path", "digest"}, path="sbom")

    dependency_path = _safe_package_file(root, environment["dependency_lock"])
    python_abi = _identifier(environment["python_abi"], "environment.python_abi")
    if python_abi != sys.implementation.cache_tag:
        raise ExtensionBoundaryError(
            "extension_python_abi_mismatch",
            "extension ABI pin does not match the broker interpreter",
            path="environment.python_abi",
        )
    sdk_version = _identifier(environment["sdk_version"], "environment.sdk_version")
    if sdk_version != EXTENSION_SDK_VERSION:
        raise ExtensionBoundaryError(
            "extension_sdk_version_mismatch",
            "extension SDK pin does not match the broker SDK",
            path="environment.sdk_version",
        )
    dependency_digest = _checked_digest(environment["dependency_digest"], "environment.dependency_digest")
    if _file_digest(dependency_path) != dependency_digest:
        raise ExtensionBoundaryError(
            "extension_dependency_digest_mismatch",
            "dependency lock content does not match its pin",
            path="environment.dependency_digest",
        )
    sbom_path = _safe_package_file(root, sbom["path"])
    sbom_digest = _checked_digest(sbom["digest"], "sbom.digest")
    if _file_digest(sbom_path) != sbom_digest:
        raise ExtensionBoundaryError(
            "extension_sbom_digest_mismatch",
            "SBOM content does not match its pin",
            path="sbom.digest",
        )

    raw_entries = raw["entries"]
    if not isinstance(raw_entries, list) or not raw_entries:
        raise ExtensionBoundaryError(
            "extension_entrypoints_missing",
            "extension package must declare at least one entry",
            path="entries",
        )
    entries: list[ExtensionPlanEntry] = []
    seen: set[str] = set()
    for index, value in enumerate(raw_entries):
        path = f"entries[{index}]"
        entry = _object(value, path=path)
        _exact_keys(
            entry,
            {"id", "kind", "target", "capabilities", "requirements"},
            path=path,
        )
        extension_id = _identifier(entry["id"], f"{path}.id")
        if extension_id in seen:
            raise ExtensionBoundaryError(
                "extension_id_duplicate",
                f"duplicate extension id {extension_id!r}",
                path=f"{path}.id",
            )
        seen.add(extension_id)
        kind = _identifier(entry["kind"], f"{path}.kind")
        if kind not in _EXTENSION_KINDS:
            raise ExtensionBoundaryError(
                "extension_kind_unknown",
                f"unknown extension kind {kind!r}",
                path=f"{path}.kind",
            )
        target = _identifier(entry["target"], f"{path}.target")
        module_name, separator, attribute = target.partition(":")
        if not separator or not module_name or not attribute or "." in attribute:
            raise ExtensionBoundaryError(
                "extension_entrypoint_invalid",
                "entry target must be module:function metadata",
                path=f"{path}.target",
            )
        source_path = _safe_package_file(root, module_name.replace(".", "/") + ".py")
        capabilities = _string_tuple(entry["capabilities"], f"{path}.capabilities")
        requirements = _object(entry["requirements"], path=f"{path}.requirements")
        _inspect_extension_source(
            source_path,
            extension_id=extension_id,
            kind=kind,
            attribute=attribute,
            capabilities=capabilities,
        )
        entries.append(
            ExtensionPlanEntry(
                extension_id=extension_id,
                kind=kind,
                target=target,
                capabilities=capabilities,
                requirements=requirements,
                requirements_digest=_digest(requirements),
                source_path=source_path.relative_to(root).as_posix(),
            )
        )

    entrypoint_payload = [
        {
            "id": item.extension_id,
            "kind": item.kind,
            "target": item.target,
            "capabilities": list(item.capabilities),
            "requirements": item.requirements,
            "requirements_digest": item.requirements_digest,
            "source_path": item.source_path,
        }
        for item in sorted(entries, key=lambda item: item.extension_id)
    ]
    pins = {
        "package_name": _identifier(package["name"], "package.name"),
        "package_version": _identifier(package["version"], "package.version"),
        "package_digest": _directory_digest(root),
        "python_abi": python_abi,
        "sdk_version": sdk_version,
        "dependency_digest": dependency_digest,
        "entrypoint_digest": _digest(entrypoint_payload),
        "provenance_digest": _digest(provenance),
        "sbom_digest": sbom_digest,
        "execution_tier": "out-of-process",
        "entries": entrypoint_payload,
    }
    return ExtensionPackagePlan(
        package_name=pins["package_name"],
        package_version=pins["package_version"],
        package_digest=pins["package_digest"],
        python_abi=pins["python_abi"],
        sdk_version=pins["sdk_version"],
        dependency_digest=dependency_digest,
        entrypoint_digest=pins["entrypoint_digest"],
        provenance_digest=pins["provenance_digest"],
        sbom_digest=sbom_digest,
        execution_tier="out-of-process",
        entries=tuple(sorted(entries, key=lambda item: item.extension_id)),
        plan_digest=_digest(pins),
        package_root=root,
    )


def extension_package_plan_digest(plan: ExtensionPackagePlan) -> str:
    """Recompute the immutable package plan identity from its admitted fields."""

    entries = [
        {
            "id": item.extension_id,
            "kind": item.kind,
            "target": item.target,
            "capabilities": list(item.capabilities),
            "requirements": item.requirements,
            "requirements_digest": item.requirements_digest,
            "source_path": item.source_path,
        }
        for item in sorted(plan.entries, key=lambda item: item.extension_id)
    ]
    return _digest(
        {
            "package_name": plan.package_name,
            "package_version": plan.package_version,
            "package_digest": plan.package_digest,
            "python_abi": plan.python_abi,
            "sdk_version": plan.sdk_version,
            "dependency_digest": plan.dependency_digest,
            "entrypoint_digest": plan.entrypoint_digest,
            "provenance_digest": plan.provenance_digest,
            "sbom_digest": plan.sbom_digest,
            "execution_tier": plan.execution_tier,
            "entries": entries,
        }
    )


def _execute_extension_unchecked(
    plan: ExtensionPackagePlan,
    *,
    extension_id: str,
    binding_id: str,
    capabilities: Sequence[str],
    payload: Mapping[str, Any],
    publish: Callable[[ExtensionBatch], Mapping[str, Any]] | None = None,
    timeout_seconds: float = 60.0,
) -> ExtensionBrokerResult:
    """Run an admitted entry out of process, then optionally publish its batch."""

    verified = verify_extension_package_plan(plan)
    entry = next((item for item in verified.entries if item.extension_id == extension_id), None)
    if entry is None:
        raise ExtensionBoundaryError(
            "extension_id_unregistered",
            f"extension id {extension_id!r} is not registered by the package",
        )
    granted = _string_tuple(list(capabilities), "capabilities")
    undeclared = sorted(set(granted) - set(entry.capabilities))
    if undeclared:
        raise ExtensionBoundaryError(
            "extension_capability_undeclared",
            "requested capabilities are not declared: " + ", ".join(undeclared),
        )
    _reject_sensitive_payload(payload)
    module_name, _, attribute = entry.target.partition(":")
    source_path = _safe_package_file(verified.package_root, entry.source_path)
    source = source_path.read_text(encoding="utf-8")
    request = {
        "source": source,
        "filename": entry.source_path,
        "module_name": module_name,
        "attribute": attribute,
        "extension_id": entry.extension_id,
        "kind": entry.kind,
        "declared_capabilities": list(entry.capabilities),
        "binding_id": _identifier(binding_id, "binding_id"),
        "granted_capabilities": list(granted),
        "payload": dict(payload),
    }
    environment = _broker_environment()
    try:
        with tempfile.TemporaryDirectory(prefix="okg-extension-") as working_directory:
            with tempfile.TemporaryFile() as stdout_file, tempfile.TemporaryFile() as stderr_file:
                completed = subprocess.run(
                    [
                        sys.executable,
                        *inherited_bytecode_flags(),
                        "-m",
                        "okg.substrate.extension_broker_runner",
                    ],
                    input=json.dumps(request, sort_keys=True),
                    text=True,
                    stdout=stdout_file,
                    stderr=stderr_file,
                    cwd=working_directory,
                    env=environment,
                    timeout=timeout_seconds,
                    check=False,
                )
                stdout = _read_bounded_output(stdout_file)
                stderr = _read_bounded_output(stderr_file)
    except subprocess.TimeoutExpired as exc:
        raise ExtensionBoundaryError(
            "extension_execution_timeout",
            "extension process exceeded its execution timeout",
        ) from exc
    response = _parse_runner_response(stdout)
    if completed.returncode != 0 or not response.get("ok"):
        raise ExtensionBoundaryError(
            str(response.get("code") or "extension_execution_failed"),
            str(response.get("message") or "extension process failed"),
        )
    result = _object(response.get("batch"), path="batch")
    _reject_sensitive_payload(result)
    batch = ExtensionBatch(
        nodes=tuple(
            ExtensionNode(
                node_id=_identifier(item["node_id"], "batch.nodes.node_id"),
                subtype=_identifier(item["subtype"], "batch.nodes.subtype"),
                attrs=_object(item.get("attrs", {}), path="batch.nodes.attrs"),
            )
            for item in _object_list(result.get("nodes", []), path="batch.nodes")
        ),
        edges=tuple(
            ExtensionEdge(
                src=_identifier(item["src"], "batch.edges.src"),
                dst=_identifier(item["dst"], "batch.edges.dst"),
                edge_type=_identifier(item["edge_type"], "batch.edges.edge_type"),
                attrs=_object(item.get("attrs", {}), path="batch.edges.attrs"),
            )
            for item in _object_list(result.get("edges", []), path="batch.edges")
        ),
        observations=_object(result.get("observations", {}), path="batch.observations"),
    )
    publication = publish(batch) if publish is not None else None
    return ExtensionBrokerResult(
        batch=batch,
        publication=publication,
        stderr=_redact(stderr),
    )


def _broker_environment() -> dict[str, str]:
    """Build a secret-free environment that can start the pinned interpreter."""

    environment = {
        key: value
        for key, value in os.environ.items()
        if key in {"PATH", "LANG", "LC_ALL", "TMPDIR", "PYTHONUTF8"}
    }
    if sys.platform.startswith("linux"):
        interpreter_lib = _linux_interpreter_library()
        if interpreter_lib is not None:
            # setup-python's relocatable interpreter needs its own libpython
            # directory at startup. Derive it from the admitted interpreter;
            # never inherit an ambient loader path into extension execution.
            environment["LD_LIBRARY_PATH"] = str(interpreter_lib)
    return environment


def _linux_interpreter_library() -> Path | None:
    interpreter_root = Path(sys.executable).resolve().parent.parent
    configured = sysconfig.get_config_var("LIBDIR")
    candidates: list[Path] = []
    if isinstance(configured, str):
        candidates.append(Path(configured).resolve())
    candidates.extend((interpreter_root / "lib", interpreter_root / "lib64"))
    for candidate in dict.fromkeys(candidates):
        if candidate.is_dir() and any(candidate.glob("libpython*.so*")):
            return candidate
    return None


def verify_extension_package_plan(plan: ExtensionPackagePlan) -> ExtensionPackagePlan:
    """Re-plan from package bytes and reject every caller-constructed identity."""

    if extension_package_plan_digest(plan) != plan.plan_digest:
        raise ExtensionBoundaryError(
            "extension_package_plan_digest_mismatch",
            "extension package plan changed after metadata admission",
        )
    verified = plan_extension_package(plan.package_root)
    if verified != plan:
        raise ExtensionBoundaryError(
            "extension_package_plan_unverified",
            "extension package plan is not the canonical inspected plan for its package",
        )
    return verified


def _read_bounded_output(stream: Any, *, limit: int = 1_000_000) -> str:
    stream.seek(0)
    payload = stream.read(limit + 1)
    if len(payload) > limit:
        raise ExtensionBoundaryError(
            "extension_output_limit_exceeded",
            "extension process exceeded the broker output limit",
        )
    return payload.decode("utf-8", errors="replace")


def _inspect_extension_source(
    path: Path,
    *,
    extension_id: str,
    kind: str,
    attribute: str,
    capabilities: tuple[str, ...],
) -> None:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except (OSError, SyntaxError) as exc:
        raise ExtensionBoundaryError(
            "extension_source_invalid",
            f"extension source cannot be inspected: {exc}",
            path=path.name,
        ) from exc
    _validate_static_entrypoint_metadata(
        tree,
        path=path,
        extension_id=extension_id,
        kind=kind,
        attribute=attribute,
        capabilities=capabilities,
    )
    for node in ast.walk(tree):
        imported: tuple[str, ...] = ()
        if isinstance(node, ast.Import):
            imported = tuple(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported = (node.module or "",)
        for name in imported:
            if name.startswith(_FORBIDDEN_IMPORT_PREFIXES):
                raise ExtensionBoundaryError(
                    "extension_private_import_refused",
                    f"private or database import {name!r} is not allowed",
                    path=f"{path.name}:{node.lineno}",
                )
            if name == "os":
                raise ExtensionBoundaryError(
                    "extension_ambient_credentials_refused",
                    "ambient process and environment access is not allowed",
                    path=f"{path.name}:{node.lineno}",
                )
            allowed_imports = {
                "okg.deployment",
                "json",
                "math",
                "re",
                "typing",
            }
            if name == "socket" and "network" in capabilities:
                allowed_imports.add("socket")
            if name == "pathlib" and "filesystem" in capabilities:
                allowed_imports.add("pathlib")
            if name not in allowed_imports:
                raise ExtensionBoundaryError(
                    "extension_import_not_admitted",
                    f"import {name!r} is not admitted for this entrypoint",
                    path=f"{path.name}:{node.lineno}",
                )
            if name == "socket" and "network" not in capabilities:
                raise ExtensionBoundaryError(
                    "extension_capability_undeclared",
                    "direct network APIs require the declared network capability",
                    path=f"{path.name}:{node.lineno}",
                )
            if name == "pathlib" and "filesystem" not in capabilities:
                raise ExtensionBoundaryError(
                    "extension_capability_undeclared",
                    "direct filesystem APIs require the declared filesystem capability",
                    path=f"{path.name}:{node.lineno}",
                )
        if isinstance(node, ast.Call) and _call_name(node.func) in {
            "__import__",
            "importlib.import_module",
        }:
            raise ExtensionBoundaryError(
                "extension_dynamic_import_refused",
                "dynamic imports are not allowed at the extension boundary",
                path=f"{path.name}:{node.lineno}",
            )
        if isinstance(node, ast.Attribute) and _call_name(node) in {
            "os.environ",
            "os.getenv",
        }:
            raise ExtensionBoundaryError(
                "extension_ambient_credentials_refused",
                "ambient environment access is not allowed",
                path=f"{path.name}:{node.lineno}",
            )
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            upper = node.value.upper()
            if upper in _AMBIENT_NAMES or "SECRET_VALUE" in upper or "SECRET_VERSION" in upper:
                raise ExtensionBoundaryError(
                    "extension_secret_access_refused",
                    "ambient DSN, credential, or literal secret access is not allowed",
                    path=f"{path.name}:{node.lineno}",
                )
        call_name = _call_name(node.func) if isinstance(node, ast.Call) else ""
        if call_name in {"eval", "exec", "compile", "globals", "locals", "vars"}:
            raise ExtensionBoundaryError(
                "extension_dynamic_execution_refused",
                f"dynamic execution helper {call_name!r} is not allowed",
                path=f"{path.name}:{node.lineno}",
            )
        if call_name == "open" and "filesystem" not in capabilities:
            raise ExtensionBoundaryError(
                "extension_capability_undeclared",
                "direct filesystem APIs require the declared filesystem capability",
                path=f"{path.name}:{node.lineno}",
            )
    if "network" in capabilities or "filesystem" in capabilities:
        # These capabilities remain broker-contained; declaring them does not
        # grant direct host access to extension code.
        return


def _validate_static_entrypoint_metadata(
    tree: ast.AST,
    *,
    path: Path,
    extension_id: str,
    kind: str,
    attribute: str,
    capabilities: tuple[str, ...],
) -> None:
    function = next(
        (
            node
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == attribute
        ),
        None,
    )
    if function is None:
        raise ExtensionBoundaryError(
            "extension_entrypoint_missing",
            f"entrypoint function {attribute!r} is absent",
            path=path.name,
        )
    decorator = next(
        (
            item
            for item in function.decorator_list
            if isinstance(item, ast.Call) and _call_name(item.func) == "extension_entrypoint"
        ),
        None,
    )
    if decorator is None or len(decorator.args) != 1:
        raise ExtensionBoundaryError(
            "extension_entrypoint_metadata_mismatch",
            "entrypoint must use the public extension_entrypoint decorator",
            path=f"{path.name}:{function.lineno}",
        )
    declared_id = _literal_string(decorator.args[0])
    keywords = {item.arg: item.value for item in decorator.keywords if item.arg is not None}
    declared_kind = _literal_string(keywords.get("kind"))
    declared_capabilities = _literal_string_tuple(keywords.get("capabilities"))
    if (
        declared_id != extension_id
        or declared_kind != kind
        or declared_capabilities != capabilities
    ):
        raise ExtensionBoundaryError(
            "extension_entrypoint_metadata_mismatch",
            "public SDK decorator metadata does not match the package descriptor",
            path=f"{path.name}:{function.lineno}",
        )


def _literal_string(value: ast.AST | None) -> str | None:
    if isinstance(value, ast.Constant) and isinstance(value.value, str):
        return value.value
    return None


def _literal_string_tuple(value: ast.AST | None) -> tuple[str, ...] | None:
    if not isinstance(value, (ast.Tuple, ast.List)):
        return None
    values = tuple(_literal_string(item) for item in value.elts)
    if any(item is None for item in values):
        return None
    return tuple(sorted(item for item in values if item is not None))


def _reject_sensitive_payload(value: Any, *, path: str = "$") -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            normalized = re.sub(r"[^a-z0-9]+", "_", str(key).lower()).strip("_")
            if normalized in {
                "dsn",
                "database_url",
                "credential",
                "credentials",
                "password",
                "secret",
                "secret_value",
                "secret_version",
                "secret_version_id",
                "token",
            } or normalized.endswith(("_password", "_token", "_secret", "_api_key")):
                raise ExtensionBoundaryError(
                    "extension_secret_payload_refused",
                    "literal secrets, DSNs, credentials, and secret versions are not broker payloads",
                    path=f"{path}.{key}",
                )
            _reject_sensitive_payload(child, path=f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            _reject_sensitive_payload(child, path=f"{path}[{index}]")


def _parse_runner_response(stdout: str) -> dict[str, Any]:
    try:
        value = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise ExtensionBoundaryError(
            "extension_protocol_invalid",
            "extension process returned an invalid broker response",
        ) from exc
    return _object(value, path="runner_response")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ExtensionBoundaryError(
            "extension_metadata_invalid",
            f"extension metadata is invalid: {exc}",
            path=path.name,
        ) from exc
    return _object(value, path=path.name)


def _safe_package_file(root: Path, relative: Any) -> Path:
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise ExtensionBoundaryError(
            "extension_path_invalid",
            "extension metadata paths must be relative files",
        )
    candidate = (root / relative).resolve()
    if root not in candidate.parents or not candidate.is_file():
        raise ExtensionBoundaryError(
            "extension_path_invalid",
            "extension metadata path escapes the package or is missing",
            path=relative,
        )
    return candidate


def _directory_digest(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        if "__pycache__" in path.parts or path.suffix == ".pyc":
            continue
        relative = path.relative_to(root).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        content = path.read_bytes()
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return "sha256:" + digest.hexdigest()


def _file_digest(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _digest(value: Any) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _object(value: Any, *, path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ExtensionBoundaryError(
            "extension_metadata_invalid",
            f"{path} must be an object",
            path=path,
        )
    return value


def _object_list(value: Any, *, path: str) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise ExtensionBoundaryError(
            "extension_protocol_invalid",
            f"{path} must be a list of objects",
            path=path,
        )
    return value


def _exact_keys(value: Mapping[str, Any], expected: set[str], *, path: str) -> None:
    observed = set(value)
    if observed != expected:
        raise ExtensionBoundaryError(
            "extension_metadata_fields_invalid",
            f"{path} fields must be exactly {sorted(expected)}",
            path=path,
        )


def _identifier(value: Any, path: str) -> str:
    if not isinstance(value, str) or not value.strip() or any(char.isspace() for char in value):
        raise ExtensionBoundaryError(
            "extension_identifier_invalid",
            f"{path} must be a non-empty string without whitespace",
            path=path,
        )
    return value


def _checked_digest(value: Any, path: str) -> str:
    if not isinstance(value, str) or not _DIGEST_RE.fullmatch(value):
        raise ExtensionBoundaryError(
            "extension_digest_invalid",
            f"{path} must use sha256:<64 lowercase hex>",
            path=path,
        )
    return value


def _string_tuple(value: Any, path: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ExtensionBoundaryError(
            "extension_capabilities_invalid",
            f"{path} must be a list",
            path=path,
        )
    checked = tuple(sorted(_identifier(item, path) for item in value))
    if len(checked) != len(set(checked)):
        raise ExtensionBoundaryError(
            "extension_capabilities_invalid",
            f"{path} must not contain duplicates",
            path=path,
        )
    return checked


def _call_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent = _call_name(node.value)
        return f"{parent}.{node.attr}" if parent else node.attr
    return ""


def _redact(value: str) -> str:
    redacted = value
    for name in _AMBIENT_NAMES:
        material = os.environ.get(name)
        if material:
            redacted = redacted.replace(material, "[REDACTED]")
    return redacted[-4000:]


__all__ = [
    "EXTENSION_PACKAGE_SCHEMA",
    "ExtensionBoundaryError",
    "ExtensionBrokerResult",
    "ExtensionPackagePlan",
    "ExtensionPlanEntry",
    "extension_package_plan_digest",
    "plan_extension_package",
    "verify_extension_package_plan",
]
