"""Production-owned setup adapters for the Viz coordination service.

This module is the small composition boundary between Viz's transport-only
callbacks and the existing PACT, deployment, Git, Chronos, and native
coordination authorities.  It deliberately does not introduce a second
ledger and it never enrolls a target route.  The four client routes in the
tracked policy are transport shapes; concrete target enrollment belongs to
the assignment-reservation handshake.

The setup authority is a local file supplied by production startup.  It is
not an HTTP input and is read through a strict, race-resistant file boundary.
Only opaque references and the local chief's filesystem targets are accepted
by its closed schema.  Probe fragments are intentionally redacted so paths,
hosts, and connection strings do not become part of the plan response.
"""

from __future__ import annotations

import base64
import hashlib
import ipaddress
import importlib
import json
import os
import re
import socket
import stat
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol

from psycopg.conninfo import conninfo_to_dict

from okg.substrate.deployments import (
    DeploymentResolutionError,
    find_deployment_dir,
    inject_deployment_path,
    resolve_deployment,
)
from okg.substrate.db.connection import connect as db_connect
from okg.substrate.library.agent_runtimes.coordination_client import (
    DEFAULT_CLIENT_REGISTRY_PATH,
    ClientRegistry,
    load_client_registry,
)
from okg.substrate.library.agent_runtimes import coordination_ssh
from okg.substrate.library.agent_runtimes.paired_workspace import (
    PairedWorkspaceLifecycle,
    PairedWorkspaceRequest,
    ProductionGitWorkspaceAdapter,
    ProductionGraphWorkspaceAdapter,
    _validate_git_branch,
)
from okg.substrate.library.sources.git_scope_probe import GitScopeProbe
from okg.substrate.pact.project_settings import CoordinationPolicy, coordination_policy
from okg.viz.coordination_services import (
    SetupAuthorityProbes,
    SetupChiefResolver,
    SetupLifecycleFactory,
    SetupPairRequestFactory,
)
from okg.substrate.library.agent_runtimes.ssh_stdio import validate_coordination_route


SETUP_AUTHORITY_SCHEMA = "okg.viz.coordination-setup-authority.v1"
"""Closed JSON schema accepted by :func:`load_setup_authority`."""

MAX_SETUP_AUTHORITY_BYTES = 64 * 1024
"""Maximum bytes read from the startup-supplied authority file."""

LOCAL_HOSTS = frozenset({"local", "localhost", "127.0.0.1", "::1"})

_TOP_LEVEL_FIELDS = frozenset({"schema", "deployment", "pact", "chief"})
_PACT_FIELDS = frozenset({"change_id", "task_id"})
_CHIEF_FIELDS = frozenset(
    {
        "project_id",
        "repo_path",
        "worktree_path",
        "git_branch",
        "graph_base_branch",
        "authenticated_host",
        "ssh_auth_fingerprint",
        "source_address",
        "target_address",
        "target_port",
        "route",
        "grant_id",
    }
)
_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_COMMIT_RE = re.compile(r"^[0-9a-f]{40}$", re.IGNORECASE)
_TOKEN_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@+/-]{0,511}$")
_BRANCH_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_./@+-]{0,255}$")
_SECRET_KEY_RE = re.compile(
    r"(?:^|_)(?:secret|dsn|token|password|private[_-]?key|credential)(?:$|_)",
    re.IGNORECASE,
)
_SECRET_VALUE_RE = re.compile(
    r"(?i)(?:postgres(?:ql)?|mysql|mongodb|redis)://|"
    r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----"
)
_SSH_FINGERPRINT_RE = re.compile(r"^(?:sha256:)?[0-9a-f]{64}$", re.IGNORECASE)


class ProductionSetupUnavailable(RuntimeError):
    """A production setup authority is missing, stale, or unsafe.

    The message is deliberately stable and never includes a path, host, DSN,
    or exception text.  Startup can map every refusal to one unavailable
    response without turning configuration details into an observability
    leak.
    """


# Keep the descriptive name used by the lower-level loader while making the
# public startup error a single, stable exception type.
CoordinationAdapterUnavailable = ProductionSetupUnavailable


class ServerRegistryLoader(Protocol):
    """Optional operator-owned server route registry loader.

    The current deployment's native state is authoritative for assignments;
    this hook lets a deployment expose its existing static server registry
    without making this shared module invent a registry format.
    """

    def __call__(self) -> Any: ...


ReachabilityChecker = Callable[[str, int], bool]
IdentityFingerprintLoader = Callable[[Path], str | None]
SSH_KEYGEN_EXECUTABLE = Path("/usr/bin/ssh-keygen")


def _bounded_tcp_reachable(host: str, port: int) -> bool:
    """Probe one operator-configured route without writing or leaking it."""

    try:
        with socket.create_connection((host, port), timeout=0.25):
            return True
    except (OSError, ValueError):
        return False


def _route_host_port(entry: Any) -> tuple[str, int]:
    host = getattr(entry, "host", None)
    port = getattr(entry, "port", None)
    if isinstance(entry, Mapping):
        if host is None:
            host = entry.get("host")
        if port is None:
            port = entry.get("port")
    if (
        not isinstance(host, str)
        or not host.strip()
        or any(char.isspace() for char in host)
        or isinstance(port, bool)
        or not isinstance(port, int)
        or not 0 < port < 65536
    ):
        raise CoordinationAdapterUnavailable("operator route registry is incomplete")
    return host.strip(), port


@dataclass(frozen=True, slots=True)
class SetupAuthority:
    """Validated startup-owned setup authority.

    The object is internal configuration.  Its path values are never placed
    in a probe fragment or a returned transport receipt.
    """

    schema: str
    deployment: str
    change_id: str
    task_id: str
    project_id: str
    repo_path: Path
    worktree_path: Path
    git_branch: str
    graph_base_branch: str
    authenticated_host: str
    ssh_auth_fingerprint: str
    source_address: str
    target_address: str
    target_port: int
    route: str
    grant_id: str
    config_digest: str

    @property
    def credential_id(self) -> str:
        """Return the native credential key derived from protected config."""

        return f"ssh-auth:{self.ssh_auth_fingerprint}"


@dataclass(frozen=True, slots=True)
class CoordinationAdapterBundle:
    """Callbacks ready for injection into ``build_native_coordination_services``."""

    setup_probes: SetupAuthorityProbes
    chief_resolver: SetupChiefResolver
    pair_request_factory: SetupPairRequestFactory
    lifecycle_factory: SetupLifecycleFactory

    # This spelling is useful to startup code that calls the result an
    # adapter bundle rather than a callback bundle.
    @property
    def probes(self) -> SetupAuthorityProbes:
        return self.setup_probes


# Public spelling used by Viz startup.  The field layout intentionally stays
# identical to the callback bundle consumed by coordination_services.
ProductionSetupAdapters = CoordinationAdapterBundle


def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise CoordinationAdapterUnavailable("setup authority has duplicate keys")
        result[key] = value
    return result


def _canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise CoordinationAdapterUnavailable(
            "setup authority is not JSON-safe"
        ) from exc


def _digest(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _key_name(value: Any) -> str:
    return str(value).strip().lower().replace("-", "_")


def _reject_secret_fields(value: Any, *, path: str = "authority") -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            key_name = _key_name(key)
            if _SECRET_KEY_RE.search(key_name):
                raise CoordinationAdapterUnavailable(
                    f"setup authority contains an inline sensitive field at {path}"
                )
            _reject_secret_fields(item, path=f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _reject_secret_fields(item, path=f"{path}[{index}]")
    elif isinstance(value, str) and _SECRET_VALUE_RE.search(value):
        raise CoordinationAdapterUnavailable(
            "setup authority contains credential material"
        )


def _read_authority_bytes(path: Path) -> bytes:
    """Read one private regular file without following a replacement link."""

    if not path.is_absolute():
        raise CoordinationAdapterUnavailable("setup authority path must be absolute")
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise CoordinationAdapterUnavailable("setup authority is unavailable") from exc
    owner = os.geteuid()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid not in {0, owner}
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise CoordinationAdapterUnavailable(
            "setup authority has unsafe ownership or mode"
        )
    try:
        fd = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
    except OSError as exc:
        raise CoordinationAdapterUnavailable("setup authority is unavailable") from exc
    try:
        opened = os.fstat(fd)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid not in {0, owner}
            or stat.S_IMODE(opened.st_mode) & 0o077
            or (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino)
        ):
            raise CoordinationAdapterUnavailable(
                "setup authority changed while reading"
            )
        chunks: list[bytes] = []
        size = 0
        while size <= MAX_SETUP_AUTHORITY_BYTES:
            chunk = os.read(fd, min(65536, MAX_SETUP_AUTHORITY_BYTES + 1 - size))
            if not chunk:
                break
            chunks.append(chunk)
            size += len(chunk)
        if size > MAX_SETUP_AUTHORITY_BYTES:
            raise CoordinationAdapterUnavailable("setup authority is too large")
        return b"".join(chunks)
    except CoordinationAdapterUnavailable:
        raise
    except OSError as exc:
        raise CoordinationAdapterUnavailable("setup authority is unreadable") from exc
    finally:
        os.close(fd)


def _required_string(
    value: Any, *, field: str, pattern: re.Pattern[str] = _TOKEN_RE
) -> str:
    if not isinstance(value, str) or not pattern.fullmatch(value.strip()):
        raise CoordinationAdapterUnavailable(f"setup authority has invalid {field}")
    return value.strip()


def _required_path(value: Any, *, field: str) -> Path:
    if not isinstance(value, str):
        raise CoordinationAdapterUnavailable(f"setup authority has invalid {field}")
    text = value.strip()
    if not text or len(text) > 4096 or "\x00" in text or "\n" in text or "\r" in text:
        raise CoordinationAdapterUnavailable(f"setup authority has invalid {field}")
    path = Path(text)
    if not path.is_absolute() or any(part in {".", ".."} for part in path.parts):
        raise CoordinationAdapterUnavailable(f"setup authority has invalid {field}")
    return path


def _required_fingerprint(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not _SSH_FINGERPRINT_RE.fullmatch(value.strip()):
        raise CoordinationAdapterUnavailable(f"setup authority has invalid {field}")
    fingerprint = value.strip().lower()
    if not fingerprint.startswith("sha256:"):
        fingerprint = f"sha256:{fingerprint}"
    return fingerprint


def _required_loopback_address(value: Any, *, field: str) -> str:
    if not isinstance(value, str):
        raise CoordinationAdapterUnavailable(f"setup authority has invalid {field}")
    try:
        address = ipaddress.ip_address(value.strip())
    except ValueError as exc:
        raise CoordinationAdapterUnavailable(
            f"setup authority has invalid {field}"
        ) from exc
    if not address.is_loopback:
        raise CoordinationAdapterUnavailable(
            f"setup authority {field} must be loopback"
        )
    return str(address)


def _required_port(value: Any, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 < value < 65536:
        raise CoordinationAdapterUnavailable(f"setup authority has invalid {field}")
    return value


def _canonical_postgres_identity(value: Any) -> tuple[str, int, str]:
    """Return only the stable endpoint/database identity of a PostgreSQL DSN.

    Credentials and connection-tuning options are deliberately excluded.  A
    service name, address list, or explicit ``hostaddr`` is rejected because
    resolving those requires another authority and could make two apparently
    equal strings target different ledgers.
    """

    if not isinstance(value, str) or not value.strip():
        raise CoordinationAdapterUnavailable("operator route DSN is unavailable")
    try:
        parameters = conninfo_to_dict(value.strip())
    except Exception as exc:  # noqa: BLE001 - do not expose DSN parser details
        raise CoordinationAdapterUnavailable("operator route DSN is invalid") from exc
    if not isinstance(parameters, Mapping):
        raise CoordinationAdapterUnavailable("operator route DSN is invalid")
    if any(name in parameters for name in ("service", "servicefile", "hostaddr")):
        raise CoordinationAdapterUnavailable("operator route DSN is ambiguous")

    host = parameters.get("host")
    port = parameters.get("port", "5432")
    database = parameters.get("dbname")
    if (
        not isinstance(host, str)
        or not host.strip()
        or any(char.isspace() or char == "\x00" for char in host)
        or "," in host
        or not isinstance(port, (str, int))
        or isinstance(port, bool)
        or not isinstance(database, str)
        or not database.strip()
        or "\x00" in database
    ):
        raise CoordinationAdapterUnavailable("operator route DSN is ambiguous")
    port_text = str(port).strip()
    if not port_text.isdecimal() or not 0 < int(port_text) < 65536:
        raise CoordinationAdapterUnavailable("operator route DSN is ambiguous")
    normalized_host = host.strip()
    if normalized_host.startswith("[") and normalized_host.endswith("]"):
        normalized_host = normalized_host[1:-1]
    if not normalized_host:
        raise CoordinationAdapterUnavailable("operator route DSN is ambiguous")
    try:
        normalized_host = ipaddress.ip_address(normalized_host).compressed.lower()
    except ValueError:
        if normalized_host.startswith("/"):
            # Unix socket paths are stable identities but remain digest-only.
            if any(part in {"", ".", ".."} for part in Path(normalized_host).parts):
                raise CoordinationAdapterUnavailable("operator route DSN is ambiguous")
        else:
            normalized_host = normalized_host.rstrip(".").lower()
    if not normalized_host:
        raise CoordinationAdapterUnavailable("operator route DSN is ambiguous")
    return normalized_host, int(port_text), database.strip()


def _normalize_identity_fingerprint(value: Any, *, field: str) -> str:
    """Normalize a registry fingerprint to the native credential spelling."""

    if isinstance(value, str) and value.strip().lower().startswith("ssh-auth:"):
        value = value.strip()[len("ssh-auth:") :]
    return _required_fingerprint(value, field=field)


def _derive_identity_fingerprint(identity_file: Any) -> str:
    """Derive the stable public-key digest from one protected private key.

    The client registry already validates these references before returning a
    production registry.  Repeat the ownership/mode check here because this
    function may be used by a hermetic registry seam, and because the key is
    read after bundle construction as part of the plan authority.
    """

    if not isinstance(identity_file, (str, Path)):
        raise CoordinationAdapterUnavailable(
            "operator client identity fingerprint is unavailable"
        )
    path = Path(identity_file)
    if not path.is_absolute() or any(part in {".", ".."} for part in path.parts):
        raise CoordinationAdapterUnavailable(
            "operator client identity fingerprint is unavailable"
        )
    owner = os.geteuid()
    try:
        metadata = path.lstat()
        mode = stat.S_IMODE(metadata.st_mode)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_uid not in {0, owner}
            or mode & 0o022
            or not mode & 0o400
            or mode & 0o004
        ):
            raise CoordinationAdapterUnavailable(
                "operator client identity fingerprint is unavailable"
            )
        keygen_metadata = SSH_KEYGEN_EXECUTABLE.lstat()
        if (
            not stat.S_ISREG(keygen_metadata.st_mode)
            or stat.S_ISLNK(keygen_metadata.st_mode)
            or keygen_metadata.st_uid != 0
            or keygen_metadata.st_mode & 0o022
            or not keygen_metadata.st_mode & 0o111
        ):
            raise CoordinationAdapterUnavailable(
                "operator client identity fingerprint is unavailable"
            )
        result = subprocess.run(
            [str(SSH_KEYGEN_EXECUTABLE), "-y", "-f", str(path)],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            check=False,
            text=True,
            timeout=2.0,
            env={"PATH": "/usr/bin:/bin", "LC_ALL": "C", "LANG": "C"},
        )
        current = path.lstat()
        if (
            (current.st_dev, current.st_ino) != (metadata.st_dev, metadata.st_ino)
            or stat.S_IMODE(current.st_mode) != mode
            or current.st_uid not in {0, owner}
        ):
            raise CoordinationAdapterUnavailable(
                "operator client identity fingerprint changed"
            )
    except CoordinationAdapterUnavailable:
        raise
    except (OSError, subprocess.SubprocessError, ValueError) as exc:
        raise CoordinationAdapterUnavailable(
            "operator client identity fingerprint is unavailable"
        ) from exc
    if result.returncode != 0:
        raise CoordinationAdapterUnavailable(
            "operator client identity fingerprint is unavailable"
        )
    fields = result.stdout.strip().split()
    if len(fields) < 2 or not fields[0] or not fields[1]:
        raise CoordinationAdapterUnavailable(
            "operator client identity fingerprint is unavailable"
        )
    try:
        base64.b64decode(fields[1], validate=True)
    except (ValueError, base64.binascii.Error) as exc:
        raise CoordinationAdapterUnavailable(
            "operator client identity fingerprint is unavailable"
        ) from exc
    if len(fields[0]) > 256:
        raise CoordinationAdapterUnavailable(
            "operator client identity fingerprint is unavailable"
        )
    # ``read_sshd_peer_evidence`` hashes this exact ExposeAuthInfo line.  Do
    # not use ssh-keygen's display fingerprint or only the decoded key blob:
    # either would bind a configured key to a different native credential id.
    try:
        evidence_line = f"publickey {fields[0]} {fields[1]}".encode("ascii")
    except UnicodeEncodeError as exc:
        raise CoordinationAdapterUnavailable(
            "operator client identity fingerprint is unavailable"
        ) from exc
    return "sha256:" + hashlib.sha256(evidence_line).hexdigest()


def _route_identity_fingerprint(
    entry: Any,
    *,
    identity_fingerprint_loader: IdentityFingerprintLoader | None = None,
    require_identity_fingerprint: bool = False,
) -> str | None:
    """Read an authoritative route fingerprint without returning its value."""

    for field in (
        "ssh_auth_fingerprint",
        "public_key_fingerprint",
        "identity_fingerprint",
        "fingerprint",
        "credential_id",
    ):
        declared = _registry_value(entry, field)
        if declared is not None:
            return _normalize_identity_fingerprint(
                declared, field=f"client route {field}"
            )
    identity_file = _registry_value(entry, "identity_file")
    if identity_fingerprint_loader is not None:
        if not isinstance(identity_file, (str, Path)):
            raise CoordinationAdapterUnavailable(
                "operator client identity fingerprint is unavailable"
            )
        try:
            observed = identity_fingerprint_loader(Path(identity_file))
        except Exception as exc:  # noqa: BLE001 - sanitize key-reader details
            raise CoordinationAdapterUnavailable(
                "operator client identity fingerprint is unavailable"
            ) from exc
        if observed is None:
            if require_identity_fingerprint:
                raise CoordinationAdapterUnavailable(
                    "operator client identity fingerprint is unavailable"
                )
            return None
        return _normalize_identity_fingerprint(
            observed, field="client route identity fingerprint"
        )
    if not require_identity_fingerprint:
        return None
    return _derive_identity_fingerprint(identity_file)


def _directory_has_write_search_access(path: Path) -> bool:
    """Check effective mode bits for the directory that bounds a target."""

    try:
        metadata = path.lstat()
    except OSError as exc:
        raise CoordinationAdapterUnavailable(
            "worktree destination boundary is unavailable"
        ) from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise CoordinationAdapterUnavailable(
            "worktree destination boundary is not a directory"
        )
    mode = stat.S_IMODE(metadata.st_mode)
    uid = os.geteuid()
    gid = os.getegid()
    if metadata.st_uid == uid:
        permissions = (mode >> 6) & 0o7
    elif metadata.st_gid == gid or metadata.st_gid in os.getgroups():
        permissions = (mode >> 3) & 0o7
    else:
        permissions = mode & 0o7
    if permissions & 0o3 != 0o3 or not os.access(path, os.W_OK | os.X_OK):
        raise CoordinationAdapterUnavailable(
            "worktree destination boundary is not writable"
        )
    return True


def _preflight_worktree_destination(
    value: Any, *, repo_root: Path | None = None
) -> None:
    """Validate an assignment worktree target without creating directories.

    The path is walked lexically so a symlink cannot redirect Git's later
    worktree creation.  Missing descendants are allowed, but their nearest
    existing directory must already be writable and searchable.
    """

    if not isinstance(value, (str, Path)):
        raise CoordinationAdapterUnavailable("worktree destination is unavailable")
    target = Path(value)
    if not target.is_absolute() or any(part in {".", ".."} for part in target.parts):
        raise CoordinationAdapterUnavailable("worktree destination is unavailable")
    current = Path(target.anchor)
    boundary: Path | None = None
    target_exists = False
    for component in target.parts[1:]:
        current = current / component
        try:
            metadata = current.lstat()
        except FileNotFoundError:
            boundary = current.parent
            break
        except OSError as exc:
            raise CoordinationAdapterUnavailable(
                "worktree destination is unavailable"
            ) from exc
        if stat.S_ISLNK(metadata.st_mode):
            raise CoordinationAdapterUnavailable(
                "worktree destination must not traverse a symlink"
            )
        if not stat.S_ISDIR(metadata.st_mode):
            raise CoordinationAdapterUnavailable(
                "worktree destination is not a directory"
            )
        if current == target:
            target_exists = True
    if boundary is None:
        boundary = target.parent
    _directory_has_write_search_access(boundary)
    if target_exists:
        _directory_has_write_search_access(target)
    if repo_root is not None:
        _reject_repo_worktree_overlap(repo_root, target)


def _reject_repo_worktree_overlap(repo_path: Path, worktree_path: Path) -> None:
    """Keep a staged worktree outside both sides of the source checkout."""

    try:
        repo = repo_path.resolve()
        worktree = worktree_path.resolve()
        worktree_inside_repo = worktree == repo or repo in worktree.parents
        repo_inside_worktree = repo == worktree or worktree in repo.parents
    except OSError as exc:
        raise CoordinationAdapterUnavailable(
            "setup repository/worktree boundary is unavailable"
        ) from exc
    if worktree_inside_repo or repo_inside_worktree:
        raise CoordinationAdapterUnavailable("setup repository/worktree paths overlap")


def _validate_pact_binding(root: Path, *, change_id: str, task_id: str) -> None:
    pact_path = root / "pact" / "changes" / change_id / "pact.yaml"
    try:
        metadata = pact_path.lstat()
    except OSError as exc:
        raise CoordinationAdapterUnavailable(
            "tracked PACT authority is unavailable"
        ) from exc
    if not stat.S_ISREG(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise CoordinationAdapterUnavailable(
            "tracked PACT authority is not a regular file"
        )
    try:
        import yaml

        document = yaml.safe_load(pact_path.read_bytes())
    except Exception as exc:  # noqa: BLE001 - safe setup boundary
        raise CoordinationAdapterUnavailable(
            "tracked PACT authority is unreadable"
        ) from exc
    if not isinstance(document, Mapping):
        raise CoordinationAdapterUnavailable("tracked PACT authority is incomplete")
    # v5 carries the change id at the top level; v4's `change:` mapping is
    # gone from the corpus, and CI refuses a v4 manifest outright, so this
    # reads the v5 field only rather than accepting a shape that can no
    # longer be committed.
    tasks = document.get("tasks")
    if document.get("id") != change_id:
        raise CoordinationAdapterUnavailable("setup PACT change binding is stale")
    if not isinstance(tasks, list) or not any(
        isinstance(item, Mapping) and item.get("id") == task_id for item in tasks
    ):
        raise CoordinationAdapterUnavailable("setup PACT task binding is stale")


def load_setup_authority(
    path: str | Path,
    *,
    expected_deployment: str | None = None,
    repo_root: str | Path | None = None,
) -> SetupAuthority:
    """Load the strict startup-supplied JSON setup authority.

    ``path`` is intentionally required: this module never discovers it from
    an HTTP request or an ambient environment variable.
    """

    authority_path = Path(path)
    raw_bytes = _read_authority_bytes(authority_path)
    try:
        document = json.loads(
            raw_bytes.decode("utf-8"), object_pairs_hook=_no_duplicate_keys
        )
    except CoordinationAdapterUnavailable:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CoordinationAdapterUnavailable("setup authority is invalid JSON") from exc
    _reject_secret_fields(document)
    if not isinstance(document, Mapping) or set(document) != _TOP_LEVEL_FIELDS:
        raise CoordinationAdapterUnavailable("setup authority has unexpected fields")
    if document.get("schema") != SETUP_AUTHORITY_SCHEMA:
        raise CoordinationAdapterUnavailable("unsupported setup authority schema")
    deployment = _required_string(document.get("deployment"), field="deployment")
    if expected_deployment is not None and deployment != expected_deployment:
        raise CoordinationAdapterUnavailable(
            "setup authority deployment does not match startup"
        )
    pact = document.get("pact")
    chief = document.get("chief")
    if not isinstance(pact, Mapping) or set(pact) != _PACT_FIELDS:
        raise CoordinationAdapterUnavailable(
            "setup authority PACT section is incomplete"
        )
    if not isinstance(chief, Mapping) or set(chief) != _CHIEF_FIELDS:
        raise CoordinationAdapterUnavailable(
            "setup authority chief section is incomplete"
        )
    change_id = _required_string(pact.get("change_id"), field="pact.change_id")
    task_id = _required_string(pact.get("task_id"), field="pact.task_id")
    project_id = _required_string(chief.get("project_id"), field="chief.project_id")
    repo_path = _required_path(chief.get("repo_path"), field="chief.repo_path")
    worktree_path = _required_path(
        chief.get("worktree_path"), field="chief.worktree_path"
    )
    if repo_path == worktree_path:
        raise CoordinationAdapterUnavailable(
            "setup authority worktree must differ from repository"
        )
    git_branch = _required_string(
        chief.get("git_branch"), field="chief.git_branch", pattern=_BRANCH_RE
    )
    try:
        _validate_git_branch(git_branch)
    except ValueError as exc:
        raise CoordinationAdapterUnavailable(
            "setup authority has invalid chief.git_branch"
        ) from exc
    graph_base_branch = _required_string(
        chief.get("graph_base_branch"),
        field="chief.graph_base_branch",
        pattern=_BRANCH_RE,
    )
    try:
        _validate_git_branch(graph_base_branch)
    except ValueError as exc:
        raise CoordinationAdapterUnavailable(
            "setup authority has invalid chief.graph_base_branch"
        ) from exc
    if git_branch == graph_base_branch:
        raise CoordinationAdapterUnavailable(
            "setup authority target branch conflicts with graph base"
        )
    ssh_auth_fingerprint = _required_fingerprint(
        chief.get("ssh_auth_fingerprint"), field="chief.ssh_auth_fingerprint"
    )
    source_address = _required_loopback_address(
        chief.get("source_address"), field="chief.source_address"
    )
    target_address = _required_loopback_address(
        chief.get("target_address"), field="chief.target_address"
    )
    target_port = _required_port(chief.get("target_port"), field="chief.target_port")
    authenticated_host = (
        _required_string(
            chief.get("authenticated_host"), field="chief.authenticated_host"
        )
        .lower()
        .rstrip(".")
    )
    if authenticated_host != target_address:
        raise CoordinationAdapterUnavailable(
            "setup authority host does not match target address"
        )
    route = _required_string(chief.get("route"), field="chief.route")
    try:
        route = validate_coordination_route(route)
    except ValueError as exc:
        raise CoordinationAdapterUnavailable(
            "setup authority route is invalid"
        ) from exc
    if route.startswith("opaque:"):
        raise CoordinationAdapterUnavailable(
            "setup authority route must be a registry key"
        )
    grant_id = _required_string(chief.get("grant_id"), field="chief.grant_id")
    root = Path(repo_root).resolve() if repo_root is not None else repo_path
    if root != repo_path.resolve():
        raise CoordinationAdapterUnavailable(
            "setup authority repository does not match startup"
        )
    _reject_repo_worktree_overlap(repo_path, worktree_path)
    _validate_pact_binding(root, change_id=change_id, task_id=task_id)
    return SetupAuthority(
        schema=SETUP_AUTHORITY_SCHEMA,
        deployment=deployment,
        change_id=change_id,
        task_id=task_id,
        project_id=project_id,
        repo_path=repo_path,
        worktree_path=worktree_path,
        git_branch=git_branch,
        graph_base_branch=graph_base_branch,
        authenticated_host=authenticated_host,
        ssh_auth_fingerprint=ssh_auth_fingerprint,
        source_address=source_address,
        target_address=target_address,
        target_port=target_port,
        route=route,
        grant_id=grant_id,
        config_digest="sha256:" + hashlib.sha256(raw_bytes).hexdigest(),
    )


def _registry_parts(
    registry: Any, *, require_workspaces: bool = True
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    routes = getattr(registry, "routes", None)
    workspaces = getattr(registry, "workspaces", None)
    if isinstance(registry, Mapping):
        if not isinstance(routes, Mapping):
            routes = registry.get("routes")
        if not isinstance(workspaces, Mapping):
            workspaces = registry.get("workspaces")
    if not isinstance(routes, Mapping) or (
        require_workspaces and not isinstance(workspaces, Mapping)
    ):
        raise CoordinationAdapterUnavailable("operator route registry is incomplete")
    return routes, workspaces if isinstance(workspaces, Mapping) else {}


def _route_key(route_ref: Any) -> str:
    if not isinstance(route_ref, str) or not route_ref.startswith("opaque:"):
        raise CoordinationAdapterUnavailable(
            "tracked policy contains a non-opaque route"
        )
    return route_ref[len("opaque:") :]


def _registry_route_entry(routes: Mapping[str, Any], route: str) -> Any:
    if route in routes:
        entry = routes[route]
    elif f"opaque:{route}" in routes:
        entry = routes[f"opaque:{route}"]
    else:
        raise CoordinationAdapterUnavailable(
            "tracked client route is absent from operator registry"
        )
    configured_route = _registry_route_name(entry)
    if configured_route is not None and configured_route not in {
        route,
        f"opaque:{route}",
    }:
        raise CoordinationAdapterUnavailable("operator route registry is mismatched")
    return entry


def _registry_route_name(value: Any) -> str | None:
    route = getattr(value, "route", None)
    if route is None and isinstance(value, Mapping):
        route = value.get("route")
    return str(route).strip() if route is not None else None


def _registry_value(value: Any, field: str) -> Any:
    """Read one route field from a native object or a hermetic mapping."""

    result = getattr(value, field, None)
    if result is None and isinstance(value, Mapping):
        result = value.get(field)
    return result


def _canonical_route_name(route_key: Any, entry: Any) -> str:
    """Return the route name independent of an optional opaque key prefix."""

    configured = _registry_route_name(entry)
    if configured is not None:
        return configured.removeprefix("opaque:")
    return str(route_key).removeprefix("opaque:")


def _server_route_bindings(
    routes: Mapping[str, Any],
    *,
    selected_routes: set[str] | None = None,
    expected_deployment: str,
    expected_repo_root: Path,
    current_git_commit: str | None = None,
    service_dsn: str | None = None,
) -> dict[str, dict[str, str | int]]:
    """Validate selected forced-command routes and return a digestable view.

    The server registry is a physical authority, not just a route-name
    allowlist.  Every route used by the tracked client policy must point at
    this deployment and this exact trusted checkout, and its expected Git
    commit must agree with the setup checkout whenever that head is known.
    The returned values are only used as digest input; callers never put them
    on the transport surface.
    """

    trusted_root = expected_repo_root.resolve()
    expected_head = (
        None if current_git_commit is None else str(current_git_commit).strip().lower()
    )
    if expected_head is not None and not _COMMIT_RE.fullmatch(expected_head):
        raise CoordinationAdapterUnavailable("Git setup authority is partial")
    service_identity = (
        None if service_dsn is None else _canonical_postgres_identity(service_dsn)
    )
    selected = (
        None
        if selected_routes is None
        else {str(route).removeprefix("opaque:") for route in selected_routes}
    )
    result: dict[str, dict[str, str | int]] = {}
    for route_key, entry in sorted(routes.items(), key=lambda item: str(item[0])):
        route = _canonical_route_name(route_key, entry)
        if selected is not None and route not in selected:
            continue
        deployment = _registry_value(entry, "deployment_name")
        repo_root = _registry_value(entry, "repo_root")
        expected_commit = _registry_value(entry, "expected_git_commit")
        dsn = _registry_value(entry, "dsn")
        if (
            not isinstance(deployment, str)
            or deployment.strip() != expected_deployment
            or not isinstance(repo_root, (str, Path))
            or not isinstance(expected_commit, str)
            or not _COMMIT_RE.fullmatch(expected_commit.strip())
            or not isinstance(dsn, str)
            or not dsn.strip()
        ):
            raise CoordinationAdapterUnavailable("operator server route is incomplete")
        configured_root = Path(repo_root).expanduser()
        try:
            # Production's loader already proves this path is trusted.  Keep
            # the injected seam equally strict so a test cannot accidentally
            # turn a physical path into a logical-only check.
            if (
                not configured_root.is_absolute()
                or configured_root.is_symlink()
                or configured_root.resolve() != trusted_root
            ):
                raise CoordinationAdapterUnavailable(
                    "operator server route repository is mismatched"
                )
        except OSError as exc:
            raise CoordinationAdapterUnavailable(
                "operator server route repository is unavailable"
            ) from exc
        commit = expected_commit.strip().lower()
        if expected_head is not None and commit != expected_head:
            raise CoordinationAdapterUnavailable(
                "operator server route Git binding is stale"
            )
        if service_identity is not None:
            try:
                route_identity = _canonical_postgres_identity(dsn)
            except CoordinationAdapterUnavailable:
                raise
            if route_identity != service_identity:
                raise CoordinationAdapterUnavailable(
                    "operator server route database is mismatched"
                )
        result[route] = {
            "route": route,
            "deployment_name": deployment.strip(),
            "repo_root": str(configured_root.resolve()),
            "expected_git_commit": commit,
            # Include the DSN in the digest input, never in a returned
            # fragment.  A server credential rotation must invalidate a plan.
            "dsn": dsn.strip(),
        }
    return result


def _client_route_bindings(
    routes: Mapping[str, Any],
    workspaces: Mapping[str, Any],
    *,
    selected_routes: set[str] | None = None,
    identity_fingerprint_loader: IdentityFingerprintLoader | None = None,
    require_identity_fingerprint: bool = False,
) -> dict[str, Any]:
    """Extract selected physical client-route/grant bindings for hashing."""

    selected = (
        None
        if selected_routes is None
        else {str(route).removeprefix("opaque:") for route in selected_routes}
    )
    route_values: dict[str, Any] = {}
    for route_key, entry in sorted(routes.items(), key=lambda item: str(item[0])):
        route = _canonical_route_name(route_key, entry)
        if selected is not None and route not in selected:
            continue
        route_values[str(route_key)] = {
            field: _registry_value(entry, field)
            for field in (
                "route",
                "host",
                "user",
                "identity_file",
                "known_hosts_file",
                "port",
            )
        }
        route_values[str(route_key)]["identity_fingerprint"] = (
            _route_identity_fingerprint(
                entry,
                identity_fingerprint_loader=identity_fingerprint_loader,
                require_identity_fingerprint=require_identity_fingerprint,
            )
        )
    workspace_values: dict[str, Any] = {}
    for grant_key, entry in sorted(workspaces.items(), key=lambda item: str(item[0])):
        route = _canonical_route_name("", entry)
        if selected is not None and route not in selected:
            continue
        workspace_values[str(grant_key)] = {
            "grant_id": _registry_value(entry, "grant_id"),
            "route": _registry_value(entry, "route"),
        }
    return {"routes": route_values, "workspaces": workspace_values}


def _validate_client_identity_bindings(
    routes: Mapping[str, Any],
    *,
    selected_routes: set[str],
    expected_fingerprint: str,
    identity_fingerprint_loader: IdentityFingerprintLoader | None = None,
    require_identity_fingerprint: bool = False,
) -> None:
    """Fence selected routes to the setup authority's enrolled SSH key.

    A disk-backed ``ClientRegistry`` already requires every route to share one
    user and identity-file pair.  We repeat that invariant for injected
    registries and compare every available fingerprint; with the production
    loader, every selected route is derived and checked.  Thus the chief
    check is enough for the native registry's shared identity, while a custom
    registry cannot smuggle a second explicitly fingerprinted key through a
    non-chief route.
    """

    selected = {str(route).removeprefix("opaque:") for route in selected_routes}
    identity_bindings: set[tuple[str, str]] = set()
    for route_key, entry in routes.items():
        route = _canonical_route_name(route_key, entry)
        if route not in selected:
            continue
        user = _registry_value(entry, "user")
        identity_file = _registry_value(entry, "identity_file")
        if isinstance(user, str) and isinstance(identity_file, (str, Path)):
            identity_bindings.add((user.strip(), str(identity_file)))
        elif require_identity_fingerprint:
            raise CoordinationAdapterUnavailable(
                "operator client identity binding is incomplete"
            )
        observed = _route_identity_fingerprint(
            entry,
            identity_fingerprint_loader=identity_fingerprint_loader,
            require_identity_fingerprint=require_identity_fingerprint,
        )
        if observed is not None and observed != expected_fingerprint:
            raise CoordinationAdapterUnavailable(
                "operator client identity fingerprint is mismatched"
            )
    if len(identity_bindings) > 1:
        raise CoordinationAdapterUnavailable(
            "operator client identity binding is mismatched"
        )


def _physical_registry_digest(
    registry: Any,
    server_registry: Any,
    *,
    selected_routes: set[str] | None = None,
    identity_fingerprint_loader: IdentityFingerprintLoader | None = None,
    require_identity_fingerprint: bool = False,
) -> str:
    """Hash selected physical route bindings without exposing their values."""

    client_routes, client_workspaces = _registry_parts(registry)
    if server_registry is None:
        server_bindings: dict[str, Any] = {}
    else:
        server_routes, _server_workspaces = _registry_parts(
            server_registry, require_workspaces=False
        )
        server_bindings = {
            str(key): {
                field: _registry_value(entry, field)
                for field in (
                    "route",
                    "deployment_name",
                    "repo_root",
                    "expected_git_commit",
                    "dsn",
                )
            }
            for key, entry in sorted(
                server_routes.items(), key=lambda item: str(item[0])
            )
            if selected_routes is None
            or _canonical_route_name(key, entry)
            in {str(route).removeprefix("opaque:") for route in selected_routes}
        }
    return _digest(
        {
            "client": _client_route_bindings(
                client_routes,
                client_workspaces,
                selected_routes=selected_routes,
                identity_fingerprint_loader=identity_fingerprint_loader,
                require_identity_fingerprint=require_identity_fingerprint,
            ),
            "server": server_bindings,
        }
    )


def _validate_registries(
    policy: CoordinationPolicy,
    registry: ClientRegistry | Any,
    *,
    server_registry: Any = None,
    expected_deployment: str | None = None,
    expected_repo_root: Path | None = None,
    current_git_commit: str | None = None,
    service_dsn: str | None = None,
    expected_ssh_auth_fingerprint: str | None = None,
    identity_fingerprint_loader: IdentityFingerprintLoader | None = None,
    require_identity_fingerprint: bool = False,
) -> dict[str, Any]:
    routes, workspaces = _registry_parts(registry, require_workspaces=True)
    configured: dict[str, Any] = {}
    # Route names come from tracked policy (`.okg/coordination.yaml`); the
    # source carries no deployment's machine names.
    route_names = policy.client_route_names
    policy_routes = {
        name: _route_key(policy.client_routes.get(name)) for name in route_names
    }
    if len(set(policy_routes.values())) != len(route_names):
        raise CoordinationAdapterUnavailable(
            "tracked client routes are not physically distinct"
        )
    for name in route_names:
        route = policy_routes[name]
        _registry_route_entry(routes, route)
        grants = sorted(
            str(grant)
            for grant, workspace in workspaces.items()
            if _registry_route_name(workspace) in {route, f"opaque:{route}"}
        )
        if not grants:
            raise CoordinationAdapterUnavailable(
                "tracked client route has no operator grant"
            )
        configured[name] = {"route": route, "grants": grants}

    if server_registry is not None:
        server_routes, _server_workspaces = _registry_parts(
            server_registry, require_workspaces=False
        )
        if expected_deployment is not None and expected_repo_root is not None:
            _server_route_bindings(
                server_routes,
                selected_routes={value["route"] for value in configured.values()},
                expected_deployment=expected_deployment,
                expected_repo_root=expected_repo_root,
                current_git_commit=current_git_commit,
                service_dsn=service_dsn,
            )
        for value in configured.values():
            _registry_route_entry(server_routes, value["route"])
    if expected_ssh_auth_fingerprint is not None:
        try:
            expected_fingerprint = _normalize_identity_fingerprint(
                expected_ssh_auth_fingerprint,
                field="setup authority SSH fingerprint",
            )
        except CoordinationAdapterUnavailable:
            raise
        _validate_client_identity_bindings(
            routes,
            selected_routes={value["route"] for value in configured.values()},
            expected_fingerprint=expected_fingerprint,
            identity_fingerprint_loader=identity_fingerprint_loader,
            require_identity_fingerprint=require_identity_fingerprint,
        )
    return configured


def _load_client_registry_for(
    path: Path,
    loader: Callable[..., Any] | None,
) -> Any:
    if loader is None:
        loader = load_client_registry
    try:
        # Production registry readers retain their root-owned-file check.
        # Deliberately do not retry on TypeError: a real loader defect must
        # stay an unavailable authority, not silently select another call
        # shape.
        return loader(path, expected_uid=0)
    except Exception as exc:  # noqa: BLE001 - sanitize registry details
        raise CoordinationAdapterUnavailable(
            "operator client registry is unavailable"
        ) from exc


def _load_native_module(deployment_name: str) -> Any:
    try:
        deployment_dir = find_deployment_dir(deployment_name)
        if deployment_dir is None:
            raise CoordinationAdapterUnavailable(
                "deployment native coordination is unavailable"
            )
        inject_deployment_path(deployment_dir)
        module = importlib.import_module("coordination.native_state")
    except CoordinationAdapterUnavailable:
        raise
    except Exception as exc:  # noqa: BLE001 - sanitize import details
        raise CoordinationAdapterUnavailable(
            "deployment native coordination is unavailable"
        ) from exc
    origin = getattr(module, "__file__", None)
    if not origin:
        raise CoordinationAdapterUnavailable(
            "deployment native coordination is unavailable"
        )
    try:
        if deployment_dir.resolve() not in Path(origin).resolve().parents:
            raise CoordinationAdapterUnavailable(
                "deployment native coordination is stale"
            )
    except OSError as exc:
        raise CoordinationAdapterUnavailable(
            "deployment native coordination is unavailable"
        ) from exc
    required = ("read_control_state", "read_capacity_status", "AssignmentRequest")
    if not all(
        callable(getattr(module, name, None)) or name == "AssignmentRequest"
        for name in required
    ):
        raise CoordinationAdapterUnavailable(
            "deployment native coordination is incomplete"
        )
    return module


def _setup_plan_payload(plan: Any) -> Mapping[str, Any]:
    if isinstance(plan, Mapping):
        return dict(plan)
    unsigned = getattr(plan, "_unsigned_payload", None)
    if callable(unsigned):
        payload = unsigned()
        if isinstance(payload, Mapping):
            return dict(payload)
    as_dict = getattr(plan, "to_json", None)
    if callable(as_dict):
        payload = as_dict()
        if isinstance(payload, Mapping):
            payload = dict(payload)
            payload.pop("plan_digest", None)
            return payload
    raise CoordinationAdapterUnavailable("setup plan is not a mapping")


def _snapshot_digest(snapshot: Mapping[str, Mapping[str, Any]]) -> str:
    return _digest(snapshot)


def _snapshot_git_head(snapshot: Mapping[str, Mapping[str, Any]]) -> str | None:
    git = snapshot.get("git")
    if not isinstance(git, Mapping):
        return None
    binding = git.get("git_binding")
    if not isinstance(binding, Mapping):
        return None
    head = binding.get("head_commit")
    return head if isinstance(head, str) else None


def _read_source_git_head(repo_path: Path) -> str:
    """Read the pinned source checkout HEAD without touching its worktree."""

    try:
        snapshot = GitScopeProbe(
            repo_root=repo_path,
            config={"include_untracked": True, "purpose": "coordination-setup"},
            emit_targets=(GitScopeProbe,),
        ).capture_snapshot()
    except Exception as exc:  # noqa: BLE001 - sanitize live Git errors
        raise CoordinationAdapterUnavailable(
            "Git setup authority is unavailable"
        ) from exc
    head = snapshot.get("head_sha") if isinstance(snapshot, Mapping) else None
    if not isinstance(head, str) or not _COMMIT_RE.fullmatch(head.strip()):
        raise CoordinationAdapterUnavailable("Git setup authority is partial")
    return head.strip().lower()


def _connect_graph_store(dsn: str) -> Any:
    """Open a read/write-owned Chronos handle without ensuring schema."""

    from okg.substrate.graph_store import ChronosGraphStore

    return ChronosGraphStore.connect(dsn, ensure_schema=False)


class ProductionSetupLifecycle(PairedWorkspaceLifecycle):
    """Paired lifecycle that owns and closes its setup graph connection."""

    def __init__(
        self,
        store: Any,
        *,
        before_stage: Callable[[PairedWorkspaceRequest], None] | None = None,
    ) -> None:
        super().__init__(
            ProductionGitWorkspaceAdapter(),
            ProductionGraphWorkspaceAdapter.from_store(store),
        )
        self._setup_store = store
        self._setup_closed = False
        self._before_stage = before_stage

    def stage_or_adopt(self, request: PairedWorkspaceRequest) -> Any:
        """Recheck pinned setup authorities at the final write boundary."""

        if self._before_stage is not None:
            self._before_stage(request)
        return super().stage_or_adopt(request)

    def close(self) -> None:
        """Close the setup-owned graph handle once, even after close errors."""

        if self._setup_closed:
            return
        self._setup_closed = True
        closer = getattr(self._setup_store, "close", None)
        if callable(closer):
            try:
                closer()
            except Exception as exc:  # noqa: BLE001 - sanitize teardown errors
                raise CoordinationAdapterUnavailable(
                    "setup graph handle could not close"
                ) from exc


class _ProductionSetupProbes:
    """Read-only probes over the existing setup authorities."""

    def __init__(
        self,
        *,
        dsn: str,
        deployment_name: str,
        repo_root: Path,
        authority_loader: Callable[[], SetupAuthority],
        client_registry_path: Path,
        client_registry_loader: Callable[..., Any] | None,
        server_registry_loader: ServerRegistryLoader | None,
        reachability_checker: ReachabilityChecker | None,
        identity_fingerprint_loader: IdentityFingerprintLoader | None,
        require_identity_fingerprint: bool,
        pinned_physical_registry_digest: str,
    ) -> None:
        self._dsn = dsn
        self._deployment_name = deployment_name
        self._repo_root = repo_root
        self._authority_loader = authority_loader
        self._client_registry_path = client_registry_path
        self._client_registry_loader = client_registry_loader
        self._server_registry_loader = server_registry_loader
        self._reachability_checker = reachability_checker or _bounded_tcp_reachable
        self._identity_fingerprint_loader = identity_fingerprint_loader
        self._require_identity_fingerprint = require_identity_fingerprint
        self._pinned_physical_registry_digest = pinned_physical_registry_digest

    def _authority(self) -> SetupAuthority:
        return self._authority_loader()

    def _policy(self) -> CoordinationPolicy:
        try:
            return coordination_policy(self._repo_root)
        except Exception as exc:  # noqa: BLE001 - fail closed at probe boundary
            raise CoordinationAdapterUnavailable(
                "tracked coordination policy is unavailable"
            ) from exc

    def _registries(
        self, *, current_git_commit: str | None = None
    ) -> tuple[CoordinationPolicy, Any, Any, dict[str, Any]]:
        authority = self._authority()
        policy = self._policy()
        registry = _load_client_registry_for(
            self._client_registry_path, self._client_registry_loader
        )
        try:
            server = (
                self._server_registry_loader() if self._server_registry_loader else None
            )
        except Exception as exc:  # noqa: BLE001 - sanitize operator registry errors
            raise CoordinationAdapterUnavailable(
                "operator server registry is unavailable"
            ) from exc
        try:
            configured = _validate_registries(
                policy,
                registry,
                server_registry=server,
                expected_deployment=self._deployment_name,
                expected_repo_root=self._repo_root,
                current_git_commit=current_git_commit,
                service_dsn=self._dsn,
                expected_ssh_auth_fingerprint=authority.ssh_auth_fingerprint,
                identity_fingerprint_loader=self._identity_fingerprint_loader,
                require_identity_fingerprint=self._require_identity_fingerprint,
            )
        except CoordinationAdapterUnavailable:
            raise
        except Exception as exc:  # noqa: BLE001 - sanitize registry details
            raise CoordinationAdapterUnavailable(
                "operator route registry is unavailable"
            ) from exc
        physical_digest = _physical_registry_digest(
            registry,
            server,
            selected_routes={value["route"] for value in configured.values()},
            identity_fingerprint_loader=self._identity_fingerprint_loader,
            require_identity_fingerprint=self._require_identity_fingerprint,
        )
        if physical_digest != self._pinned_physical_registry_digest:
            raise CoordinationAdapterUnavailable(
                "operator route registry changed; restart is required"
            )
        return policy, registry, server, configured

    def git(self) -> Mapping[str, Any]:
        authority = self._authority()
        try:
            snapshot = GitScopeProbe(
                repo_root=authority.repo_path,
                config={"include_untracked": True, "purpose": "coordination-setup"},
                emit_targets=(GitScopeProbe,),
            ).capture_snapshot()
        except Exception as exc:  # noqa: BLE001 - sanitize live Git errors
            raise CoordinationAdapterUnavailable(
                "Git setup authority is unavailable"
            ) from exc
        head = str(snapshot.get("head_sha") or "")
        branch = str(snapshot.get("branch") or "")
        tree = str(snapshot.get("tree_sha") or "")
        dirty = snapshot.get("dirty_manifest")
        if not head or not branch or not tree or not isinstance(dirty, Mapping):
            raise CoordinationAdapterUnavailable("Git setup authority is partial")
        # Keep the configuration-collision aggregate owned by this probe.  A
        # separate graph probe must not emit a competing empty/list value,
        # because the composition layer correctly rejects disagreeing
        # authority fragments.  The setup head also fences every forced-
        # command server route's expected commit.
        self._registries(current_git_commit=head)
        collisions: list[str] = []
        if authority.git_branch == branch:
            collisions.append("git_target_branch_is_current")
        if authority.git_branch == authority.graph_base_branch:
            collisions.append("git_target_branch_is_graph_base")
        try:
            if ProductionGitWorkspaceAdapter().branch_exists(
                authority.repo_path, authority.git_branch
            ):
                collisions.append("git_target_branch_exists")
            store = _connect_graph_store(self._dsn)
            try:
                graph = ProductionGraphWorkspaceAdapter.from_store(store)
                if graph.branch_exists(self._deployment_name, authority.git_branch):
                    collisions.append("graph_target_branch_exists")
            finally:
                store.close()
            worktree = authority.worktree_path
            _preflight_worktree_destination(worktree, repo_root=authority.repo_path)
            if worktree.is_symlink() or (
                worktree.exists() and (not worktree.is_dir() or any(worktree.iterdir()))
            ):
                collisions.append("git_target_worktree_exists")
        except Exception as exc:  # noqa: BLE001 - sanitize Git target errors
            raise CoordinationAdapterUnavailable(
                "Git target collision authority is unavailable"
            ) from exc
        return {
            "git_ready": not bool(dirty),
            "configuration_collisions": sorted(set(collisions)),
            "git_binding": {
                "branch": authority.git_branch,
                "base_commit": head,
                "head_commit": head,
                # Never put the configured local path in the plan response.
                "worktree": f"opaque:worktree:{authority.config_digest[7:23]}",
            },
        }

    def pact(self) -> Mapping[str, Any]:
        authority = self._authority()
        policy = self._policy()
        pact_path = (
            self._repo_root / "pact" / "changes" / authority.change_id / "pact.yaml"
        )
        try:
            digest = "sha256:" + hashlib.sha256(pact_path.read_bytes()).hexdigest()
        except OSError as exc:
            raise CoordinationAdapterUnavailable(
                "tracked PACT authority is unavailable"
            ) from exc
        return {
            "pact_binding": {
                "change_id": authority.change_id,
                "task_id": authority.task_id,
                "intent_digest": digest,
            },
            "policy_binding": {
                "policy_digest": policy.policy_digest,
                "project_digest": policy.project_digest,
            },
        }

    def deployment(self) -> Mapping[str, Any]:
        try:
            manifest = resolve_deployment(self._deployment_name)
        except (DeploymentResolutionError, OSError, ValueError) as exc:
            raise CoordinationAdapterUnavailable(
                "deployment setup authority is unavailable"
            ) from exc
        if getattr(manifest, "name", None) != self._deployment_name:
            raise CoordinationAdapterUnavailable(
                "deployment setup authority is mismatched"
            )
        return {"deployment_exists": True, "deployment_ready": True}

    def graph(self) -> Mapping[str, Any]:
        authority = self._authority()
        try:
            store = _connect_graph_store(self._dsn)
            try:
                generation = store.latest_generation(
                    deployment_name=self._deployment_name,
                    branch=authority.graph_base_branch,
                    status="published",
                )
            finally:
                store.close()
        except Exception as exc:  # noqa: BLE001 - no DB details at the Viz boundary
            raise CoordinationAdapterUnavailable(
                "published graph generation is unavailable"
            ) from exc
        checkpoint = getattr(generation, "checkpoint_id", generation)
        if checkpoint is None or not str(checkpoint).strip():
            raise CoordinationAdapterUnavailable(
                "published graph generation is unavailable"
            )
        return {
            "okg_ready": True,
            "graph_binding": {
                "generation": str(checkpoint),
                "branch": authority.graph_base_branch,
            },
        }

    def native_control(self) -> Mapping[str, Any]:
        native = _load_native_module(self._deployment_name)
        try:
            with db_connect(self._dsn) as conn:
                state = native.read_control_state(
                    conn, deployment_name=self._deployment_name
                )
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT 1 FROM okg.coordination_native_state "
                        "WHERE deployment_name = %s LIMIT 1",
                        (self._deployment_name,),
                    )
                    assignment_exists = cur.fetchone() is not None
        except Exception as exc:  # noqa: BLE001 - read only and fail closed
            raise CoordinationAdapterUnavailable(
                "native coordination state is unavailable"
            ) from exc
        if not isinstance(state, Mapping):
            raise CoordinationAdapterUnavailable(
                "native coordination control is partial"
            )
        mode = str(state.get("control_mode") or "")
        if mode not in {"uninitialized", "preparing", "open", "frozen"}:
            raise CoordinationAdapterUnavailable(
                "native coordination control is invalid"
            )
        return {
            "ledger_empty": not assignment_exists,
            "operator_control_available": all(
                callable(getattr(native, name, None))
                for name in (
                    "prepare_bootstrap_control",
                    "record_bootstrap_pair_proof",
                    "complete_bootstrap_control",
                    "fallback_control",
                    "reactivate_control",
                )
            ),
            "identity_ready": bool(mode),
        }

    def capacity(self) -> Mapping[str, Any]:
        native = _load_native_module(self._deployment_name)
        try:
            with db_connect(self._dsn) as conn:
                status = native.read_capacity_status(
                    conn, deployment_name=self._deployment_name
                )
        except Exception as exc:  # noqa: BLE001 - read only and fail closed
            raise CoordinationAdapterUnavailable(
                "native capacity authority is unavailable"
            ) from exc
        if not isinstance(status, Mapping) or not isinstance(
            status.get("capacity_digest"), str
        ):
            raise CoordinationAdapterUnavailable("native capacity authority is partial")
        return {
            "capacity": {
                "ok": not any(
                    bool(status.get(name))
                    for name in (
                        "overage",
                        "lane_overage",
                        "named_resource_overage",
                    )
                ),
                "capacity_digest": status["capacity_digest"],
                "lane_capacity": status.get("lane_capacity"),
                "named_resource_capacity": status.get("named_resource_capacity"),
                "lane_usage": status.get("lane_usage"),
                "named_resource_usage": status.get("named_resource_usage"),
            }
        }

    def runtime(self) -> Mapping[str, Any]:
        authority = self._authority()
        policy, registry, server, configured = self._registries()
        chief = next(
            (
                value
                for value in configured.values()
                if value["route"] == authority.route
            ),
            None,
        )
        if authority.route not in {value["route"] for value in configured.values()}:
            raise CoordinationAdapterUnavailable("chief route is not tracked by policy")
        if not chief or authority.grant_id not in chief["grants"]:
            raise CoordinationAdapterUnavailable(
                "chief grant is not bound to its tracked route"
            )
        route_entry = _registry_route_entry(
            _registry_parts(registry)[0], authority.route
        )
        host = getattr(route_entry, "host", None)
        if host is None and isinstance(route_entry, Mapping):
            host = route_entry.get("host")
        if (
            authority.route.startswith("local-")
            and str(host or "").lower().rstrip(".") not in LOCAL_HOSTS
        ):
            raise CoordinationAdapterUnavailable("local chief route is not local")
        _route_host, route_port = _route_host_port(route_entry)
        if route_port != authority.target_port:
            raise CoordinationAdapterUnavailable("local chief target port is stale")
        registry_digest = _digest(
            {
                "authority": authority.config_digest,
                "routes": configured,
                "policy_routes": dict(policy.client_routes),
                "physical_registry": _physical_registry_digest(
                    registry,
                    server,
                    selected_routes={value["route"] for value in configured.values()},
                    identity_fingerprint_loader=self._identity_fingerprint_loader,
                    require_identity_fingerprint=self._require_identity_fingerprint,
                ),
            }
        )
        return {
            "credential_references": {
                name: "${OKG_AGENT_SSH_GRANT}" for name in configured
            },
            "runtime_binding": {
                "route_ref": f"opaque:{authority.route}",
                "config_digest": registry_digest,
            },
            # This is a safe digest-only binding.  It lets the chief/pair
            # factories prove that their pinned authority is the same one
            # used by the production probes without exposing the authority
            # file or any physical route value.
            "setup_authority_digest": authority.config_digest,
        }

    def route(self) -> Mapping[str, Any]:
        authority = self._authority()
        _policy, registry, _server, configured = self._registries()
        if authority.route not in {value["route"] for value in configured.values()}:
            raise CoordinationAdapterUnavailable("chief route is not tracked by policy")
        if not authority.route.startswith("local-"):
            raise CoordinationAdapterUnavailable(
                "setup supports only a local chief route"
            )
        host_reachability: dict[str, bool] = {}
        for name in configured:
            route_entry = _registry_route_entry(
                _registry_parts(registry)[0], configured[name]["route"]
            )
            host, port = _route_host_port(route_entry)
            try:
                host_reachability[name] = bool(self._reachability_checker(host, port))
            except Exception:  # noqa: BLE001 - unavailable route is a false check
                host_reachability[name] = False
        return {
            "local_only": True,
            "host_reachability": host_reachability,
        }


def _chief_binding(
    authority: SetupAuthority,
    *,
    plan: Mapping[str, Any],
    snapshot: Mapping[str, Mapping[str, Any]],
) -> str:
    return _digest(
        {
            "authority": authority.config_digest,
            "plan": plan,
            "snapshot": snapshot,
        }
    )


def _provision_attempt_id(chief_request: Any) -> str:
    """Derive one stable, non-secret provisioning fence for a chief."""

    assignment_id = getattr(chief_request, "assignment_id", None)
    assignment_generation = getattr(chief_request, "assignment_generation", None)
    if (
        not isinstance(assignment_id, str)
        or not assignment_id.strip()
        or isinstance(assignment_generation, bool)
        or not isinstance(assignment_generation, int)
        or assignment_generation < 1
    ):
        raise CoordinationAdapterUnavailable("chief request has no provisioning fence")
    return _digest(
        {
            "assignment_id": assignment_id,
            "assignment_generation": assignment_generation,
        }
    )


def _require_pinned_authority_snapshot(
    authority: SetupAuthority, snapshot: Mapping[str, Mapping[str, Any]]
) -> None:
    """Keep server-created assignments on the authority used by probes."""

    runtime = snapshot.get("runtime")
    if not isinstance(runtime, Mapping):
        raise CoordinationAdapterUnavailable("pair snapshot has no runtime authority")
    observed = runtime.get("setup_authority_digest")
    if observed != authority.config_digest:
        raise CoordinationAdapterUnavailable(
            "pair snapshot authority is stale or incomplete"
        )


def build_coordination_adapters(
    *,
    dsn: str,
    deployment_name: str,
    repo_root: str | Path,
    setup_authority_path: str | Path,
    client_registry_path: str | Path = DEFAULT_CLIENT_REGISTRY_PATH,
    client_registry_loader: Callable[..., Any] | None = None,
    server_registry_loader: ServerRegistryLoader | None = None,
    reachability_checker: ReachabilityChecker | None = None,
    identity_fingerprint_loader: IdentityFingerprintLoader | None = None,
    setup_probes: SetupAuthorityProbes | None = None,
) -> CoordinationAdapterBundle:
    """Compose the server-owned setup callbacks for one production process.

    The startup caller supplies the authority path explicitly.  Construction
    validates it and all tracked route references, but database reads and
    filesystem probes remain lazy and read-only until the setup-plan route is
    invoked.  The lifecycle callback is the only callback that can stage a
    pair, and it uses the existing paired-workspace implementation.
    """

    if not isinstance(dsn, str) or not dsn.strip():
        raise CoordinationAdapterUnavailable("setup DSN is unavailable")
    bound_deployment = deployment_name
    root = Path(repo_root).resolve()
    initial = load_setup_authority(
        setup_authority_path,
        expected_deployment=deployment_name,
        repo_root=root,
    )
    if initial.deployment != deployment_name:
        raise CoordinationAdapterUnavailable("setup authority deployment is mismatched")
    registry_path = Path(client_registry_path)
    # Validate route shape at construction so startup cannot install a bundle
    # whose protected chief route is already impossible.  The probes repeat
    # this read before each plan snapshot to detect operator changes.
    registry = _load_client_registry_for(registry_path, client_registry_loader)
    policy = coordination_policy(root)
    effective_server_loader = server_registry_loader or (
        lambda: coordination_ssh.load_server_config()
    )
    try:
        server_registry = effective_server_loader()
    except Exception as exc:  # noqa: BLE001 - sanitize server registry details
        raise CoordinationAdapterUnavailable(
            "operator server registry is unavailable"
        ) from exc
    if server_registry is None:
        raise CoordinationAdapterUnavailable("operator server registry is unavailable")
    require_identity_fingerprint = (
        client_registry_loader is None or identity_fingerprint_loader is not None
    )
    configured = _validate_registries(
        policy,
        registry,
        server_registry=server_registry,
        expected_deployment=deployment_name,
        expected_repo_root=root,
        service_dsn=dsn,
        expected_ssh_auth_fingerprint=initial.ssh_auth_fingerprint,
        identity_fingerprint_loader=identity_fingerprint_loader,
        require_identity_fingerprint=require_identity_fingerprint,
    )
    if initial.route not in {value["route"] for value in configured.values()}:
        raise CoordinationAdapterUnavailable("chief route is not tracked by policy")
    selected = next(
        value for value in configured.values() if value["route"] == initial.route
    )
    if initial.grant_id not in selected["grants"]:
        raise CoordinationAdapterUnavailable(
            "chief grant is not bound to its tracked route"
        )
    if not initial.route.startswith("local-"):
        raise CoordinationAdapterUnavailable("setup supports only a local chief route")
    selected_routes = {value["route"] for value in configured.values()}
    pinned_physical_registry_digest = _physical_registry_digest(
        registry,
        server_registry,
        selected_routes=selected_routes,
        identity_fingerprint_loader=identity_fingerprint_loader,
        require_identity_fingerprint=require_identity_fingerprint,
    )

    def validate_pinned_registries(*, current_git_commit: str | None = None) -> None:
        """Re-read physical authorities immediately before a write boundary."""

        current_registry = _load_client_registry_for(
            registry_path, client_registry_loader
        )
        try:
            current_server = effective_server_loader()
        except Exception as exc:  # noqa: BLE001 - sanitize operator registry errors
            raise CoordinationAdapterUnavailable(
                "operator server registry is unavailable"
            ) from exc
        if current_server is None:
            raise CoordinationAdapterUnavailable(
                "operator server registry is unavailable"
            )
        current_configured = _validate_registries(
            policy,
            current_registry,
            server_registry=current_server,
            expected_deployment=deployment_name,
            expected_repo_root=root,
            current_git_commit=current_git_commit,
            service_dsn=dsn,
            expected_ssh_auth_fingerprint=initial.ssh_auth_fingerprint,
            identity_fingerprint_loader=identity_fingerprint_loader,
            require_identity_fingerprint=require_identity_fingerprint,
        )
        current_digest = _physical_registry_digest(
            current_registry,
            current_server,
            selected_routes={value["route"] for value in current_configured.values()},
            identity_fingerprint_loader=identity_fingerprint_loader,
            require_identity_fingerprint=require_identity_fingerprint,
        )
        if current_digest != pinned_physical_registry_digest:
            raise CoordinationAdapterUnavailable(
                "operator route registry changed; restart is required"
            )

    def authority_loader() -> SetupAuthority:
        current = load_setup_authority(
            setup_authority_path,
            expected_deployment=deployment_name,
            repo_root=root,
        )
        if current.config_digest != initial.config_digest:
            raise CoordinationAdapterUnavailable(
                "setup authority changed; restart is required"
            )
        return current

    probes = setup_probes
    if probes is None:
        probes = _ProductionSetupProbes(
            dsn=dsn,
            deployment_name=deployment_name,
            repo_root=root,
            authority_loader=authority_loader,
            client_registry_path=registry_path,
            client_registry_loader=client_registry_loader,
            server_registry_loader=effective_server_loader,
            reachability_checker=reachability_checker,
            identity_fingerprint_loader=identity_fingerprint_loader,
            require_identity_fingerprint=require_identity_fingerprint,
            pinned_physical_registry_digest=pinned_physical_registry_digest,
        )

    def chief_resolver(
        *,
        plan: Mapping[str, Any],
        snapshot: Mapping[str, Mapping[str, Any]],
        deployment_name: str,
    ) -> Any:
        authority = authority_loader()
        _require_pinned_authority_snapshot(authority, snapshot)
        validate_pinned_registries(current_git_commit=_snapshot_git_head(snapshot))
        if (
            deployment_name != bound_deployment
            or authority.deployment != bound_deployment
        ):
            raise CoordinationAdapterUnavailable("chief deployment binding is stale")
        # Import the deployment-owned type after its path has been resolved;
        # this keeps cross-deployment native classes from being mixed.
        native = _load_native_module(bound_deployment)
        policy_now = coordination_policy(root)
        binding = _chief_binding(
            authority, plan=_setup_plan_payload(plan), snapshot=snapshot
        )
        digest = binding[len("sha256:") :]
        return native.AssignmentRequest(
            assignment_kind="chief",
            assignment_id=f"coordination:native:assignment:chief:{digest[:32]}",
            assignment_generation=1,
            deployment_name=bound_deployment,
            project_id=authority.project_id,
            change_id=authority.change_id,
            task_id=authority.task_id,
            owner_principal=f"chief:server:{digest[:32]}",
            owner_session_id=f"chief:server-session:{digest[32:64]}",
            git_worktree_id=str(authority.worktree_path),
            okg_branch_id=authority.git_branch,
            authenticated_host=authority.authenticated_host,
            credential_id=authority.credential_id,
            source_address=authority.source_address,
            source_port=None,
            target_address=authority.target_address,
            target_port=authority.target_port,
            allowed_event_types=("claim", "heartbeat", "renew", "release"),
            policy_digest=policy_now.policy_digest,
            project_digest=policy_now.project_digest,
        )

    def pair_request_factory(
        *,
        chief_request: Any,
        snapshot: Mapping[str, Mapping[str, Any]],
        deployment_name: str,
    ) -> PairedWorkspaceRequest:
        authority = authority_loader()
        _require_pinned_authority_snapshot(authority, snapshot)
        validate_pinned_registries(current_git_commit=_snapshot_git_head(snapshot))
        if deployment_name != authority.deployment:
            raise CoordinationAdapterUnavailable("pair deployment binding is stale")
        native = _load_native_module(deployment_name)
        if not isinstance(chief_request, native.AssignmentRequest):
            raise CoordinationAdapterUnavailable("chief request is not server-owned")
        if (
            chief_request.assignment_kind != "chief"
            or chief_request.assignment_generation != 1
        ):
            raise CoordinationAdapterUnavailable("chief request is invalid")
        if chief_request.deployment_name != deployment_name:
            raise CoordinationAdapterUnavailable("chief request deployment is stale")
        if chief_request.project_id != authority.project_id:
            raise CoordinationAdapterUnavailable("chief request project is stale")
        if chief_request.git_worktree_id != str(authority.worktree_path):
            raise CoordinationAdapterUnavailable("chief request worktree is stale")
        if chief_request.okg_branch_id != authority.git_branch:
            raise CoordinationAdapterUnavailable("chief request branch is stale")
        if (
            chief_request.authenticated_host not in LOCAL_HOSTS
            or chief_request.authenticated_host != authority.authenticated_host
        ):
            raise CoordinationAdapterUnavailable("pair supports only a local chief")
        if (
            chief_request.credential_id != authority.credential_id
            or chief_request.source_address != authority.source_address
            or chief_request.source_port is not None
            or chief_request.target_address != authority.target_address
            or chief_request.target_port != authority.target_port
        ):
            raise CoordinationAdapterUnavailable("chief transport binding is stale")
        graph_binding = snapshot.get("graph", {}).get("graph_binding")
        git_binding = snapshot.get("git", {}).get("git_binding")
        if not isinstance(graph_binding, Mapping) or not isinstance(
            git_binding, Mapping
        ):
            raise CoordinationAdapterUnavailable("pair snapshot is incomplete")
        if (
            git_binding.get("branch") != authority.git_branch
            or graph_binding.get("branch") != authority.graph_base_branch
        ):
            raise CoordinationAdapterUnavailable("pair snapshot branches are stale")
        base_commit = git_binding.get("head_commit")
        generation = graph_binding.get("generation")
        if (
            not isinstance(base_commit, str)
            or not base_commit
            or not isinstance(generation, str)
            or not generation
        ):
            raise CoordinationAdapterUnavailable("pair snapshot has no immutable bases")
        return PairedWorkspaceRequest(
            assignment_id=chief_request.assignment_id,
            assignment_generation=chief_request.assignment_generation,
            authenticated_host=authority.authenticated_host,
            repo_path=authority.repo_path,
            worktree_path=authority.worktree_path,
            git_branch=authority.git_branch,
            graph_deployment=deployment_name,
            graph_base_branch=authority.graph_base_branch,
            graph_branch=authority.git_branch,
            git_base_commit=base_commit,
            graph_base_generation=generation,
            graph_base_checkpoint=generation,
            provision_attempt_id=_provision_attempt_id(chief_request),
            mcp_route=authority.route,
            grant_id=authority.grant_id,
        )

    def lifecycle_factory(request: Any) -> PairedWorkspaceLifecycle:
        authority = authority_loader()
        if not isinstance(request, PairedWorkspaceRequest):
            raise CoordinationAdapterUnavailable("pair request is not typed")
        if (
            request.assignment_generation != 1
            or request.assignment_id.startswith("-")
            or request.authenticated_host not in LOCAL_HOSTS
            or request.authenticated_host != authority.authenticated_host
            or request.graph_deployment != deployment_name
            or Path(request.repo_path).resolve() != authority.repo_path.resolve()
            or Path(request.worktree_path).resolve()
            != authority.worktree_path.resolve()
            or request.git_branch != authority.git_branch
            or request.graph_branch != authority.git_branch
            or request.provision_attempt_id != _provision_attempt_id(request)
            or request.mcp_route != authority.route
            or request.grant_id != authority.grant_id
        ):
            raise CoordinationAdapterUnavailable(
                "pair request is outside protected setup authority"
            )
        validate_pinned_registries(current_git_commit=request.git_base_commit)
        try:
            store = _connect_graph_store(dsn)
        except Exception as exc:  # noqa: BLE001 - redact connection details
            raise CoordinationAdapterUnavailable(
                "graph workspace adapter is unavailable"
            ) from exc

        def validate_before_stage(stage_request: PairedWorkspaceRequest) -> None:
            authority_loader()
            validate_pinned_registries(current_git_commit=stage_request.git_base_commit)
            final_authority = authority_loader()
            _preflight_worktree_destination(
                final_authority.worktree_path,
                repo_root=final_authority.repo_path,
            )
            current_head = _read_source_git_head(final_authority.repo_path)
            planned_base = stage_request.git_base_commit
            if (
                not isinstance(planned_base, str)
                or current_head != planned_base.strip().lower()
            ):
                raise CoordinationAdapterUnavailable(
                    "Git source checkout changed; restart is required"
                )

        return ProductionSetupLifecycle(
            store,
            before_stage=validate_before_stage,
        )

    return CoordinationAdapterBundle(
        setup_probes=probes,
        chief_resolver=chief_resolver,
        pair_request_factory=pair_request_factory,
        lifecycle_factory=lifecycle_factory,
    )


def build_production_setup_adapters(
    *,
    dsn: str,
    deployment_name: str,
    repo_root: Path,
    config_path: Path,
    client_registry_path: Path = DEFAULT_CLIENT_REGISTRY_PATH,
    client_registry_loader: Callable[..., Any] | None = None,
    server_registry_loader: ServerRegistryLoader | None = None,
    reachability_checker: ReachabilityChecker | None = None,
    identity_fingerprint_loader: IdentityFingerprintLoader | None = None,
    setup_probes: SetupAuthorityProbes | None = None,
) -> ProductionSetupAdapters:
    """Build the production callback bundle from startup-owned config.

    ``config_path`` is intentionally explicit: the HTTP surface cannot
    select or replace the setup authority.  Optional readers are a narrow
    hermetic-test/deployment seam; normal production startup uses the
    operator-owned registry and the lazy read-only probes above.
    """

    try:
        return build_coordination_adapters(
            dsn=dsn,
            deployment_name=deployment_name,
            repo_root=repo_root,
            setup_authority_path=config_path,
            client_registry_path=client_registry_path,
            client_registry_loader=client_registry_loader,
            server_registry_loader=server_registry_loader,
            reachability_checker=reachability_checker,
            identity_fingerprint_loader=identity_fingerprint_loader,
            setup_probes=setup_probes,
        )
    except ProductionSetupUnavailable:
        raise
    except Exception as exc:  # noqa: BLE001 - one sanitized startup error
        raise ProductionSetupUnavailable(
            "production coordination setup is unavailable"
        ) from exc


# Compatibility spellings for startup code that names this boundary by its
# output rather than by the protocol it satisfies.
build_setup_adapters = build_coordination_adapters
build_coordination_adapter_bundle = build_coordination_adapters
build_native_setup_adapters = build_coordination_adapters


__all__ = [
    "CoordinationAdapterBundle",
    "CoordinationAdapterUnavailable",
    "MAX_SETUP_AUTHORITY_BYTES",
    "ProductionSetupAdapters",
    "ProductionSetupLifecycle",
    "ProductionSetupUnavailable",
    "ReachabilityChecker",
    "SETUP_AUTHORITY_SCHEMA",
    "SetupAuthority",
    "build_coordination_adapter_bundle",
    "build_coordination_adapters",
    "build_native_setup_adapters",
    "build_production_setup_adapters",
    "build_setup_adapters",
    "load_setup_authority",
]
