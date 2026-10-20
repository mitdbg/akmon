"""Host filesystem headroom sentinel (PACT ``bound-host-storage-pressure``).

The host hit ENOSPC twice on 2026-07-17 with ZERO warning from any operator
surface: every storage consumer watched ITSELF (the DB report reads the
deployment DSN, docker tracks volumes) and nothing watched the HOST
filesystem those consumers share. The first symptom of exhaustion was
Postgres PANIC. This module is the substrate-level sentinel:

* :func:`check_host_disk_headroom` — reports bytes + percent available for
  (a) the filesystem backing the deployment/repo dir and (b) the Docker
  data root when resolvable. Consumed by the scheduled doctor (the
  ``okg_substrate_doctor`` workflow shells ``okg doctor --deployment X
  --json``, which calls this) and by deployment readiness / dogfood status.
* :func:`big_writer_preflight` — refuses to start a known big write
  (``okg up``'s backup phase, the complete-reference harness's scratch-DB
  provisioning) when the write would land below the blocker floor, naming
  the shortfall (needed vs available) instead of writing the disk to zero
  mid-operation.

Threshold contract (P0: no silent default). WARN and BLOCKER thresholds are
deployment config — a ``host_storage:`` block in ``deployment.yaml``::

    host_storage:
      warn_available_pct: 15
      blocker_available_pct: 6

An undeclared or invalid pair is NEVER replaced by a baked-in default: the
check reports its own misconfiguration loudly (``status="misconfigured"``,
message says declare them) and big-writer enforcement stays explicitly
un-armed (annotated, not silently defaulted).

Probe injectability. All filesystem reads go through the module-level
:func:`probe_filesystem` (``os.statvfs``); callers accept a ``probe=``
parameter and internal call sites resolve the module attribute lazily, so
tests inject a fake probe (or monkeypatch ``host_headroom.probe_filesystem``)
and never touch real disk state.

The Docker data root is resolved by path existence only (macOS Docker
Desktop data dir, then ``/var/lib/docker``) — deliberately no ``docker
info`` subprocess, so the scheduled doctor's 60s budget can never hang on a
wedged daemon. On macOS the Docker.raw file typically lives on the same
APFS volume as the repo; both entries are still reported (honest, if
redundant) rather than deduped.
"""
from __future__ import annotations

import math
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping

from okg.substrate.support.user_hints import doc_reference

__all__ = [
    "BLOCKER_KEY",
    "FilesystemHeadroom",
    "HOST_STORAGE_BLOCK",
    "HostHeadroomVerdict",
    "HostStorageThresholds",
    "RECLAIM_RUNBOOK",
    "WARN_KEY",
    "big_writer_preflight",
    "check_host_disk_headroom",
    "load_host_storage_thresholds",
    "parse_host_storage_thresholds",
    "probe_filesystem",
    "resolve_docker_data_root",
]

# The reclaim runbook is owned by the sibling task task.host.vm-reclaim;
# this module only references it: the repository path in a checkout, the
# public documentation link for an installed package (issue #3480).
RECLAIM_RUNBOOK = doc_reference("docs/runbooks/troubleshooting/storage.md")

HOST_STORAGE_BLOCK = "host_storage"
WARN_KEY = "warn_available_pct"
BLOCKER_KEY = "blocker_available_pct"

# (total_bytes, available_bytes) for the filesystem backing a path.
ProbeFn = Callable[[Path], tuple[int, int]]


def probe_filesystem(path: Path) -> tuple[int, int]:
    """Return ``(total_bytes, available_bytes)`` for the filesystem
    backing ``path`` via ``os.statvfs``.

    Walks up to the nearest EXISTING ancestor first (``okg up``'s backup
    dir may not exist yet on a first run; its filesystem does). Raises
    ``OSError`` when even the root probe fails — the caller reports that
    loudly, never substitutes default numbers.
    """
    candidate = Path(path).expanduser().absolute()
    while not candidate.exists() and candidate.parent != candidate:
        candidate = candidate.parent
    st = os.statvfs(candidate)
    block = st.f_frsize or st.f_bsize
    return (block * st.f_blocks, block * st.f_bavail)


def resolve_docker_data_root() -> Path | None:
    """Best-effort host path holding Docker's data (the growth surface).

    Filesystem probes only — no ``docker`` subprocess (a wedged daemon
    must not hang the scheduled doctor). Returns None when no known
    location exists; the verdict then simply omits the docker entry.
    """
    candidates = (
        # macOS Docker Desktop: the VM file (Docker.raw) lives here.
        Path.home() / "Library" / "Containers" / "com.docker.docker" / "Data",
        # Linux dockerd default data-root.
        Path("/var/lib/docker"),
    )
    for candidate in candidates:
        try:
            if candidate.exists():
                return candidate
        except OSError:
            continue
    return None


@dataclass(frozen=True)
class HostStorageThresholds:
    """Declared WARN/BLOCKER floors, in percent of the filesystem
    available. ``warn`` must exceed ``blocker``; both in (0, 100)."""

    warn_available_pct: float
    blocker_available_pct: float

    def to_dict(self) -> dict[str, Any]:
        return {
            WARN_KEY: self.warn_available_pct,
            BLOCKER_KEY: self.blocker_available_pct,
        }


def parse_host_storage_thresholds(
    raw: Mapping[str, Any] | None,
    *,
    source_label: str = "deployment.yaml",
) -> tuple[HostStorageThresholds | None, list[str]]:
    """Validate a manifest's ``host_storage:`` block without executing it.

    Returns ``(thresholds, problems)``. ``problems`` is non-empty exactly
    when ``thresholds`` is None — every reason the pair could not be
    admitted is named so the check can report its own misconfiguration.
    There is NO fallback pair (P0: an undeclared threshold is a loud
    misconfiguration, never a silent default).
    """
    declare_hint = (
        f"declare {HOST_STORAGE_BLOCK}.{WARN_KEY} and "
        f"{HOST_STORAGE_BLOCK}.{BLOCKER_KEY} in deployment.yaml "
        "(percent available; warn above blocker, both in (0, 100))"
    )
    block = raw.get(HOST_STORAGE_BLOCK) if isinstance(raw, Mapping) else None
    if not isinstance(block, Mapping):
        return None, [
            f"{source_label} declares no {HOST_STORAGE_BLOCK} block; "
            f"{declare_hint}"
        ]
    problems: list[str] = []
    values: dict[str, float] = {}
    for key in (WARN_KEY, BLOCKER_KEY):
        value = block.get(key)
        if value is None:
            problems.append(
                f"{HOST_STORAGE_BLOCK}.{key} is undeclared; {declare_hint}"
            )
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            problems.append(
                f"{HOST_STORAGE_BLOCK}.{key} is not a number "
                f"({value!r}); {declare_hint}"
            )
            continue
        if not (0.0 < number < 100.0):
            problems.append(
                f"{HOST_STORAGE_BLOCK}.{key} must be in (0, 100), "
                f"got {number}; {declare_hint}"
            )
            continue
        values[key] = number
    if not problems and values[WARN_KEY] <= values[BLOCKER_KEY]:
        problems.append(
            f"{HOST_STORAGE_BLOCK}.{WARN_KEY} ({values[WARN_KEY]}) must "
            f"exceed {HOST_STORAGE_BLOCK}.{BLOCKER_KEY} "
            f"({values[BLOCKER_KEY]}); {declare_hint}"
        )
    if problems:
        return None, problems
    return (
        HostStorageThresholds(
            warn_available_pct=values[WARN_KEY],
            blocker_available_pct=values[BLOCKER_KEY],
        ),
        [],
    )


def load_host_storage_thresholds(
    deployment_dir: Path | None,
) -> tuple[HostStorageThresholds | None, list[str]]:
    """Read and validate ``host_storage:`` from ``deployment.yaml``."""
    declare_hint = (
        f"declare {HOST_STORAGE_BLOCK}.{WARN_KEY} and "
        f"{HOST_STORAGE_BLOCK}.{BLOCKER_KEY} in deployment.yaml "
        "(percent available; warn above blocker, both in (0, 100))"
    )
    if deployment_dir is None:
        return None, [f"deployment dir not found; {declare_hint}"]
    manifest_path = Path(deployment_dir) / "deployment.yaml"
    if not manifest_path.is_file():
        return None, [
            f"deployment.yaml not found at {manifest_path}; {declare_hint}"
        ]
    try:
        from okg.substrate import fastyaml

        raw = fastyaml.safe_load(
            manifest_path.read_text(encoding="utf-8")
        ) or {}
    except Exception as exc:  # noqa: BLE001 - unreadable manifest is itself the misconfiguration to report.
        return None, [f"deployment.yaml unreadable ({exc}); {declare_hint}"]
    return parse_host_storage_thresholds(
        raw if isinstance(raw, Mapping) else None,
    )


@dataclass(frozen=True)
class FilesystemHeadroom:
    """One probed filesystem: the numbers plus its classification
    against the declared thresholds (``status`` is ``unclassified``
    when no thresholds are admitted)."""

    path: str
    kind: str  # "deployment_dir" | "docker_data_root"
    total_bytes: int
    available_bytes: int
    status: str  # "ok" | "warn" | "blocker" | "unclassified"

    @property
    def available_pct(self) -> float:
        if self.total_bytes <= 0:
            return 0.0
        return round(100.0 * self.available_bytes / self.total_bytes, 2)

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "kind": self.kind,
            "total_bytes": self.total_bytes,
            "available_bytes": self.available_bytes,
            "available_pct": self.available_pct,
            "status": self.status,
        }


@dataclass(frozen=True)
class HostHeadroomVerdict:
    """Outcome of the host-headroom check.

    ``status`` is one of:

    * ``ok`` — every probed filesystem is above the warn floor.
    * ``warn`` — some filesystem is below warn but above blocker.
    * ``blocker`` — some filesystem is below the blocker floor; trust
      must degrade and the next action is the reclaim runbook.
    * ``misconfigured`` — the deployment declares no admissible
      threshold pair; the check reports that loudly instead of
      substituting a default (P0).
    * ``error`` — the probe itself failed; reported loudly, never
      papered over with default numbers.
    """

    deployment: str
    ok: bool
    status: str
    code: str
    message: str
    filesystems: tuple[FilesystemHeadroom, ...] = ()
    thresholds: HostStorageThresholds | None = None
    problems: tuple[str, ...] = field(default=())
    runbook: str = RECLAIM_RUNBOOK

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "status": self.status,
            "code": self.code,
            "message": self.message,
            "deployment": self.deployment,
            "filesystems": [fs.to_dict() for fs in self.filesystems],
            "thresholds": (
                self.thresholds.to_dict()
                if self.thresholds is not None
                else None
            ),
            "problems": list(self.problems),
            "runbook": self.runbook,
        }


def _classify_pct(
    available_pct: float, thresholds: HostStorageThresholds,
) -> str:
    if available_pct < thresholds.blocker_available_pct:
        return "blocker"
    if available_pct < thresholds.warn_available_pct:
        return "warn"
    return "ok"


def _probe_targets(
    *,
    deployment_dir: Path | None,
    repo_path: Path | None,
) -> list[tuple[str, Path]]:
    targets: list[tuple[str, Path]] = []
    anchor = repo_path or deployment_dir or Path.cwd()
    targets.append(("deployment_dir", Path(anchor)))
    docker_root = resolve_docker_data_root()
    if docker_root is not None:
        targets.append(("docker_data_root", docker_root))
    return targets


def check_host_disk_headroom(
    *,
    deployment: str,
    deployment_dir: Path | None = None,
    repo_path: Path | None = None,
    probe: ProbeFn | None = None,
) -> HostHeadroomVerdict:
    """Probe host filesystem headroom and classify it against the
    deployment's declared thresholds.

    ``deployment_dir`` defaults to ``find_deployment_dir(deployment)``;
    ``repo_path`` overrides the primary probe anchor (the repo checkout
    when the caller knows it). ``probe`` injects a fake filesystem for
    witnesses; the default is :func:`probe_filesystem`.
    """
    if deployment_dir is None:
        from okg.substrate.deployments import find_deployment_dir

        deployment_dir = find_deployment_dir(deployment)
    thresholds, problems = load_host_storage_thresholds(deployment_dir)
    # module attribute lookup (not a bare name) so a monkeypatched
    # `host_headroom.probe_filesystem` is honored even without `probe=`.
    probe_fn = probe if probe is not None else probe_filesystem

    filesystems: list[FilesystemHeadroom] = []
    for kind, target in _probe_targets(
        deployment_dir=deployment_dir, repo_path=repo_path,
    ):
        try:
            total, available = probe_fn(target)
        except OSError as exc:
            return HostHeadroomVerdict(
                deployment=deployment,
                ok=False,
                status="error",
                code="host_headroom_probe_failed",
                message=(
                    f"host headroom probe failed for {target} "
                    f"({kind}): {exc}; refusing to report default "
                    "numbers"
                ),
                filesystems=tuple(filesystems),
                thresholds=thresholds,
                problems=tuple(problems),
            )
        pct = (
            round(100.0 * available / total, 2) if total > 0 else 0.0
        )
        filesystems.append(
            FilesystemHeadroom(
                path=str(target),
                kind=kind,
                total_bytes=int(total),
                available_bytes=int(available),
                status=(
                    _classify_pct(pct, thresholds)
                    if thresholds is not None
                    else "unclassified"
                ),
            )
        )

    if thresholds is None:
        return HostHeadroomVerdict(
            deployment=deployment,
            ok=False,
            status="misconfigured",
            code="host_headroom_thresholds_undeclared",
            message=(
                f"host-storage headroom thresholds are undeclared for "
                f"{deployment!r}: " + "; ".join(problems)
            ),
            filesystems=tuple(filesystems),
            thresholds=None,
            problems=tuple(problems),
        )

    worst = min(
        filesystems,
        key=lambda fs: fs.available_pct,
        default=None,
    )
    if worst is None:  # pragma: no cover - _probe_targets always yields >= 1
        raise RuntimeError("host headroom check probed no filesystems")
    status = _classify_pct(worst.available_pct, thresholds)
    if status == "blocker":
        return HostHeadroomVerdict(
            deployment=deployment,
            ok=False,
            status="blocker",
            code="host_headroom_blocker",
            message=(
                f"host disk headroom is below the blocker floor: "
                f"{worst.available_bytes} bytes "
                f"({worst.available_pct}%) available on {worst.path}, "
                f"blocker floor is {thresholds.blocker_available_pct}% "
                f"— reclaim storage now ({RECLAIM_RUNBOOK})"
            ),
            filesystems=tuple(filesystems),
            thresholds=thresholds,
        )
    if status == "warn":
        return HostHeadroomVerdict(
            deployment=deployment,
            ok=True,
            status="warn",
            code="host_headroom_warn",
            message=(
                f"host disk headroom is below the warn floor: "
                f"{worst.available_bytes} bytes "
                f"({worst.available_pct}%) available on {worst.path}, "
                f"warn floor is {thresholds.warn_available_pct}% "
                f"(blocker at {thresholds.blocker_available_pct}%) — "
                f"plan a reclaim ({RECLAIM_RUNBOOK})"
            ),
            filesystems=tuple(filesystems),
            thresholds=thresholds,
        )
    return HostHeadroomVerdict(
        deployment=deployment,
        ok=True,
        status="ok",
        code="host_headroom_ok",
        message=(
            f"host disk headroom is above the warn floor "
            f"({worst.available_pct}% available at the tightest "
            f"filesystem)"
        ),
        filesystems=tuple(filesystems),
        thresholds=thresholds,
    )


def big_writer_preflight(
    *,
    operation: str,
    deployment: str,
    target_path: Path,
    deployment_dir: Path | None = None,
    needed_bytes: int | None = None,
    probe: ProbeFn | None = None,
    threshold_override: (
        tuple[HostStorageThresholds | None, list[str]] | None
    ) = None,
) -> dict[str, Any]:
    """Headroom preflight for a known big writer.

    Returns a payload dict; ``refuse=True`` means the caller MUST abort
    before writing. Semantics:

    * Thresholds declared + ``needed_bytes`` known: refuse when
      ``available - needed_bytes`` would land below the blocker floor
      (the write itself must not push the host into blocker territory).
    * Thresholds declared + ``needed_bytes`` unknown: refuse when the
      target filesystem is ALREADY below the blocker floor (the blocker
      threshold alone).
    * Probe failure with declared thresholds: refuse (fail closed — a
      big write with unmeasurable headroom is the incident shape).
    * Thresholds undeclared: enforcement is explicitly un-armed; the
      payload carries the loud misconfiguration (``status=
      "misconfigured"``, ``refuse=False``) so the caller's report names
      it — never a silently-defaulted floor (P0).

    Refusals name the shortfall: ``needed_bytes`` vs
    ``available_bytes``, plus the blocker floor and the missing bytes.
    """
    if deployment_dir is None:
        from okg.substrate.deployments import find_deployment_dir

        deployment_dir = find_deployment_dir(deployment)
    thresholds, problems = (
        threshold_override
        if threshold_override is not None
        else load_host_storage_thresholds(deployment_dir)
    )
    payload: dict[str, Any] = {
        "operation": operation,
        "deployment": deployment,
        "target_path": str(target_path),
        "thresholds": (
            thresholds.to_dict() if thresholds is not None else None
        ),
        "problems": list(problems),
        "needed_bytes": needed_bytes,
        "runbook": RECLAIM_RUNBOOK,
    }
    if thresholds is None:
        payload.update({
            "ok": False,
            "refuse": False,
            "status": "misconfigured",
            "code": "host_headroom_thresholds_undeclared",
            "message": (
                f"{operation}: host-storage headroom enforcement is NOT "
                f"armed for {deployment!r} — " + "; ".join(problems)
            ),
        })
        return payload

    probe_fn = probe if probe is not None else probe_filesystem
    try:
        total, available = probe_fn(Path(target_path))
    except OSError as exc:
        payload.update({
            "ok": False,
            "refuse": True,
            "status": "error",
            "code": "host_headroom_probe_failed",
            "message": (
                f"{operation} refused: host headroom probe failed for "
                f"{target_path} ({exc}); a big write with unmeasurable "
                "headroom is the ENOSPC incident shape"
            ),
        })
        return payload

    available_pct = (
        round(100.0 * available / total, 2) if total > 0 else 0.0
    )
    blocker_floor_bytes = int(
        math.ceil(total * thresholds.blocker_available_pct / 100.0)
    )
    projected_available = available - (needed_bytes or 0)
    payload.update({
        "total_bytes": int(total),
        "available_bytes": int(available),
        "available_pct": available_pct,
        "blocker_floor_bytes": blocker_floor_bytes,
        "projected_available_bytes": int(projected_available),
    })
    if projected_available < blocker_floor_bytes:
        shortfall = int(blocker_floor_bytes - projected_available)
        if needed_bytes is not None:
            detail = (
                f"needs ~{needed_bytes} bytes but only {available} bytes "
                f"({available_pct}%) are available on {target_path}; "
                f"finishing the write would land "
                f"{projected_available} bytes, below the blocker floor "
                f"of {blocker_floor_bytes} bytes "
                f"({thresholds.blocker_available_pct}%)"
            )
        else:
            detail = (
                f"only {available} bytes ({available_pct}%) are "
                f"available on {target_path}, below the blocker floor "
                f"of {blocker_floor_bytes} bytes "
                f"({thresholds.blocker_available_pct}%)"
            )
        payload.update({
            "ok": False,
            "refuse": True,
            "status": "blocker",
            "code": "host_headroom_blocker",
            "shortfall_bytes": shortfall,
            "message": (
                f"{operation} refused: {detail} — shortfall "
                f"{shortfall} bytes; reclaim storage first "
                f"({RECLAIM_RUNBOOK})"
            ),
        })
        return payload

    payload.update({
        "ok": True,
        "refuse": False,
        "status": "ok",
        "code": "host_headroom_ok",
        "message": (
            f"{operation}: headroom preflight passed "
            f"({available} bytes / {available_pct}% available; "
            f"blocker floor {blocker_floor_bytes} bytes)"
        ),
    })
    return payload
