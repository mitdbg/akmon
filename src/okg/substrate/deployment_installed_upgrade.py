"""`okg up` for a deployment that runs from an installed okg package (#3415).

A deployment made by `okg install` has no okg Git checkout behind it: it
declares `release.kind: standalone`, or okg runs from a verified installed
wheel (`release.kind: distribution`, declared or detected). The Git upgrade
engine (`deployment_upgrade.py`) syncs a code revision, which such a
deployment does not have. Jason's decision `upgrade A` (2026-10-05): the
operator upgrades the okg package itself (`uv tool upgrade`, `pip install
--upgrade`), then runs the same `okg up <deployment>`, which routes here.

The steps run in this order and stop at the first failure:

1. ``compat``: check the deployment against the okg now installed, with the
   breaking changes since the previous release's contract. A blocker refuses
   before anything is touched. An unknown result refuses too, unless the
   operator passes ``--accept-unknown-compat``; that never overrides a
   blocker.
2. ``preflight``: record which database this run works on (server
   identifier, database name, port), the graph readers see and the
   readiness findings that already exist.
3. ``stop_worker``: stop the deployment's runtime worker, if one runs, through
   `okg dogfood down`, and prove it is gone.
4. ``backup``: `pg_dump` the database, restore the archive into a disposable
   database on the same server, compare what matters, drop the disposable
   database. The archive and a sidecar describing it (`<archive>.json`) are
   kept; `okg deployment backup restore` restores from them.
5. ``migrate``: `okg migrate --apply`, then prove in-process that this
   database has no pending migration left.
6. ``deployment_apply``: `okg deployment apply` when the deployment is not
   already converged (catalog, Nomos baseline, sources, worker).
7. ``restart_worker``: start the worker again when this upgrade stopped it.
8. ``readiness``: `okg deployment ready` must report no blocker or unknown
   finding that it did not already report before the upgrade, and the
   generation readers saw before must still be published with the same node
   and edge counts.
9. ``receipt``: write the installed-release receipt.

Everything this module keeps for a deployment lives in its per-user state
directory, ``${OKG_USER_HOME:-$XDG_DATA_HOME/okg}/state/deployments/<name>/``
(owner-only): the receipt ``installed-release.json``, the ``backups/``
archives and, while an upgrade is unfinished, ``upgrade-in-progress.json``.
`okg install` writes the receipt too, so the next upgrade knows what it is
upgrading from. A deployment installed before this receipt existed has none;
its previous contract is then unknown, and the operator passes the previous
release's contract with ``--from-contract`` or accepts the unknown result.

Re-running is safe: every step is idempotent, the receipt is written only
after the whole sequence succeeds, and the in-progress marker carries the
restart obligation and the pre-upgrade archive into the next run. When the
receipt already names the installed okg and the database has no pending
migration, `okg up` reports ``up_to_date`` and changes nothing. An okg older
than the receipt's is refused: the way back is the restore verb.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from okg.substrate.errors import SubstrateError

RECEIPT_SCHEMA = "okg.installed_release.v1"
UPGRADE_SCHEMA = "okg.installed_upgrade.v1"
BACKUP_SCHEMA = "okg.installed_backup.v1"
MARKER_SCHEMA = "okg.installed_upgrade_marker.v1"
RECEIPT_NAME = "installed-release.json"
MARKER_NAME = "upgrade-in-progress.json"
BACKUPS_NAME = "backups"
RELEASES_URL = "https://github.com/mitdbg/akmon/releases"
VERIFY_DATABASE_PREFIX = "okg_upgrade_verify_"

STEPS: tuple[str, ...] = (
    "compat",
    "preflight",
    "stop_worker",
    "backup",
    "migrate",
    "deployment_apply",
    "restart_worker",
    "readiness",
    "receipt",
)

INSTALLED_RELEASE_KINDS = frozenset({"standalone", "distribution"})

# Seconds to wait for a restarted worker to show up in the process table.
WORKER_START_TIMEOUT_SECONDS = 60.0

_DEPLOYMENT_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")


class InstalledUpgradeIncompatible(SubstrateError):
    """The deployment does not work with the okg now installed."""

    code = "installed_upgrade_incompatible"


class InstalledUpgradeCompatUnknown(SubstrateError):
    """Compatibility with the okg now installed could not be established."""

    code = "installed_upgrade_compat_unknown"


class InstalledUpgradeRefused(SubstrateError):
    """`okg up` cannot upgrade this installed deployment as asked."""

    code = "installed_upgrade_refused"


class InstalledReceiptUnreadable(SubstrateError):
    """The installed-release receipt exists but cannot be trusted."""

    code = "installed_release_receipt_unreadable"


class InstalledUpgradeWorkerUnsupervised(SubstrateError):
    """okg has no worker service, and the upgrade needs one.

    Either the deployment enables the runtime (the upgrade converges the
    worker through its service) or a worker is running (the upgrade stops
    and restarts it through its service).
    """

    code = "installed_upgrade_worker_unsupervised"


# ------------------------------------------------------------------ routing


def installed_upgrade_route(deployment: str) -> dict[str, Any] | None:
    """Whether `okg up <deployment>` must take the installed-package path.

    Returns the route (release kind, how it was decided, deployment
    directory) or None for the Git upgrade engine. The rule is the one
    release authority uses (`deployment_release.current_release_artifact`):

    * ``release.kind: standalone`` or ``distribution`` declared: installed.
    * no kind and no ``release.pin.code_repository``: installed only when Git
      cannot find the okg checkout the running code came from AND the running
      okg is a verified installed wheel. A source checkout keeps the Git path.

    A manifest that cannot be found or read returns None; the Git path's own
    resolution refusals then fail closed with their causes.
    """
    from okg.substrate.deployment_authority import _read_manifest_with_dir

    raw, readable, deployment_dir = _read_manifest_with_dir(deployment)
    if not readable or not isinstance(raw, dict) or deployment_dir is None:
        return None
    release = raw.get("release")
    release = release if isinstance(release, dict) else {}
    kind = release.get("kind")
    kind = kind.strip() if isinstance(kind, str) else None
    if kind in INSTALLED_RELEASE_KINDS:
        return {
            "release_kind": kind,
            "decided_by": "declared",
            "deployment_dir": str(deployment_dir),
        }
    if kind is not None:
        return None
    pin = release.get("pin")
    if isinstance(pin, dict) and "code_repository" in pin:
        return None
    if not _framework_checkout_missing(Path(deployment_dir)):
        return None
    from okg.substrate.distributions.installed_framework import (
        installed_framework_distribution,
    )

    if installed_framework_distribution() is None:
        return None
    return {
        "release_kind": "distribution",
        "decided_by": "detected_installed_wheel",
        "deployment_dir": str(deployment_dir),
    }


def _framework_checkout_missing(deployment_dir: Path) -> bool:
    """True only when Git authority fails to find the okg checkout itself."""
    from okg.substrate.deployment_upgrade import (
        RevisionAuthorityError,
        _resolve_repo_authorities,
    )

    try:
        _resolve_repo_authorities(deployment_dir)
    except RevisionAuthorityError as exc:
        data = exc.data or {}
        return (
            data.get("authority_role") == "framework"
            and data.get("operation") == "discover_framework_repository"
        )
    return False


# -------------------------------------------------------------- state paths


def state_dir(deployment: str) -> Path:
    """The deployment's private state directory (receipt, backups, marker).

    ``${OKG_USER_HOME:-$XDG_DATA_HOME/okg}/state/deployments/<name>/``: out
    of the deployment directory, so a deployment kept in Git never commits a
    database dump or a receipt.
    """
    if not isinstance(deployment, str) or not _DEPLOYMENT_NAME.fullmatch(deployment):
        raise InstalledUpgradeRefused(
            "deployment name cannot name a state directory",
            data={"deployment": deployment},
            fix_hint="use a deployment name of letters, digits, '.', '_' and '-'",
        )
    from okg.substrate.deployments import _user_data_dir

    return _user_data_dir() / "state" / "deployments" / deployment


def _private_dir(path: Path) -> Path:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path, 0o700)
    return path


def receipt_path(deployment: str) -> Path:
    return state_dir(deployment) / RECEIPT_NAME


def marker_path(deployment: str) -> Path:
    return state_dir(deployment) / MARKER_NAME


def backups_dir(deployment: str) -> Path:
    return state_dir(deployment) / BACKUPS_NAME


def _write_private_json(path: Path, payload: Mapping[str, Any]) -> Path:
    """Write JSON atomically, owner-only (directory 0700, file 0600)."""
    _private_dir(path.parent)
    fd, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent,
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=1, sort_keys=True)
            handle.write("\n")
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise
    return path


def _read_private_json(path: Path) -> tuple[Any, str | None]:
    """(payload, problem). The payload is None when the file is absent."""
    if not path.exists() and not path.is_symlink():
        return None, None
    if path.is_symlink() or not path.is_file():
        return None, "not a regular file"
    try:
        return json.loads(path.read_text(encoding="utf-8")), None
    except (OSError, ValueError) as exc:
        return None, f"{type(exc).__name__}: {exc}"


# ------------------------------------------------------------------ receipt


def read_installed_release_receipt(
    deployment: str, deployment_dir: str | Path,
) -> dict[str, Any] | None:
    """The receipt, or None when none was written. A bad one refuses.

    A receipt written for another deployment directory (a deployment of the
    same name created elsewhere) is foreign and refused, never trusted.
    """
    path = receipt_path(deployment)
    payload, reason = _read_private_json(path)
    if payload is None and reason is None:
        return None
    if reason is None:
        reason = _receipt_problem(payload, deployment, Path(deployment_dir))
    if reason is not None:
        raise InstalledReceiptUnreadable(
            "installed-release receipt is unreadable",
            data={"path": str(path), "reason": reason},
            fix_hint=(
                "inspect the file; if it is damaged or belongs to another "
                "deployment, move it aside and pass the previous release's "
                "contract with --from-contract, or --accept-unknown-compat"
            ),
        )
    return payload


def _receipt_problem(
    payload: Any, deployment: str, deployment_dir: Path,
) -> str | None:
    if not isinstance(payload, Mapping):
        return "not a JSON object"
    if payload.get("schema") != RECEIPT_SCHEMA:
        return f"schema is {payload.get('schema')!r}, expected {RECEIPT_SCHEMA!r}"
    for key in ("okg_version", "contract_digest", "schema_checksum", "deployment_dir"):
        if not isinstance(payload.get(key), str) or not payload.get(key):
            return f"{key} is missing"
    if payload.get("deployment") != deployment:
        return f"receipt names deployment {payload.get('deployment')!r}"
    if payload.get("deployment_dir") != str(deployment_dir):
        return (
            f"receipt was written for deployment directory "
            f"{payload.get('deployment_dir')}, not {deployment_dir}"
        )
    contract = payload.get("contract")
    if not isinstance(contract, Mapping) or not isinstance(
        contract.get("sections"), Mapping
    ):
        return "contract is missing"
    if _contract_digest(contract) != payload.get("contract_digest"):
        return "contract does not match contract_digest"
    return None


def _contract_digest(contract: Mapping[str, Any]) -> str:
    from okg.substrate.release_contract import render_contract

    return "sha256:" + hashlib.sha256(
        render_contract(contract).encode("utf-8")
    ).hexdigest()


def _schema_checksum() -> str:
    """Digest of every migration this okg ships (id and checksum)."""
    from okg.substrate.db.management import current_known_migrations

    lines = "".join(
        f"{m.migration_id}:{m.checksum}\n" for m in current_known_migrations()
    )
    return "sha256:" + hashlib.sha256(lines.encode("utf-8")).hexdigest()


def current_installed_identity() -> dict[str, Any]:
    """The okg that is running: version, wheel digest, schema, contract."""
    import okg
    from okg.substrate.distributions.installed_framework import (
        installed_framework_distribution,
    )
    from okg.substrate.release_contract import build_contract

    framework = installed_framework_distribution()
    contract = build_contract()
    return {
        "okg_version": str(okg.__version__),
        "payload_digest": framework.payload_digest if framework else None,
        "installed_from_wheel": framework is not None,
        "schema_checksum": _schema_checksum(),
        "contract_digest": _contract_digest(contract),
        "contract": contract,
    }


def write_installed_release_receipt(
    deployment: str,
    deployment_dir: str | Path,
    *,
    written_by: str,
    identity: Mapping[str, Any] | None = None,
    previous: Mapping[str, Any] | None = None,
    upgrade: Mapping[str, Any] | None = None,
) -> Path:
    """Write the receipt atomically into the deployment's state directory."""
    identity = identity or current_installed_identity()
    payload: dict[str, Any] = {
        "schema": RECEIPT_SCHEMA,
        "deployment": deployment,
        "deployment_dir": str(deployment_dir),
        "written_by": written_by,
        "written_at": _now(),
        "okg_version": identity["okg_version"],
        "payload_digest": identity["payload_digest"],
        "installed_from_wheel": identity["installed_from_wheel"],
        "schema_checksum": identity["schema_checksum"],
        "contract_digest": identity["contract_digest"],
        "contract": identity["contract"],
    }
    if previous is not None:
        payload["previous"] = dict(previous)
    if upgrade is not None:
        payload["upgrade"] = dict(upgrade)
    return _write_private_json(receipt_path(deployment), payload)


def record_install_receipt(deployment: str | None) -> dict[str, Any]:
    """After a successful `okg install`: write the receipt when it applies.

    Returns what happened, so the caller can say it. It never raises: the
    install itself succeeded. ``error`` means no receipt could be written
    (the next upgrade then needs ``--from-contract`` or
    ``--accept-unknown-compat``); ``warning`` means a receipt for a
    different okg already exists and was kept, because overwriting it would
    let the next `okg up` skip the upgrade that receipt still needs.
    """
    if not deployment:
        return {"written": False, "reason": "no deployment name"}
    try:
        route = installed_upgrade_route(deployment)
        if route is None:
            return {"written": False, "reason": "not an installed-package deployment"}
        identity = current_installed_identity()
        existing = read_installed_release_receipt(deployment, route["deployment_dir"])
        path = receipt_path(deployment)
        if existing is not None and not _same_install(existing, identity):
            return {
                "written": False,
                "kept": str(path),
                "warning": (
                    f"kept the existing installed-release receipt {path}: it "
                    f"records okg {existing.get('okg_version')}, and this "
                    f"install ran okg {identity['okg_version']}. Run `okg up "
                    f"{deployment}` to upgrade the deployment; it checks "
                    "compatibility and backs the database up first"
                ),
            }
        path = write_installed_release_receipt(
            deployment, route["deployment_dir"],
            written_by="install", identity=identity,
        )
    except Exception as exc:  # noqa: BLE001 - reported loudly by the caller.
        return {
            "written": False,
            "error": f"{type(exc).__name__}: {exc}",
        }
    return {"written": True, "path": str(path)}


# ------------------------------------------------------------------- marker


def read_upgrade_marker(deployment: str) -> dict[str, Any] | None:
    path = marker_path(deployment)
    payload, reason = _read_private_json(path)
    if payload is None and reason is None:
        return None
    if reason is None and (
        not isinstance(payload, Mapping) or payload.get("schema") != MARKER_SCHEMA
    ):
        reason = "not an upgrade marker"
    if reason is not None:
        raise InstalledUpgradeRefused(
            "the unfinished-upgrade marker is unreadable",
            data={"deployment": deployment, "path": str(path), "reason": reason},
            fix_hint=(
                "inspect the file; it records whether the last `okg up` "
                "stopped the worker and which archive was taken before it"
            ),
        )
    return dict(payload)


def _write_marker(deployment: str, marker: Mapping[str, Any]) -> None:
    _write_private_json(marker_path(deployment), {**marker, "schema": MARKER_SCHEMA})


# ------------------------------------------------------------------ upgrade

CommandRunner = Callable[[list[str], dict[str, str]], Any]


def run_installed_upgrade(
    options: Any,
    route: Mapping[str, Any],
    *,
    command_runner: CommandRunner | None = None,
) -> dict[str, Any]:
    """Upgrade one installed deployment to the okg that is running now."""
    _refuse_inapplicable_options(options, route)
    deployment = str(options.deployment)
    state_dir(deployment)  # validates the name before anything else
    deployment_dir = Path(str(route["deployment_dir"]))
    _refuse_tracked_backup_dir(options)
    runner = command_runner or _default_runner()
    identity = current_installed_identity()
    if route.get("release_kind") == "distribution" and not identity["installed_from_wheel"]:
        raise InstalledUpgradeRefused(
            "deployment requires okg installed from a wheel",
            data={"deployment": deployment, "release_kind": "distribution"},
            fix_hint=(
                "install okg from a built wheel into the environment that runs "
                "this command, or remove `release.kind: distribution`"
            ),
        )
    previous = read_installed_release_receipt(deployment, deployment_dir)
    _refuse_downgrade(deployment, previous, identity)
    marker = read_upgrade_marker(deployment)
    report: dict[str, Any] = {
        "schema": UPGRADE_SCHEMA,
        "deployment": deployment,
        "route": dict(route),
        "mode": "plan" if options.plan_only else "apply",
        "from": _identity_summary(previous) if previous else None,
        "to": _identity_summary(identity),
        "started_at": _now(),
        "resumed": marker is not None,
        "steps": [],
    }
    if previous is not None and marker is None and _same_install(previous, identity):
        dsn = _resolve_dsn(options)
        pending = _pending_migrations(dsn)
        if not pending:
            report.update({
                "ok": True,
                "status": "up_to_date",
                "text": (
                    f"{deployment} already runs okg {identity['okg_version']}; "
                    "nothing to upgrade. Upgrade the okg package first, then "
                    "run okg up again."
                ),
            })
            return report
        report["receipt_matches_but_pending"] = pending

    compat = _step_compat(options, deployment, deployment_dir, previous, identity)
    report["steps"].append(compat)
    report["compat_override"] = compat.get("override")
    report["contract_override"] = compat.get("contract_override")
    report["worker_service"] = _refuse_unsupervised_worker(
        deployment, deployment_dir, lambda: _resolve_dsn(options), marker=marker,
    )
    if options.plan_only:
        skipped = None
        try:
            plan_dsn: str | None = _resolve_dsn(options)
        except SubstrateError as exc:
            # Presence and pairing are still checked; the report says why
            # the version was not.
            plan_dsn, skipped = None, f"the database is unresolved: {exc.message}"
        tools = _client_tools_check(plan_dsn, version_skipped=skipped)
        report["client_tools"] = tools
        if not tools["ok"]:
            report.update({
                "ok": False,
                "status": "blocked",
                "planned_steps": list(STEPS[1:]),
                "text": (
                    f"plan: okg up {deployment} would stop at its backup "
                    f"step: {tools['reason']}; no change made"
                ),
                "next_action": (
                    f"{tools['fix_hint']}, then run `okg up {deployment} "
                    "--plan` again"
                ),
            })
            return report
        report.update({
            "ok": True,
            "status": "planned",
            "planned_steps": list(STEPS[1:]),
            "text": (
                f"plan: upgrade {deployment} from okg "
                f"{_version(previous)} to {identity['okg_version']}; no change made"
                + ("; an unfinished upgrade will be resumed" if marker else "")
            ),
        })
        return report

    dsn = _resolve_dsn(options)
    env = _child_env(deployment, dsn)
    state: dict[str, Any] = {
        "marker": marker,
        "stop": {},
        "backup": {},
        "preflight": {},
        "dsn": dsn,
    }

    def preflight() -> dict[str, Any]:
        row = _step_preflight(options, deployment, dsn, runner, env, marker)
        state["preflight"] = row
        return row

    def stop_worker() -> dict[str, Any]:
        current = state["marker"] or {
            "started_at": report["started_at"],
            "target_version": identity["okg_version"],
            "worker_stopped_by_upgrade": False,
            "pre_upgrade_archive": None,
            "database": state["preflight"].get("database"),
            "graph_before": state["preflight"].get("graph_before_full"),
            "readiness_before": state["preflight"].get("readiness_before"),
        }
        current["last_run_target_version"] = identity["okg_version"]
        _write_marker(deployment, current)
        state["marker"] = current
        row = _step_stop_worker(deployment, runner, env)
        if row.get("stopped"):
            current["worker_stopped_by_upgrade"] = True
            _write_marker(deployment, current)
        state["stop"] = row
        return row

    def backup() -> dict[str, Any]:
        row = _step_backup(
            options, deployment, deployment_dir, dsn, previous, identity,
            resumed=marker is not None,
        )
        state["backup"] = row
        current = state["marker"]
        if row.get("ok") and current is not None and not current.get("pre_upgrade_archive"):
            current["pre_upgrade_archive"] = row.get("archive")
            _write_marker(deployment, current)
        return row

    def expected_database() -> Mapping[str, Any] | None:
        return state["preflight"].get("database")

    sequence: list[tuple[str, Callable[[], dict[str, Any]]]] = [
        ("preflight", preflight),
        ("stop_worker", stop_worker),
        ("backup", backup),
        ("migrate", lambda: _step_migrate(
            deployment, dsn, runner, env, expected_database(),
        )),
        ("deployment_apply", lambda: _step_deployment_apply(
            deployment, deployment_dir, dsn, runner, env, expected_database(),
        )),
        ("restart_worker", lambda: _step_restart_worker(
            deployment, runner, env,
            must_restart=bool((state["marker"] or {}).get("worker_stopped_by_upgrade")),
        )),
        ("readiness", lambda: _step_readiness(
            options, deployment, dsn, runner, env, expected_database(),
            state["preflight"].get("graph_before_full") or {},
            state["preflight"].get("readiness_before") or {},
        )),
    ]
    for name, step in sequence:
        row = _run_step(name, step)
        report["steps"].append(_public_row(row))
        if name == "preflight" and row.get("ok"):
            report["graph_before"] = row.get("graph_before")
            report["readiness_before"] = row.get("readiness_before")
            report["database"] = row.get("database")
        if not row.get("ok"):
            return _failed(report, deployment, row, state, previous)

    current_marker = state["marker"] or {}
    pre_upgrade_archive = current_marker.get("pre_upgrade_archive") or state["backup"].get("archive")
    upgrade_record = {
        "started_at": current_marker.get("started_at") or report["started_at"],
        "finished_at": _now(),
        "resumed": report["resumed"],
        "backup_archive": state["backup"].get("archive"),
        "pre_upgrade_archive": pre_upgrade_archive,
        "compat_contract_source": compat.get("contract_source"),
        "compat_override": compat.get("override"),
        "contract_override": compat.get("contract_override"),
        "database": state["preflight"].get("database"),
        "graph_before": report.get("graph_before"),
        "graph_after": report["steps"][-1].get("graph_after"),
    }

    def receipt() -> dict[str, Any]:
        path = write_installed_release_receipt(
            deployment,
            deployment_dir,
            written_by="upgrade",
            identity=identity,
            previous=_previous_summary(previous, compat),
            upgrade=upgrade_record,
        )
        marker_path(deployment).unlink(missing_ok=True)
        return {"step": "receipt", "ok": True, "path": str(path)}

    row = _run_step("receipt", receipt)
    report["steps"].append(row)
    if not row.get("ok"):
        return _failed(report, deployment, row, state, previous)
    report.update({
        "ok": True,
        "status": "upgraded",
        "finished_at": upgrade_record["finished_at"],
        "backup_archive": state["backup"].get("archive"),
        "pre_upgrade_archive": pre_upgrade_archive,
        "graph_after": upgrade_record["graph_after"],
        "text": (
            f"upgraded {deployment} from okg {_version(previous)} to "
            f"{identity['okg_version']}; backup taken before the upgrade: "
            f"{pre_upgrade_archive}"
        ),
    })
    return report


def _failed(
    report: dict[str, Any],
    deployment: str,
    failed: Mapping[str, Any],
    state: Mapping[str, Any],
    previous: Mapping[str, Any] | None,
) -> dict[str, Any]:
    marker = state.get("marker") or {}
    pre_upgrade_archive = marker.get("pre_upgrade_archive") or (
        state["backup"].get("archive") if state["backup"].get("ok") else None
    )
    worker_stopped = bool(marker.get("worker_stopped_by_upgrade")) and not any(
        row.get("step") == "restart_worker" and row.get("ok")
        for row in report["steps"]
    )
    report.update({
        "ok": False,
        "status": "failed",
        "failed_step": failed.get("step"),
        "finished_at": _now(),
        "backup_archive": state["backup"].get("archive"),
        "pre_upgrade_archive": pre_upgrade_archive,
        "worker_stopped": worker_stopped,
        "next_action": _next_action_after_failure(
            deployment, failed, pre_upgrade_archive, previous,
            worker_stopped=worker_stopped,
            resumed=bool(report.get("resumed")),
            deployment_dir=Path(str(report["route"]["deployment_dir"])),
            resolve_dsn=lambda: str(state["dsn"]),
        ),
    })
    return report


def _run_step(name: str, step: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    """Run one step; an exception becomes a failed row, never a lost report."""
    try:
        row = dict(step())
    except Exception as exc:  # noqa: BLE001 - reported as the failed step.
        row = {
            "step": name,
            "ok": False,
            "reason": f"the {name} step raised {type(exc).__name__}",
            "error": _error_summary(exc),
        }
    row.setdefault("step", name)
    return row


def _public_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in row.items() if not key.endswith("_full")}


def _refuse_inapplicable_options(options: Any, route: Mapping[str, Any]) -> None:
    """Flags that only mean something to the Git engine are refused, not ignored."""
    checks = {
        "--target dev": options.profile != "prod",
        "--dsn": options.dsn is not None,
        "--include": bool(options.include),
        "--skip-backup": bool(options.skip_backup),
        "--skip-migrate": bool(options.skip_migrate),
        "--backup-keep": options.backup_keep != 1,
        "--state-file": options.state_file is not None,
        "--no-resume": not options.resume,
        "--reset-state": bool(options.reset_state),
        "--verified-backup-state": options.verified_backup_state is not None,
        "--verified-backup-source-revision": (
            options.verified_backup_source_revision is not None
        ),
        "--lifecycle-plan": options.lifecycle_plan is not None,
        "--precheckout-target-revision": (
            options.precheckout_target_revision is not None
        ),
        "--confirm-safety-plan": options.confirm_safety_plan is not None,
        "--stop-after": options.stop_after is not None,
        "--runtime-fence-owner": options.runtime_fence_owner is not None,
        "--no-apply-services": not options.plan_only and not options.apply_services,
        "--no-sync-code": not options.plan_only and not options.sync_code,
        "--no-sync-deps": not options.plan_only and not options.sync_deps,
        "--confirm-destructive-migrations": bool(
            options.confirm_destructive_migrations
        ),
        "--skip-destructive-migrations": bool(options.skip_destructive_migrations),
    }
    used = sorted(flag for flag, present in checks.items() if present)
    if used:
        raise InstalledUpgradeRefused(
            "these okg up options do not apply to an installed deployment",
            data={
                "deployment": options.deployment,
                "release_kind": route.get("release_kind"),
                "options": used,
            },
            fix_hint=(
                "an installed deployment is upgraded by upgrading the okg "
                "package and running `okg up <deployment>`; its database is "
                "the one its deployment.yaml names; drop "
                + ", ".join(used)
            ),
        )


def _refuse_tracked_backup_dir(options: Any) -> None:
    """A --backup-dir inside a Git work tree must be ignored there.

    A database dump holds every row of the graph; one that Git does not
    ignore is one `git add .` away from a commit.
    """
    if options.backup_dir is None:
        return
    target = Path(options.backup_dir).expanduser().resolve()
    probe = target
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent
    try:
        top = subprocess.run(
            ["git", "-C", str(probe), "rev-parse", "--show-toplevel"],
            capture_output=True, text=True, check=False, timeout=30,
        )
    except FileNotFoundError:
        # No Git on this machine: nothing can commit the archive from here.
        return
    if top.returncode != 0:
        return
    ignored = subprocess.run(
        ["git", "-C", top.stdout.strip(), "check-ignore", "-q", str(target)],
        capture_output=True, text=True, check=False, timeout=30,
    )
    if ignored.returncode == 0:
        return
    raise InstalledUpgradeRefused(
        "--backup-dir is inside a Git work tree and not ignored there",
        data={
            "backup_dir": str(target),
            "work_tree": top.stdout.strip(),
            "check_ignore_returncode": ignored.returncode,
        },
        fix_hint=(
            "drop --backup-dir to use the deployment's private state "
            "directory, pick a directory outside any Git work tree, or add it "
            "to that work tree's .gitignore"
        ),
    )


def _refuse_downgrade(
    deployment: str,
    previous: Mapping[str, Any] | None,
    identity: Mapping[str, Any],
) -> None:
    if previous is None:
        return
    from packaging.version import InvalidVersion, Version

    installed, recorded = identity["okg_version"], str(previous.get("okg_version"))
    try:
        older = Version(installed) < Version(recorded)
    except InvalidVersion as exc:
        raise InstalledUpgradeRefused(
            "cannot compare the installed okg version with the receipt's",
            data={"deployment": deployment, "installed": installed, "receipt": recorded},
            fix_hint="install a released okg wheel, or inspect the receipt",
        ) from exc
    if not older:
        return
    archive = (previous.get("upgrade") or {}).get("pre_upgrade_archive")
    restore = (
        f"`okg deployment backup restore {deployment} --archive {archive}` "
        "(it shows a plan; add --apply)"
        if archive else
        f"`okg deployment backup restore {deployment} --archive <archive>` "
        "with the backup taken before that upgrade"
    )
    raise InstalledUpgradeRefused(
        f"okg {installed} is older than okg {recorded}, which {deployment} "
        "already runs; okg up does not downgrade",
        data={"deployment": deployment, "installed": installed, "receipt": recorded},
        fix_hint=(
            f"reinstall okg {recorded}; or, to go back to the database as it "
            f"was before the upgrade, run {restore} with okg {recorded}, then "
            f"reinstall okg {installed}"
        ),
    )


def _same_install(previous: Mapping[str, Any], identity: Mapping[str, Any]) -> bool:
    return (
        previous.get("okg_version") == identity["okg_version"]
        and previous.get("payload_digest") == identity["payload_digest"]
        and previous.get("schema_checksum") == identity["schema_checksum"]
        and previous.get("contract_digest") == identity["contract_digest"]
    )


def _identity_summary(identity: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "okg_version": identity.get("okg_version"),
        "payload_digest": identity.get("payload_digest"),
        "schema_checksum": identity.get("schema_checksum"),
        "contract_digest": identity.get("contract_digest"),
    }


def _previous_summary(
    previous: Mapping[str, Any] | None, compat: Mapping[str, Any],
) -> dict[str, Any] | None:
    """What this upgrade came from: the receipt, else the contract it used."""
    if previous is not None:
        return {**_identity_summary(previous), "source": "receipt"}
    if compat.get("previous_contract_digest") is None:
        return None
    return {
        "okg_version": compat.get("from_version"),
        "payload_digest": None,
        "schema_checksum": None,
        "contract_digest": compat.get("previous_contract_digest"),
        "source": compat.get("contract_source"),
    }


def _version(previous: Mapping[str, Any] | None) -> str:
    if previous is None:
        return "(unrecorded version)"
    return str(previous.get("okg_version"))


# ------------------------------------------------------------------- compat


def _step_compat(
    options: Any,
    deployment: str,
    deployment_dir: Path,
    previous: Mapping[str, Any] | None,
    identity: Mapping[str, Any],
) -> dict[str, Any]:
    from okg.substrate.deployment_compat import check_deployment_compat

    contract: Mapping[str, Any] | None = None
    source: str | None = None
    contract_override: dict[str, Any] | None = None
    if options.from_contract is not None:
        source = f"--from-contract {options.from_contract}"
        try:
            contract = json.loads(Path(options.from_contract).read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise InstalledUpgradeRefused(
                "--from-contract file is unreadable",
                data={"path": str(options.from_contract), "reason": str(exc)},
                fix_hint="pass the previous release's contract JSON file",
            ) from exc
        if not isinstance(contract, Mapping) or not isinstance(
            contract.get("sections"), Mapping
        ):
            raise InstalledUpgradeRefused(
                "--from-contract file is not a release contract",
                data={"path": str(options.from_contract)},
                fix_hint="pass the previous release's contract JSON file",
            )
        if _contract_digest(contract) == identity["contract_digest"]:
            raise InstalledUpgradeRefused(
                "--from-contract names the contract of the okg now installed; "
                "there is nothing to compare",
                data={"path": str(options.from_contract)},
                fix_hint=(
                    "pass the contract of the okg version you upgraded FROM; "
                    "if the two releases really share one contract, rerun "
                    "without --from-contract and with --accept-unknown-compat"
                ),
            )
        if previous is not None:
            contract_override = {
                "flag": "--from-contract",
                "path": str(options.from_contract),
                "overrode_receipt_version": previous.get("okg_version"),
                "overrode_receipt_contract_digest": previous.get("contract_digest"),
            }
            print(
                f"WARNING: --from-contract {options.from_contract} overrides "
                f"the receipt's contract (okg {previous.get('okg_version')}) "
                f"for {deployment}. This is recorded in the upgrade receipt.",
                file=sys.stderr,
            )
    elif previous is not None:
        contract = previous["contract"]
        source = f"receipt {receipt_path(deployment)}"

    if contract is not None:
        with tempfile.TemporaryDirectory(prefix="okg-installed-upgrade-") as tmp:
            against = Path(tmp) / "previous-contract.json"
            against.write_text(json.dumps(contract), encoding="utf-8")
            compat = check_deployment_compat(deployment_dir, against=against)
    else:
        compat = check_deployment_compat(deployment_dir)

    since = compat.get("breaking_since") or {}
    affecting = list(since.get("affecting_this_deployment") or [])
    blockers = [
        row for row in compat.get("findings") or ()
        if row.get("severity") == "blocker"
    ]
    unknowns = [
        row for row in compat.get("findings") or ()
        if row.get("severity") == "unknown"
    ]
    from_version = (
        since.get("from_version")
        or (contract or {}).get("okg_version")
        or _version(previous)
    )
    row: dict[str, Any] = {
        "step": "compat",
        "contract_source": source,
        "contract_override": contract_override,
        "verdict": compat.get("verdict"),
        "exit_code": compat.get("exit_code"),
        "from_version": from_version,
        "to_version": identity["okg_version"],
        "previous_contract_digest": (
            _contract_digest(contract) if contract is not None else None
        ),
        "breaking_affecting_this_deployment": affecting,
        "blockers": blockers,
        "unknowns": unknowns,
    }
    go_back = _go_back_hint(from_version)
    notes = _release_notes_link(identity["okg_version"])
    if blockers or affecting:
        lines = [
            f"  BLOCKER [{b.get('code')}]: {b.get('message')}" for b in blockers
        ] + [
            f"  BREAKING {r.get('section')}: {r.get('item')} ({r.get('change')})"
            for r in affecting
        ]
        raise InstalledUpgradeIncompatible(
            f"{deployment} does not work with okg {identity['okg_version']}; "
            "nothing was changed\n" + "\n".join(lines),
            data={
                **row,
                "release_notes": notes,
                "go_back": go_back,
            },
            fix_hint=(
                "fix the deployment for the rows above"
                + (f" (release notes: {notes})" if notes else "")
                + f", or go back: {go_back}"
            ),
        )
    if unknowns or contract is None:
        reasons = [str(u.get("message")) for u in unknowns]
        if contract is None:
            reasons.append(
                "no previous release contract: this deployment has no "
                f"installed-release receipt ({receipt_path(deployment)}) "
                "and no --from-contract was given"
            )
        if not options.accept_unknown_compat:
            raise InstalledUpgradeCompatUnknown(
                f"cannot tell whether {deployment} works with okg "
                f"{identity['okg_version']}; nothing was changed\n"
                + "\n".join(f"  UNKNOWN: {r}" for r in reasons),
                data={**row, "reasons": reasons, "go_back": go_back},
                fix_hint=(
                    "pass the previous release's contract with "
                    "--from-contract FILE (see docs/operate/lifecycle.md), "
                    "or rerun with --accept-unknown-compat to upgrade without "
                    f"knowing; to stay put: {go_back}"
                ),
            )
        row["override"] = {
            "flag": "--accept-unknown-compat",
            "reasons": reasons,
        }
        print(
            "WARNING: --accept-unknown-compat: upgrading "
            f"{deployment} although its compatibility with okg "
            f"{identity['okg_version']} is unknown: " + "; ".join(reasons)
            + ". This is recorded in the upgrade receipt.",
            file=sys.stderr,
        )
    row["ok"] = True
    return row


def _go_back_hint(from_version: str | None) -> str:
    if not from_version or from_version.startswith("("):
        return (
            "reinstall the okg version you upgraded from, the way you "
            "installed it (`uv tool install --force "
            "okg-<version>-py3-none-any.whl` or `pip install "
            "okg-<version>-py3-none-any.whl`; wheels are attached to each "
            f"release at {RELEASES_URL})"
        )
    wheel = f"okg-{from_version}-py3-none-any.whl"
    return (
        f"reinstall okg {from_version} the way you installed it (`uv tool "
        f"install --force {wheel}` or `pip install {wheel}`; the wheel is "
        f"attached to its release at {RELEASES_URL})"
    )


def _release_notes_link(version: str) -> str | None:
    if re.fullmatch(r"0\.\d+\.\d+", version):
        return f"{RELEASES_URL}/tag/v{version}"
    return None


# ---------------------------------------------------------------- preflight


def _step_preflight(
    options: Any,
    deployment: str,
    dsn: str,
    runner: CommandRunner,
    env: dict[str, str],
    marker: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Which database this run works on, and what readers see before it.

    On a resumed run the graph and readiness recorded by the run that
    started the upgrade are kept: the database may already be half
    upgraded, so a fresh "before" would hide what the upgrade changed.
    """
    database = _database_identity(dsn)
    row: dict[str, Any] = {"step": "preflight", "database": database}
    tools = _client_tools_check(dsn)
    row["client_tools"] = {"ok": tools["ok"], "server_major": tools["server_major"]}
    if not tools["ok"]:
        return {
            **row,
            "ok": False,
            "reason": tools["reason"],
            "fix_hint": tools["fix_hint"],
            "client_tools": tools,
        }
    rights = restore_rights(dsn, str(database["database"]))
    row["restore_rights"] = {key: rights[key] for key in ("actor", "superuser", "timescaledb")}
    if rights["problems"]:
        return {
            **row,
            "ok": False,
            "reason": (
                "the backup check restores the archive into a disposable "
                "database, and this role cannot: " + "; ".join(rights["problems"])
                + ". Run okg up as a superuser, or give this role those rights"
            ),
        }
    if marker is not None:
        recorded = marker.get("database")
        if recorded and recorded != database:
            return {
                **row,
                "ok": False,
                "reason": (
                    "the unfinished upgrade recorded a different database "
                    f"({recorded}); this run resolved {database}"
                ),
                "marker": str(marker_path(deployment)),
            }
        if marker.get("graph_before") is not None and marker.get("readiness_before") is not None:
            graph_before = dict(marker["graph_before"])
            readiness_before = dict(marker["readiness_before"])
            row["before_from"] = "unfinished upgrade marker"
        else:
            graph_before = _graph_snapshot(dsn, deployment)
            readiness_before = _readiness_snapshot(options, deployment, runner, env)
    else:
        graph_before = _graph_snapshot(dsn, deployment)
        readiness_before = _readiness_snapshot(options, deployment, runner, env)
    row.update({
        "ok": True,
        "graph_before": _public_graph(graph_before),
        "graph_before_full": graph_before,
        "readiness_before": readiness_before,
    })
    return row


def _client_tools_check(
    dsn: str | None,
    tools: tuple[str, ...] = ("pg_dump", "pg_restore"),
    *,
    version_skipped: str | None = None,
) -> dict[str, Any]:
    """pg_dump and pg_restore: on PATH and at least the server's version.

    The backup step needs both (issue #3587); a check here stops the run
    before the worker is stopped instead of at the backup. Without a server
    version (``dsn`` None, or the server does not answer) only presence and
    the pg_dump/pg_restore pairing are checked, and ``version_checked`` says
    so.
    """
    from okg.substrate.postgres_clone import (
        CLIENT_TOOLS_FIX_HINT,
        client_tool_report,
        describe_client_tool_problems,
        server_major,
    )

    major = server_major(dsn) if dsn else None
    report = client_tool_report(major, tools)
    problems = report["problems"]
    check: dict[str, Any] = {
        "ok": not problems,
        "server_major": major,
        "versions": report["versions"],
        "version_checked": major is not None,
        "problems": problems,
    }
    if major is None:
        check["version_skipped"] = version_skipped or (
            "the database server did not answer"
        )
    if report["unknown_version"]:
        check["unknown_version"] = report["unknown_version"]
    if problems:
        check["reason"] = describe_client_tool_problems(problems)
        check["fix_hint"] = CLIENT_TOOLS_FIX_HINT
    return check


def _database_identity(dsn: str) -> dict[str, Any]:
    from okg.substrate.db.connection import connect as db_connect

    with db_connect(dsn, autocommit=True, connect_timeout=10) as conn:
        row = conn.execute(
            "SELECT current_database(), inet_server_port(), "
            "(SELECT system_identifier::text FROM pg_control_system())"
        ).fetchone()
    return {
        "database": str(row[0]),
        "port": int(row[1]) if row[1] is not None else None,
        "system_identifier": str(row[2]),
    }


def _identity_problem(dsn: str, expected: Mapping[str, Any] | None) -> str | None:
    if expected is None:
        return "the database identity was not recorded"
    observed = _database_identity(dsn)
    if observed != dict(expected):
        return (
            f"the deployment database changed during the upgrade: was "
            f"{dict(expected)}, now {observed}"
        )
    return None


# ------------------------------------------------------------------- worker


SYSTEMD_TEMPLATE = Path("/etc/systemd/system/okg-runtime-worker@.service")


def worker_service(
    deployment: str, *, deployment_dir: Path, resolve_dsn: Callable[[], str],
) -> dict[str, Any]:
    """The worker service `okg up` would stop, reload and restart, and its state.

    ``available`` is True only when this deployment's own service is in
    place:

    * Linux (systemd, system scope, what ``okg dogfood up`` targets): the
      instance ``okg-runtime-worker@<deployment>.service`` is enabled AND the
      shared template ``/etc/systemd/system/okg-runtime-worker@.service`` is
      exactly what ``okg dogfood service render`` produces for this
      deployment. Any instance of an installed template reports
      LoadState=loaded, so LoadState says nothing about this deployment.
      ``template_writable`` says whether this process could rewrite the
      template, which the converge step does on every worker reload.
    * macOS (launchd): the plist ``~/Library/LaunchAgents/
      org.okg.runtime-worker.<deployment>.plist`` exists. A user can write
      it, so ``self_installable`` is True: the converge step installs it.
    * Anywhere else ``dogfood up`` would run ``uv run okg``, which is not
      the installed okg, so there is no service.
    """
    import platform
    import shutil
    import subprocess

    from okg.substrate.deployment_apply import deployment_uses_process_worker

    if deployment_uses_process_worker(deployment_dir):
        # A plain install: the operator starts `okg runtime worker`, and no
        # service definition exists for okg to stop, rewrite or restart.
        return {
            "adapter": "process", "available": False,
            "self_installable": False, "template_writable": False,
            "reason": (
                "this deployment's worker is started by hand with `okg "
                "runtime worker`; its folder is outside any Git checkout "
                "and declares no runtime.supervisor"
            ),
        }
    system = platform.system().lower()
    if system == "linux":
        unit = f"okg-runtime-worker@{deployment}.service"
        row: dict[str, Any] = {
            "adapter": "systemd", "unit": unit, "available": False,
            "self_installable": False, "template": str(SYSTEMD_TEMPLATE),
            "template_writable": _path_writable(SYSTEMD_TEMPLATE),
        }
        if shutil.which("systemctl") is None:
            return {**row, "reason": "systemctl is not installed"}
        probe = subprocess.run(
            ["systemctl", "show", "-p", "UnitFileState", "--value", unit],
            capture_output=True, text=True, check=False, timeout=30,
        )
        state = probe.stdout.strip()
        row["unit_file_state"] = state or None
        if probe.returncode != 0 or state != "enabled":
            return {**row, "reason": f"systemd unit {unit} is not enabled ({state or 'unknown'})"}
        matches, detail = _systemd_template_matches(deployment, deployment_dir, resolve_dsn())
        row["template_matches"] = matches
        if not matches:
            return {**row, "reason": detail}
        return {**row, "available": True, "reason": None}
    if system == "darwin":
        plist = (
            Path.home() / "Library" / "LaunchAgents"
            / f"org.okg.runtime-worker.{deployment}.plist"
        )
        return {
            "adapter": "launchd",
            "plist": str(plist),
            "available": plist.is_file(),
            "self_installable": True,
            "template_writable": True,
            "reason": None if plist.is_file() else f"launchd plist {plist} does not exist",
        }
    return {"adapter": "foreground", "available": False, "self_installable": False,
            "template_writable": False,
            "reason": f"no supervised worker service on {system or 'this system'}"}


def _path_writable(path: Path) -> bool:
    """Whether this process could create or replace ``path``."""
    if path.exists():
        return os.access(path, os.W_OK) and os.access(path.parent, os.W_OK)
    return os.access(path.parent, os.W_OK)


def _systemd_template_matches(
    deployment: str, deployment_dir: Path, dsn: str,
) -> tuple[bool, str]:
    """Whether the installed systemd template is this deployment's own.

    Renders the template the converge step would install, with the same
    arguments it passes (``deployment_apply`` builds them from the desired
    state's service authority), and compares it byte for byte with the
    installed file.
    """
    import subprocess

    from okg.substrate.deployment_apply import desired_deployment_state

    authority = desired_deployment_state(
        deployment=deployment, deployment_dir=deployment_dir, dsn=dsn,
    )["service_authority"]
    result = subprocess.run(
        [
            *_okg_argv(), "dogfood", "service", "render",
            "--deployment", deployment,
            "--adapter", str(authority["adapter"]),
            "--repo-root", str(authority["working_directory"]),
            "--dsn", "${OKG_DEPLOYMENT_APPLY_DSN}",
            "--json",
        ],
        capture_output=True, text=True, check=False, timeout=120,
    )
    try:
        rendered = json.loads(result.stdout)
    except ValueError:
        rendered = None
    if result.returncode != 0 or not isinstance(rendered, Mapping) or "content" not in rendered:
        return False, (
            "could not render this deployment's worker service template "
            f"(rc {result.returncode}): {_redacted_tail(result.stderr)[-300:]}"
        )
    destination = Path(str(rendered.get("destination") or SYSTEMD_TEMPLATE))
    try:
        installed = destination.read_text(encoding="utf-8")
    except OSError as exc:
        return False, f"cannot read the installed template {destination}: {exc}"
    if installed != str(rendered["content"]):
        return False, (
            f"the installed template {destination} is not the one okg renders "
            f"for {deployment} (it was written for another deployment or "
            "another okg)"
        )
    return True, "matches"


SERVICE_INSTALL_COMMAND = (
    "okg dogfood service install --deployment {deployment} --adapter {adapter} --apply"
)


def _runtime_enabled(deployment_dir: Path) -> bool:
    """``runtime.enabled`` from the route's deployment.yaml, read as apply reads it."""
    import yaml

    try:
        raw = yaml.safe_load((Path(deployment_dir) / "deployment.yaml").read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError):
        return False
    runtime = raw.get("runtime") if isinstance(raw, Mapping) else None
    return isinstance(runtime, Mapping) and bool(runtime.get("enabled", False))


def _worker_reload_needed(
    deployment: str, deployment_dir: Path, dsn: str,
) -> dict[str, Any]:
    """Whether the upgrade's converge step would reload the worker.

    Asks the same read-only health check the converge step uses
    (``deployment_apply_health``); this code does not judge the worker ack
    itself. ``unknown`` is True when that check could not run.
    """
    from okg.substrate.deployment_apply import deployment_apply_health

    health = deployment_apply_health(
        deployment=deployment, deployment_dir=deployment_dir, dsn=dsn,
    )
    reload = next(
        (row for row in health.get("actions") or () if row.get("stage") == "worker_reload"),
        None,
    )
    return {
        "health_status": health.get("status"),
        "unknown": health.get("status") == "unavailable",
        "error": health.get("reason") if health.get("status") == "unavailable" else None,
        "needed": bool(reload and reload.get("needed"))
        and "stage_needed:worker_reload" in (health.get("problems") or ()),
        "reason": reload.get("reason") if reload else None,
        "worker_ack": health.get("worker_ack") is not None,
    }


def _refuse_unsupervised_worker(
    deployment: str,
    deployment_dir: Path,
    resolve_dsn: Callable[[], str],
    *,
    marker: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Refuse before anything changes when okg cannot manage the worker.

    `okg up` stops a running worker and starts it again, and its converge
    step reloads the worker by rewriting the worker service definition, all
    through the deployment's worker service. It refuses here, after
    compatibility and before preflight, stop, backup or marker, with
    ``installed_upgrade_worker_unsupervised``, when:

    * ``worker_service_needs_root``: a systemd system service is in place,
      the converge step would reload the worker, and this process cannot
      rewrite ``/etc/systemd/system/okg-runtime-worker@.service`` (run
      `okg up` as root);
    * ``worker_reload_unknown``: whether the converge step would reload the
      worker could not be read (the apply health check is unavailable);
    * with no service of its own for this deployment:
      ``runtime_enabled_without_service`` (``runtime.enabled: true``),
      ``worker_running_without_service`` (a worker is running, whose ack the
      next `okg up` would still find), or ``stale_worker_ack`` (a worker has
      run before and the converge step would reload it).

    On macOS the converge step installs the launchd plist itself (no root),
    so only a running worker refuses there. Per-user systemd services
    (planned as the first follow-up after 2026-10-17) would remove the Linux refusals for hosts
    without root.
    """
    audit = _worker_audit(deployment)
    running = audit.get("status") != "unavailable" and bool(audit.get("root_count"))
    runtime = _runtime_enabled(deployment_dir)
    dsn_cache: list[str] = []

    def dsn() -> str:
        if not dsn_cache:
            dsn_cache.append(resolve_dsn())
        return dsn_cache[0]

    service = worker_service(deployment, deployment_dir=deployment_dir, resolve_dsn=dsn)
    reload: dict[str, Any] | None = None

    def reload_state() -> dict[str, Any]:
        nonlocal reload
        if reload is None:
            reload = _worker_reload_needed(deployment, deployment_dir, dsn())
        return reload

    reason: str | None = None
    if service.get("available"):
        if service.get("adapter") == "systemd" and not service.get("template_writable"):
            state = reload_state()
            if state["unknown"]:
                reason = "worker_reload_unknown"
            elif state["needed"]:
                reason = "worker_service_needs_root"
    elif runtime and not service.get("self_installable"):
        reason = "runtime_enabled_without_service"
    elif running:
        reason = "worker_running_without_service"
    elif not service.get("self_installable"):
        state = reload_state()
        if state["unknown"]:
            reason = "worker_reload_unknown"
        elif state["needed"]:
            reason = "stale_worker_ack"
    if reason is None:
        return {
            "worker_running": running, "runtime_enabled": runtime,
            "worker_reload": reload, **service,
        }

    adapter = str(service.get("adapter") or "systemd")
    as_root = adapter == "systemd"
    install = SERVICE_INSTALL_COMMAND.format(deployment=deployment, adapter=adapter)
    install_how = f"`{install}`" + (" as root" if as_root else "")
    pids = [int(row["pid"]) for row in audit.get("roots") or () if row.get("pid")] if running else []
    commands = [row.get("command") for row in audit.get("roots") or ()] if running else []
    follow_up = (
        "per-user systemd worker services, planned as the first item "
        "after 2026-10-17, will let a host without root do this"
    )
    if marker is not None:
        changed = (
            "This run changed nothing, but an earlier `okg up` started on "
            f"{marker.get('started_at')} did not finish and has already "
            "changed the deployment (its state is in "
            f"{marker_path(deployment)}; worker stopped by it: "
            f"{bool(marker.get('worker_stopped_by_upgrade'))})"
        )
    else:
        changed = "Nothing was changed"
    data: dict[str, Any] = {
        "deployment": deployment,
        "reason": reason,
        "runtime_enabled": runtime,
        "pids": pids,
        "commands": commands,
        "service": service,
        "worker_reload": reload,
        "upgrade_command": f"okg up {deployment}",
        "install_service_command": install,
        "follow_up": "per-user systemd worker services, first item after 2026-10-17",
        "resumed_upgrade": marker is not None,
    }
    if reason == "worker_service_needs_root":
        message = (
            f"the converge step would reload {deployment}'s worker by rewriting "
            f"{service.get('template')} and restarting its systemd system service, "
            f"which this process cannot do. {changed}"
        )
        hint = (
            f"run `okg up {deployment}` as root (for example with sudo) for a "
            f"deployment with a systemd system worker service; {follow_up}"
        )
    elif reason == "worker_reload_unknown":
        message = (
            f"cannot tell whether the upgrade would have to reload {deployment}'s "
            f"worker: the apply health check is unavailable "
            f"({(reload or {}).get('error')}). {changed}"
        )
        hint = (
            f"fix what `okg deployment ready {deployment}` reports for the apply "
            f"check, then run `okg up {deployment}` again"
        )
    elif reason == "runtime_enabled_without_service":
        message = (
            f"{deployment} has runtime.enabled: true, and it has no worker "
            f"service of its own ({service.get('reason')}); the upgrade would "
            f"fail installing one after migrating the database. {changed}"
        )
        hint = f"install the worker service first ({install_how}), then run `okg up {deployment}`; {follow_up}"
    elif reason == "worker_running_without_service":
        message = (
            f"a runtime worker for {deployment} is running (pid "
            f"{', '.join(str(pid) for pid in pids)}), and it has no worker "
            f"service of its own ({service.get('reason')}). Stopping it would "
            "not help: it leaves its record behind, and the upgrade would "
            f"still have to reload it through a service. {changed}"
        )
        hint = f"install the worker service first ({install_how}), then run `okg up {deployment}`; {follow_up}"
    else:
        message = (
            f"{deployment} has run a runtime worker before, and the upgrade "
            "would have to reload it through a worker service of its own, "
            f"which it does not have ({service.get('reason')}). A deployment "
            "that has run a worker needs that service installed"
            + (", as root," if as_root else "")
            + f" before `okg up`. {changed}"
        )
        hint = f"install the worker service first ({install_how}), then run `okg up {deployment}`; {follow_up}"
    if adapter == "process":
        # No `dogfood service install` adapter fits a hand-started worker.
        data["install_service_command"] = None
        hint = (
            f"`okg up` cannot yet upgrade {deployment} while its runtime worker "
            f"is started by hand; {follow_up}"
        )
    raise InstalledUpgradeWorkerUnsupervised(message, data=data, fix_hint=hint)


def _worker_audit(deployment: str) -> dict[str, Any]:
    from okg.substrate.cli.dogfood import _runtime_worker_process_audit

    return _runtime_worker_process_audit(deployment)


def _step_stop_worker(
    deployment: str, runner: CommandRunner, env: dict[str, str],
) -> dict[str, Any]:
    row: dict[str, Any] = {"step": "stop_worker"}
    audit = _worker_audit(deployment)
    if audit.get("status") == "unavailable":
        return {
            **row,
            "ok": False,
            "reason": "cannot tell whether a runtime worker is running",
            "audit": audit,
        }
    if not audit.get("root_count"):
        return {**row, "ok": True, "was_running": False, "stopped": False}
    result = runner(
        [*_okg_argv(), "dogfood", "down", "--deployment", deployment,
         "--apply", "--json"],
        dict(env),
    )
    after = _worker_audit(deployment)
    pids = [r.get("pid") for r in audit.get("roots") or ()]
    if after.get("status") == "unavailable" or after.get("root_count"):
        return {
            **row,
            "ok": False,
            "was_running": True,
            "reason": "the runtime worker is still running after `okg dogfood down`",
            "pids": pids,
            "returncode": getattr(result, "returncode", None),
            "stderr_tail": _redacted_tail(getattr(result, "stderr", "")),
        }
    return {**row, "ok": True, "was_running": True, "stopped": True, "pids": pids}


def _step_restart_worker(
    deployment: str,
    runner: CommandRunner,
    env: dict[str, str],
    *,
    must_restart: bool,
) -> dict[str, Any]:
    row: dict[str, Any] = {"step": "restart_worker"}
    if not must_restart:
        return {
            **row,
            "ok": True,
            "skipped": "no runtime worker was running before the upgrade",
        }
    if _worker_audit(deployment).get("root_count"):
        return {**row, "ok": True, "skipped": "deployment apply already restarted it"}
    result = runner(
        [*_okg_argv(), "dogfood", "up", "--deployment", deployment,
         "--apply", "--json"],
        dict(env),
    )
    deadline = time.monotonic() + WORKER_START_TIMEOUT_SECONDS
    while True:
        audit = _worker_audit(deployment)
        if audit.get("root_count"):
            return {**row, "ok": True, "pids": [
                r.get("pid") for r in audit.get("roots") or ()
            ]}
        if time.monotonic() >= deadline:
            return {
                **row,
                "ok": False,
                "reason": "the runtime worker did not start",
                "returncode": getattr(result, "returncode", None),
                "stderr_tail": _redacted_tail(getattr(result, "stderr", "")),
            }
        time.sleep(2.0)


# ------------------------------------------------------------------- backup


def _step_backup(
    options: Any,
    deployment: str,
    deployment_dir: Path,
    dsn: str,
    previous: Mapping[str, Any] | None,
    identity: Mapping[str, Any],
    *,
    resumed: bool = False,
) -> dict[str, Any]:
    """Dump, prove the dump restores to the same content, write its sidecar.

    The dump keeps owners and privileges. The proof restores it through the
    verified restore path, replays its owners and ACLs (that path restores
    with ``--no-owner --no-privileges``), and compares both the data
    fingerprint and the access fingerprint. The disposable database is
    dropped only when the proof passed; otherwise it is kept and named, so
    the operator can see what the archive actually holds.
    """
    from okg.substrate.db import dsn as dsn_mod
    from okg.substrate.postgres_clone import restore_database_dump

    directory = (
        Path(options.backup_dir).expanduser() if options.backup_dir
        else backups_dir(deployment)
    )
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    archive = directory / (
        f"{deployment}-{_slug(_version(previous))}-to-"
        f"{_slug(identity['okg_version'])}-{stamp}.dump"
    )
    verify_db = f"{VERIFY_DATABASE_PREFIX}{secrets.token_hex(4)}"
    verify_dsn = dsn_mod.parse(dsn).with_dbname(verify_db).render()
    row: dict[str, Any] = {"step": "backup", "verify_database": verify_db}
    try:
        _private_dir(directory)
        _create_backup_dump(dsn, archive)
    except Exception as exc:  # noqa: BLE001 - reported; the run stops here.
        return {
            **row,
            "ok": False,
            "reason": "pg_dump failed",
            "error": _error_summary(exc),
            "archive": str(archive) if archive.exists() else None,
            "verify_database_created": False,
        }
    row["archive"] = str(archive)
    row["archive_bytes"] = archive.stat().st_size
    row["archive_sha256"] = _file_sha256(archive)
    try:
        restore_database_dump(dsn, verify_dsn, archive)
        row["access_replay"] = replay_archive_access(verify_dsn, archive)
        source = _backup_fingerprint(dsn, deployment)
        restored = _backup_fingerprint(verify_dsn, deployment)
        source_access = access_fingerprint(dsn)
        restored_access = access_fingerprint(verify_dsn)
    except Exception as exc:  # noqa: BLE001 - reported; the run stops here.
        return {
            **row,
            "ok": False,
            "reason": (
                "the archive did not restore into the disposable database "
                f"{verify_db}; it is kept for inspection (drop it when done)"
            ),
            "error": _error_summary(exc),
            "verify_database_kept": True,
        }
    row.update({
        "source": _public_graph(source),
        "restored": _public_graph(restored),
        "access": {
            "source_digest": source_access["digest"],
            "restored_digest": restored_access["digest"],
            "rows": source_access["rows"],
        },
    })
    if source != restored or source_access["digest"] != restored_access["digest"]:
        return {
            **row,
            "ok": False,
            "reason": (
                "the restored copy differs from the database"
                + (
                    " in owners or privileges"
                    if source == restored else ""
                )
                + f"; the disposable database {verify_db} is kept for "
                "inspection (drop it when done)"
            ),
            "access_differences": access_differences(source_access, restored_access),
            "verify_database_kept": True,
        }
    dropped = _drop_database(dsn, verify_db)
    row["verify_database_dropped"] = dropped
    if dropped is not True:
        return {
            **row,
            "ok": False,
            "reason": f"could not drop the disposable database {verify_db}",
        }
    sidecar = {
        "schema": BACKUP_SCHEMA,
        "deployment": deployment,
        "deployment_dir": str(deployment_dir),
        "archive": archive.name,
        "archive_sha256": row["archive_sha256"],
        "archive_bytes": row["archive_bytes"],
        "created_at": _now(),
        "created_by_okg_version": identity["okg_version"],
        "from_version": previous.get("okg_version") if previous else None,
        "to_version": identity["okg_version"],
        # A backup taken by a resumed upgrade may already hold part of the
        # upgrade; the restore accepts only the archive recorded as the one
        # taken before the upgrade began.
        "resumed_upgrade": bool(resumed),
        "applied_migrations": _applied_migrations(dsn),
        "database": _database_identity(dsn),
        "fingerprint": json.loads(json.dumps(source)),
        "access_fingerprint": source_access,
        "previous_receipt": dict(previous) if previous is not None else None,
    }
    row["sidecar"] = str(_write_private_json(sidecar_path(archive), sidecar))
    return {**row, "ok": True}


def _applied_migrations(dsn: str) -> list[str]:
    from okg.substrate.db.connection import connect as db_connect

    with db_connect(dsn, autocommit=True, connect_timeout=10) as conn:
        rows = conn.execute(
            "SELECT migration_id FROM okg.schema_migrations ORDER BY migration_id"
        ).fetchall()
    return [str(row[0]) for row in rows]


def _create_backup_dump(dsn: str, archive: Path) -> Path:
    """`pg_dump --format=custom` WITH owners and privileges.

    ``postgres_clone.create_clone_dump`` drops privileges from the archive
    (it feeds development clones). A way back must carry every grant and
    PUBLIC revoke the migrations made, so this dump keeps them.
    """
    from okg.substrate import postgres_clone as pc

    path = Path(archive).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    source = pc._source_spec(dsn)
    route = pc._resolved_tool_route(
        dsn,
        database=source.database,
        probe_database=source.database,
        expected_server=source.server,
    )
    pc._run(
        [
            pc._tool("pg_dump"), "--format=custom", "--compress=6",
            "--file", str(path), "--dbname", route.conninfo,
        ],
        operation="pg_dump",
        env=route.env,
        redact_values=frozenset({route.conninfo}),
    )
    path.chmod(0o600)
    return path


_ACL_TOC_ENTRY = re.compile(r"^\s*\d+; \d+ \d+ (?:DEFAULT ACL|ACL) ")

# Table-of-contents entry types whose objects have an owner this replay sets,
# with the ALTER keyword for each. Relations come first: an owned sequence
# follows its table, and setting it to the owner it already has is allowed.
_OWNER_KINDS_RELATIONS = {
    "TABLE": "TABLE",
    "FOREIGN TABLE": "FOREIGN TABLE",
}
_OWNER_KINDS_OTHER = {
    "SCHEMA": "SCHEMA",
    "VIEW": "VIEW",
    "MATERIALIZED VIEW": "MATERIALIZED VIEW",
    "SEQUENCE": "SEQUENCE",
    "TYPE": "TYPE",
    "DOMAIN": "DOMAIN",
    "FUNCTION": "FUNCTION",
    "PROCEDURE": "PROCEDURE",
    "AGGREGATE": "AGGREGATE",
}
_ROUTINE_KINDS = frozenset({"FUNCTION", "PROCEDURE", "AGGREGATE"})
# Entry types that carry no owner of their own (the owner column names the
# owner of the object they belong to) or that the restore path creates
# itself. Anything not listed here or above is refused, never guessed.
_NON_OWNER_KINDS = frozenset({
    "ACL", "DEFAULT ACL", "CONSTRAINT", "CHECK CONSTRAINT", "FK CONSTRAINT",
    "INDEX", "INDEX ATTACH", "TABLE ATTACH", "TABLE DATA",
    "MATERIALIZED VIEW DATA", "SEQUENCE SET", "SEQUENCE OWNED BY", "DEFAULT",
    "TRIGGER", "COMMENT", "EXTENSION", "ROW SECURITY", "POLICY", "RULE",
    "ENCODING", "STDSTRINGS", "SEARCHPATH", "DATABASE", "DATABASE PROPERTIES",
    "SHELL TYPE", "CAST", "LARGE OBJECT DATA", "BLOBS", "SECURITY LABEL",
})
_TOC_KINDS = sorted(
    {*_OWNER_KINDS_RELATIONS, *_OWNER_KINDS_OTHER, *_NON_OWNER_KINDS,
     "LARGE OBJECT", "STATISTICS", "OPERATOR", "OPERATOR CLASS",
     "OPERATOR FAMILY", "COLLATION", "CONVERSION", "TEXT SEARCH CONFIGURATION",
     "TEXT SEARCH DICTIONARY", "TEXT SEARCH PARSER", "TEXT SEARCH TEMPLATE",
     "EVENT TRIGGER", "PUBLICATION", "PUBLICATION TABLE",
     "PUBLICATION TABLES IN SCHEMA", "SUBSCRIPTION", "FOREIGN DATA WRAPPER",
     "SERVER", "USER MAPPING", "TRANSFORM", "ACCESS METHOD", "PROCEDURAL LANGUAGE"},
    key=len,
    reverse=True,
)
_TOC_LINE = re.compile(
    r"^\s*\d+; \d+ \d+ (" + "|".join(re.escape(kind) for kind in _TOC_KINDS) + r") (.*)$"
)
_ROUTINE_TAG = re.compile(r"^([^ ()]+)\((.*)\)$")


def owner_entries(listing: str) -> list[dict[str, str]]:
    """Owned objects from a ``pg_restore --list`` table of contents.

    Each entry is (kind, schema, name, owner); for a routine, ``name`` is
    the tag ``name(argument types)``. Nothing from the archive is ever run
    as SQL: the caller quotes names as identifiers and resolves routines
    through ``to_regprocedure``. An entry that cannot be parsed, or whose
    type owns an object this replay does not handle, refuses.
    """
    entries: list[dict[str, str]] = []
    for line in listing.splitlines():
        if not line.strip() or line.lstrip().startswith(";"):
            continue
        match = _TOC_LINE.match(line)
        if match is None:
            raise InstalledUpgradeRefused(
                "the archive's table of contents has an entry the owner replay "
                "does not understand",
                data={"entry": line[:300]},
                fix_hint="report it; the backup cannot be proven restorable as is",
            )
        kind, rest = match.group(1), match.group(2)
        if kind in _NON_OWNER_KINDS:
            continue
        if kind not in _OWNER_KINDS_RELATIONS and kind not in _OWNER_KINDS_OTHER:
            raise InstalledUpgradeRefused(
                f"the archive holds a {kind}, whose owner the replay does not set",
                data={"entry": line[:300], "kind": kind},
                fix_hint="report it; the backup cannot be proven restorable as is",
            )
        parts = rest.split(" ")
        if len(parts) < 3:
            raise InstalledUpgradeRefused(
                "an owned entry in the archive cannot be parsed",
                data={"entry": line[:300]},
                fix_hint="report it; the backup cannot be proven restorable as is",
            )
        schema, owner, name = parts[0], parts[-1], " ".join(parts[1:-1])
        bad = (
            not owner
            or (kind == "SCHEMA" and schema != "-")
            or (kind != "SCHEMA" and schema == "-")
            or (kind in _ROUTINE_KINDS and _ROUTINE_TAG.match(name) is None)
            or (kind not in _ROUTINE_KINDS and " " in name)
        )
        if bad:
            raise InstalledUpgradeRefused(
                "an owned entry in the archive cannot be parsed",
                data={"entry": line[:300]},
                fix_hint=(
                    "the deployment's database was not changed; an object or role name with a space "
                    "cannot be replayed safely; rename it, or report it"
                ),
            )
        entries.append({"kind": kind, "schema": schema, "name": name, "owner": owner})
    entries.sort(key=lambda entry: entry["kind"] not in _OWNER_KINDS_RELATIONS)
    return entries


_CHUNK_SCHEMA = "_timescaledb_internal"
_ACL_TABLE_ENTRY = re.compile(r"^\s*\d+; \d+ \d+ ACL (\S+) TABLE (\S+) \S+$")
# A column grant: "ACL <schema> COLUMN <table>.<column> <owner>". Only the
# table part is read, so a column name with a space cannot hide a chunk.
_ACL_COLUMN_ENTRY = re.compile(r"^\s*\d+; \d+ \d+ ACL (\S+) COLUMN ([^.\s]+)\.")


def _chunk_predicate(schema: str, name: str, *, timescaledb: bool) -> str:
    """The one definition of "this (schema, name) is a TimescaleDB chunk", as SQL.

    True for anything in ``_timescaledb_internal``, and for a name
    ``<prefix>_<n>_chunk`` or ``<prefix>_<n>_chunk_compressed`` in a
    hypertable's ``associated_schema_name``, where ``<prefix>`` is that
    hypertable's ``associated_table_prefix``. TimescaleDB 2.29 names a
    compressed chunk after its uncompressed chunk (``_hyper_12_10_chunk``
    and ``_hyper_12_10_chunk_compressed``, both under hypertable 12's
    prefix) and does not list it in ``_timescaledb_catalog.chunk``, so the
    name is the only record of it. The prefix is compared as text, never
    as a pattern. ``schema`` and ``name`` are SQL expressions.
    """
    base = f"({schema} = '{_CHUNK_SCHEMA}'"
    if not timescaledb:
        return base + ")"
    return base + f""" OR EXISTS (
        SELECT 1 FROM _timescaledb_catalog.hypertable AS h
         WHERE {schema} = h.associated_schema_name
           AND left({name}, length(h.associated_table_prefix) + 1)
               = h.associated_table_prefix || '_'
           AND substr({name}, length(h.associated_table_prefix) + 2)
               ~ '^[0-9]+_chunk(_compressed)?$'))"""


def _has_hypertable_catalog(conn: Any) -> bool:
    return bool(conn.execute(
        "SELECT to_regclass('_timescaledb_catalog.hypertable') IS NOT NULL"
    ).fetchone()[0])


def chunk_names(conn: Any, candidates: set[tuple[str, str]]) -> set[tuple[str, str]]:
    """Which archive (schema, name) pairs are chunks, by the copy's catalog.

    Read from the restored copy's hypertable rows, not its relations:
    retention drops chunks, never hypertable rows, so the answer still
    covers chunks already gone from the copy.
    """
    if not candidates:
        return set()
    ordered = sorted(candidates)
    predicate = _chunk_predicate("t.s", "t.n", timescaledb=_has_hypertable_catalog(conn))
    rows = conn.execute(
        "SELECT t.s, t.n FROM unnest(%s::text[], %s::text[]) AS t(s, n) WHERE "
        + predicate,
        ([pair[0] for pair in ordered], [pair[1] for pair in ordered]),
    ).fetchall()
    return {(str(row[0]), str(row[1])) for row in rows}


def _acl_table_part(line: str) -> tuple[str, str] | None:
    table = _ACL_TABLE_ENTRY.match(line) or _ACL_COLUMN_ENTRY.match(line)
    return (table.group(1), table.group(2)) if table is not None else None


def acl_toc_entries(listing: str, chunks: set[tuple[str, str]]) -> tuple[list[str], int]:
    """The ACL and DEFAULT ACL entries to replay, and how many chunk ones were left out.

    A hypertable's grants, column grants included, already apply to its
    chunks, and in a restored copy the chunk may already be gone.
    """
    entries: list[str] = []
    chunk_entries = 0
    for line in listing.splitlines():
        if not _ACL_TOC_ENTRY.match(line):
            continue
        if _acl_table_part(line) in chunks:
            chunk_entries += 1
            continue
        entries.append(line)
    return entries, chunk_entries


def _owner_statement(conn: Any, entry: Mapping[str, str]) -> Any | None:
    """The ALTER ... OWNER TO for one entry, built from identifiers only."""
    from psycopg import sql

    kind, schema, name = entry["kind"], entry["schema"], entry["name"]
    owner = sql.Identifier(entry["owner"])
    keyword = sql.SQL(_OWNER_KINDS_RELATIONS.get(kind) or _OWNER_KINDS_OTHER[kind])
    if kind == "SCHEMA":
        return sql.SQL("ALTER {} {} OWNER TO {}").format(
            keyword, sql.Identifier(name), owner,
        )
    if kind in _ROUTINE_KINDS:
        routine, arguments = _ROUTINE_TAG.match(name).groups()
        text = (
            sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(routine))
            .as_string(conn) + f"({arguments})"
        )
        row = conn.execute(
            "SELECT to_regprocedure(%s)::text", (text,), prepare=True,
        ).fetchone()
        if row is None or row[0] is None:
            raise InstalledUpgradeRefused(
                f"the restored copy has no {kind.lower()} {schema}.{name}",
                data={"entry": dict(entry)},
                fix_hint="report it; the backup cannot be proven restorable as is",
            )
        return sql.SQL("ALTER {} {} OWNER TO {}").format(keyword, sql.SQL(row[0]), owner)
    target = sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(name))
    return sql.SQL("ALTER {} {} OWNER TO {}").format(keyword, target, owner)


def replay_archive_access(target_dsn: str, archive: str | Path) -> dict[str, Any]:
    """Give a database restored with --no-owner --no-privileges its access back.

    Sets the owner of every owned object the archive lists, built from the
    table of contents (kind, schema, name, owner) with quoted identifiers
    and, for routines, a ``to_regprocedure`` lookup; then replays every ACL
    and DEFAULT ACL entry with ``pg_restore``. Nothing from the archive's
    SQL text is executed by this code, and each owner statement runs alone
    through the extended protocol, which refuses more than one statement.
    Both are limited to the entries the verified restore path restores (the
    same table-of-contents filter). Owners go first because a dumped ACL is
    written relative to its owner; chunks are left to TimescaleDB, which
    gives them their hypertable's owner and grants. Chunks are recognised
    from the archive's names (schema ``_timescaledb_internal``, or a
    ``_hyper_N_M_chunk[_compressed]`` name in a hypertable's own chunk
    schema), because
    in the restored copy TimescaleDB's retention job may already have
    dropped them. Any failure raises.
    """
    from okg.substrate import postgres_clone as pc
    from okg.substrate.db.connection import connect as db_connect

    path = Path(archive).expanduser().resolve()
    listing = pc._run(
        [pc._tool("pg_restore"), "--list", str(path)], operation="access-list",
    ).stdout
    filtered = pc._filtered_restore_toc(
        listing,
        timescaledb_prestaged="timescaledb" in pc._archive_extensions(listing),
    )
    entries = owner_entries(filtered)
    database = str(pc._database_name(target_dsn, stage="access-replay"))
    applied = 0
    chunk_entries = 0
    with db_connect(target_dsn, autocommit=True, connect_timeout=10) as conn:
        server = pc._server_identity(conn)
        candidates = {
            (entry["schema"], entry["name"]) for entry in entries
            if entry["kind"] in _OWNER_KINDS_RELATIONS
        } | {
            part for line in filtered.splitlines()
            if _ACL_TOC_ENTRY.match(line) and (part := _acl_table_part(line)) is not None
        }
        chunks = chunk_names(conn, candidates)
        with conn.transaction():
            for entry in entries:
                if entry["kind"] in _OWNER_KINDS_RELATIONS and (
                    entry["schema"], entry["name"],
                ) in chunks:
                    chunk_entries += 1  # a chunk follows its hypertable's owner
                    continue
                conn.execute(_owner_statement(conn, entry), prepare=True)
                applied += 1
    acl_entries, chunk_acl_entries = acl_toc_entries(filtered, chunks)
    if acl_entries:
        with tempfile.TemporaryDirectory(prefix="okg-access-replay-") as tmp:
            acl_toc = Path(tmp) / "acl.toc"
            acl_toc.write_text("\n".join(acl_entries) + "\n", encoding="utf-8")
            route = pc._resolved_tool_route(
                target_dsn,
                database=database,
                probe_database=database,
                expected_server=server,
            )
            pc._run(
                [
                    pc._tool("pg_restore"), "--exit-on-error", "--single-transaction",
                    "--use-list", str(acl_toc), "--dbname", route.conninfo, str(path),
                ],
                operation="access-acl",
                env=route.env,
                redact_values=frozenset({route.conninfo}),
            )
    return {
        "owner_statements": applied,
        "chunk_owners_left_to_timescaledb": chunk_entries,
        "acl_entries": len(acl_entries),
        "chunk_acl_entries_left_to_timescaledb": chunk_acl_entries,
    }


_ACCESS_CHUNKS_SQL = {
    # TimescaleDB 2.x from 2.17: the catalog keeps the chunk's relation id.
    "relid": "SELECT relid::oid AS oid FROM _timescaledb_catalog.chunk",
    # Older releases keep its schema and table name.
    "names": (
        "SELECT to_regclass(format('%I.%I', schema_name, table_name))::oid AS oid "
        "FROM _timescaledb_catalog.chunk"
    ),
    "none": "SELECT NULL::oid AS oid WHERE false",
}

_ACCESS_SQL = """
WITH excluded AS (
    {chunks}
), objects AS (
    SELECT 'schema' AS kind, n.nspname AS schema_name, n.nspname AS object_name,
           pg_get_userbyid(n.nspowner) AS owner,
           COALESCE(n.nspacl, acldefault('n', n.nspowner)) AS acl
      FROM pg_namespace AS n
     WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
       AND n.nspname NOT LIKE 'pg\\_toast%' AND n.nspname NOT LIKE 'pg\\_temp%'
       AND NOT EXISTS (
           SELECT 1 FROM pg_depend AS d
            WHERE d.classid = 'pg_namespace'::regclass AND d.objid = n.oid
              AND d.deptype = 'e')
    UNION ALL
    SELECT 'relation:' || c.relkind::text, n.nspname, c.relname,
           pg_get_userbyid(c.relowner),
           COALESCE(c.relacl, acldefault(
               CASE WHEN c.relkind = 'S' THEN 's' ELSE 'r' END::"char", c.relowner))
      FROM pg_class AS c JOIN pg_namespace AS n ON n.oid = c.relnamespace
     WHERE c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f')
       AND n.nspname NOT IN ('pg_catalog', 'information_schema', '_timescaledb_internal')
       AND n.nspname NOT LIKE 'pg\\_toast%' AND n.nspname NOT LIKE 'pg\\_temp%'
       AND c.oid NOT IN (SELECT oid FROM excluded WHERE oid IS NOT NULL)
       AND NOT {chunk_names}
       AND NOT EXISTS (
           SELECT 1 FROM pg_depend AS d
            WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid
              AND d.deptype = 'e')
    UNION ALL
    SELECT 'function', n.nspname,
           p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')',
           pg_get_userbyid(p.proowner),
           COALESCE(p.proacl, acldefault('f', p.proowner))
      FROM pg_proc AS p JOIN pg_namespace AS n ON n.oid = p.pronamespace
     WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
       AND NOT EXISTS (
           SELECT 1 FROM pg_depend AS d
            WHERE d.classid = 'pg_proc'::regclass AND d.objid = p.oid
              AND d.deptype = 'e')
    UNION ALL
    SELECT 'default:' || d.defaclobjtype::text, COALESCE(n.nspname, ''), '',
           pg_get_userbyid(d.defaclrole), d.defaclacl
      FROM pg_default_acl AS d LEFT JOIN pg_namespace AS n ON n.oid = d.defaclnamespace
)
SELECT o.kind, o.schema_name, o.object_name, o.owner,
       CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE pg_get_userbyid(a.grantee) END,
       a.privilege_type, a.is_grantable
  FROM objects AS o CROSS JOIN LATERAL aclexplode(o.acl) AS a
 ORDER BY 1, 2, 3, 5, 6, 7
"""


def access_fingerprint(dsn: str) -> dict[str, Any]:
    """Owners and privileges of every schema, relation, function and default ACL.

    Left out: objects that belong to an extension (the extension creates
    them itself on restore), and TimescaleDB chunks, compressed chunks and
    everything else in ``_timescaledb_internal``, which TimescaleDB's own
    jobs create and drop (a restored copy runs its compression and retention
    jobs at once). Chunks are taken from ``_timescaledb_catalog.chunk``
    and, for TimescaleDB 2.29's uncatalogued compressed chunks, from the
    shared chunk-name rule (``_chunk_predicate``), inside the same statement
    that reads the privileges, so a job creating a chunk between two reads
    cannot produce a mismatch. Hypertables
    themselves are compared. The digest covers every row; ``entries`` keeps
    the rows so a mismatch can be shown.
    """
    from okg.substrate.db.connection import connect as db_connect

    with db_connect(dsn, autocommit=True, connect_timeout=10) as conn:
        columns = {
            str(row[0]) for row in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = '_timescaledb_catalog' AND table_name = 'chunk'"
            ).fetchall()
        }
        variant = (
            "relid" if "relid" in columns
            else "names" if {"schema_name", "table_name"} <= columns
            else "none"
        )
        statement = _ACCESS_SQL.replace(
            "{chunks}", _ACCESS_CHUNKS_SQL[variant],
        ).replace(
            "{chunk_names}",
            _chunk_predicate(
                "n.nspname", "c.relname", timescaledb=_has_hypertable_catalog(conn),
            ),
        )
        rows = [list(row) for row in conn.execute(statement).fetchall()]
    entries = [
        [str(value) if not isinstance(value, bool) else value for value in row]
        for row in rows
    ]
    digest = "sha256:" + hashlib.sha256(
        json.dumps(entries, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return {"digest": digest, "rows": len(entries), "entries": entries}


def access_differences(
    expected: Mapping[str, Any], observed: Mapping[str, Any], limit: int = 20,
) -> dict[str, Any]:
    want = {json.dumps(row) for row in expected.get("entries") or ()}
    have = {json.dumps(row) for row in observed.get("entries") or ()}
    return {
        "missing": [json.loads(row) for row in sorted(want - have)[:limit]],
        "unexpected": [json.loads(row) for row in sorted(have - want)[:limit]],
        "missing_count": len(want - have),
        "unexpected_count": len(have - want),
    }


def restore_rights(dsn: str, database: str) -> dict[str, Any]:
    """Whether this role can run the verified restore path on this server.

    A superuser can. When the database uses TimescaleDB, only a superuser
    can: ``timescaledb_pre_restore()`` sets a superuser parameter, and the
    swap must end TimescaleDB's superuser-owned scheduler. Otherwise a role
    needs CREATEDB and membership (USAGE) in the database owner's role. A
    non-superuser's ACL replay also fails on an entry whose grantor is not
    the object's owner. Anything else fails part way, so it is refused up
    front.
    """
    from okg.substrate.db import dsn as dsn_mod
    from okg.substrate.db.connection import connect as db_connect

    problems: list[str] = []
    maintenance = dsn_mod.parse(dsn).with_dbname("postgres").render()
    with db_connect(maintenance, autocommit=True, connect_timeout=10) as conn:
        actor, superuser, createdb = conn.execute(
            "SELECT current_user, rolsuper, rolcreatedb FROM pg_roles "
            "WHERE rolname = current_user"
        ).fetchone()
        owner_row = conn.execute(
            "SELECT pg_get_userbyid(datdba), "
            "pg_has_role(current_user, datdba, 'USAGE') "
            "FROM pg_database WHERE datname = %s",
            (database,),
        ).fetchone()
    with db_connect(dsn, autocommit=True, connect_timeout=10) as conn:
        timescaledb = conn.execute(
            "SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'"
        ).fetchone() is not None
    if owner_row is None:
        problems.append(f"database {database} does not exist")
    elif not superuser:
        if timescaledb:
            problems.append(
                f"{actor} is not a superuser, and {database} uses TimescaleDB: "
                "its restore hooks and its scheduler need a superuser"
            )
        if not owner_row[1]:
            problems.append(
                f"{actor} is not a member of {database}'s owner role {owner_row[0]}"
            )
        if not createdb:
            problems.append(f"{actor} lacks CREATEDB")
    return {
        "actor": str(actor),
        "superuser": bool(superuser),
        "owner": str(owner_row[0]) if owner_row else None,
        "timescaledb": timescaledb,
        "problems": problems,
    }


def sidecar_path(archive: str | Path) -> Path:
    archive = Path(archive)
    return archive.with_name(archive.name + ".json")


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _backup_fingerprint(dsn: str, deployment: str) -> dict[str, Any]:
    """What a restore must reproduce: migrations, catalog, the read graph."""
    from okg.substrate.db.connection import connect as db_connect

    with db_connect(dsn, autocommit=True, connect_timeout=10) as conn:
        migrations = conn.execute(
            "SELECT count(*) FROM okg.schema_migrations"
        ).fetchone()[0]
        catalogs = conn.execute(
            "SELECT count(*) FROM okg.catalog_versions"
        ).fetchone()[0]
    return {
        "schema_migrations": int(migrations),
        # The ids, not only the count: a different set of the same size is
        # a different schema.
        "schema_migration_ids": _applied_migrations(dsn),
        "catalog_versions": int(catalogs),
        **_graph_snapshot(dsn, deployment),
    }


def _graph_snapshot(dsn: str, deployment: str) -> dict[str, Any]:
    """The latest published generation readers see, with its live counts."""
    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.generation_resolver import resolve_okg_generation
    from okg.substrate.graph_store import ChronosGraphStore

    with db_connect(dsn, autocommit=True, connect_timeout=10) as conn:
        resolved = resolve_okg_generation(
            conn,
            deployment_name=deployment,
            requested=None,
            status="published",
            include_counts=True,
        )
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        published = [
            cp.checkpoint_id
            for cp in store.list_generations(
                deployment_name=deployment, branch=None, status="published",
            )
        ]
    return {
        "generation_id": resolved.generation_id if resolved else None,
        "node_count": resolved.node_count if resolved else None,
        "edge_count": resolved.edge_count if resolved else None,
        "published_generations": len(published),
        "published_generation_ids": published,
    }


def _drop_database(dsn: str, database: str) -> bool | dict[str, Any]:
    from okg.substrate.db import dsn as dsn_mod
    from okg.substrate.db.connection import connect as db_connect
    from psycopg import sql

    if not database.startswith(VERIFY_DATABASE_PREFIX):
        raise AssertionError(f"refusing to drop {database!r}")
    try:
        maintenance = dsn_mod.parse(dsn).with_dbname("postgres").render()
        with db_connect(maintenance, autocommit=True, connect_timeout=10) as conn:
            conn.execute(
                sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(database))
            )
    except Exception as exc:  # noqa: BLE001 - reported by the caller.
        return _error_summary(exc)
    return True


# --------------------------------------------------------- migrate / apply


def _pending_migrations(dsn: str) -> list[str]:
    from okg.substrate.db.management import migration_status

    status = migration_status(dsn)
    return [str(row.get("migration_id")) for row in status.get("pending") or ()]


def _step_migrate(
    deployment: str,
    dsn: str,
    runner: CommandRunner,
    env: dict[str, str],
    expected_database: Mapping[str, Any] | None,
) -> dict[str, Any]:
    problem = _identity_problem(dsn, expected_database)
    if problem:
        return {"step": "migrate", "ok": False, "reason": problem}
    argv = [*_okg_argv(), "migrate", "--deployment", deployment, "--apply", "--json"]
    result = runner(argv, dict(env))
    row = _command_row("migrate", result)
    if not row["ok"]:
        return row
    pending = _pending_migrations(dsn)
    if pending:
        return {
            **row,
            "ok": False,
            "pending_after": pending,
            "reason": (
                "`okg migrate` reported success, but this deployment's "
                f"database still has pending migrations ({', '.join(pending)}); "
                "it may have migrated a different database"
            ),
        }
    return row


def _step_deployment_apply(
    deployment: str,
    deployment_dir: Path,
    dsn: str,
    runner: CommandRunner,
    env: dict[str, str],
    expected_database: Mapping[str, Any] | None,
) -> dict[str, Any]:
    from okg.substrate.deployment_apply import deployment_apply_health

    problem = _identity_problem(dsn, expected_database)
    if problem:
        return {"step": "deployment_apply", "ok": False, "reason": problem}
    health = deployment_apply_health(
        deployment=deployment, deployment_dir=deployment_dir, dsn=dsn,
    )
    if health.get("ok") is True:
        return {
            "step": "deployment_apply",
            "ok": True,
            "skipped": f"already converged ({health.get('status')})",
            "warnings": health.get("warnings") or [],
        }
    plan_result = runner(
        [*_okg_argv(), "deployment", "apply", deployment, "--json"],
        dict(env),
    )
    plan = _json(plan_result)
    plan_id = plan.get("plan_id") if isinstance(plan, Mapping) else None
    if getattr(plan_result, "returncode", 1) != 0 or not plan_id:
        return {
            **_command_row("deployment_apply", plan_result),
            "ok": False,
            "reason": "`okg deployment apply` could not plan",
            "problems": health.get("problems"),
        }
    applied = runner(
        [*_okg_argv(), "deployment", "apply", deployment, "--apply",
         "--confirm", str(plan_id), "--json"],
        dict(env),
    )
    row = _command_row("deployment_apply", applied)
    row["plan_id"] = plan_id
    row["problems_before"] = health.get("problems")
    return row


def _public_graph(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    """A snapshot without the full id list, for reports and the receipt."""
    return {
        key: value for key, value in snapshot.items()
        if key not in {"published_generation_ids", "schema_migration_ids"}
    }


# ---------------------------------------------------------------- readiness


def _finding_key(row: Mapping[str, Any]) -> str:
    """A readiness finding as a comparable key: what failed and why.

    The check id alone hides a check whose cause changed (a role that lost
    LOGIN versus one that lost its password); numbers and timestamps are
    stripped so a changed count or age is not a new finding.
    """
    details = row.get("details") if isinstance(row.get("details"), Mapping) else {}
    parts = [
        row.get("id") or row.get("code") or "",
        row.get("status") or "",
        row.get("message") or "",
        details.get("code") or details.get("reason") or details.get("status") or "",
        details.get("role") or details.get("check") or "",
    ]
    return re.sub(r"\d+", "#", " | ".join(str(part) for part in parts))


def _readiness_snapshot(
    options: Any, deployment: str, runner: CommandRunner, env: dict[str, str],
) -> dict[str, Any]:
    """`okg deployment ready` as data: exit code, blocker and unknown keys."""
    argv = [
        *_okg_argv(), "deployment", "ready", deployment,
        "--checks", str(options.readiness_profile or "general"), "--json",
    ]
    result = runner(argv, dict(env))
    payload = _json(result)
    if not isinstance(payload, Mapping):
        return {
            "measured": False,
            "returncode": getattr(result, "returncode", None),
            "blockers": [],
            "unknowns": [],
        }
    return {
        "measured": True,
        "returncode": getattr(result, "returncode", None),
        "status": payload.get("status"),
        "blockers": sorted({
            _finding_key(row) for row in payload.get("blockers") or ()
            if isinstance(row, Mapping)
        }),
        "unknowns": sorted({
            _finding_key(row) for row in payload.get("unknowns") or ()
            if isinstance(row, Mapping)
        }),
    }


def _step_readiness(
    options: Any,
    deployment: str,
    dsn: str,
    runner: CommandRunner,
    env: dict[str, str],
    expected_database: Mapping[str, Any] | None,
    graph_before: Mapping[str, Any],
    readiness_before: Mapping[str, Any],
) -> dict[str, Any]:
    """Readiness after the upgrade: nothing new, and the graph is intact.

    A blocker or unknown finding that `okg deployment ready` already
    reported before the upgrade (for example the MCP role a fresh install
    leaves without LOGIN) is not this upgrade's doing; it is listed as
    pre-existing, never hidden. One that appears only afterwards, or whose
    cause changed, fails the step. When the readiness before could not be
    read, every blocker and unknown afterwards counts as new.
    """
    problem = _identity_problem(dsn, expected_database)
    if problem:
        return {"step": "readiness", "ok": False, "reason": problem}
    after = _readiness_snapshot(options, deployment, runner, env)
    graph_after = _graph_snapshot(dsn, deployment)
    row: dict[str, Any] = {
        "step": "readiness",
        "readiness_after": after,
        "graph_before": _public_graph(graph_before),
        "graph_after": _public_graph(graph_after),
    }
    problems: list[str] = []
    if not after.get("measured"):
        problems.append("`okg deployment ready` returned no readable report")
    measured_before = bool(readiness_before.get("measured"))
    if not measured_before:
        row["readiness_before_unmeasured"] = True
    for kind in ("blockers", "unknowns"):
        before = set(readiness_before.get(kind) or ()) if measured_before else set()
        now = set(after.get(kind) or ())
        new = sorted(now - before)
        kept = sorted(now & before)
        if kept:
            row[f"pre_existing_{kind}"] = kept
        if new:
            row[f"new_{kind}"] = new
            problems.append(f"new readiness {kind}: {'; '.join(new)}")
    before_id = graph_before.get("generation_id")
    if before_id is not None:
        if before_id not in (graph_after.get("published_generation_ids") or ()):
            problems.append(f"published generation {before_id} is gone")
        elif graph_after.get("generation_id") == before_id and (
            graph_after.get("node_count") != graph_before.get("node_count")
            or graph_after.get("edge_count") != graph_before.get("edge_count")
        ):
            problems.append(
                f"generation {before_id} now reads "
                f"{graph_after.get('node_count')} nodes / "
                f"{graph_after.get('edge_count')} edges, was "
                f"{graph_before.get('node_count')} / "
                f"{graph_before.get('edge_count')}"
            )
    row["ok"] = not problems
    if problems:
        row["reason"] = "; ".join(problems)
    return row


# ------------------------------------------------------------------ helpers


def _resolve_dsn(options: Any) -> str:
    from okg.substrate.deployment_upgrade import _resolve_target

    resolution = _resolve_target(options)
    dsn = resolution.get("dsn")
    if not dsn:
        raise InstalledUpgradeRefused(
            "deployment database is unresolved",
            data={"deployment": options.deployment},
            fix_hint=f"run `okg deployment env doctor {options.deployment} --json`",
        )
    return str(dsn)


def _child_env(deployment: str, dsn: str) -> dict[str, str]:
    """The environment every child okg runs with: pinned to ``dsn``.

    ``OKG_DSN`` and every ``${VAR}`` the manifest's ``postgres.dsn`` names
    are set to the one URL this run resolved, so a child re-resolving the
    deployment's database cannot land on another one.
    """
    from okg.substrate.deployment_authority import _env_refs, _read_manifest

    env = dict(os.environ)
    env["OKG_DSN"] = dsn
    raw, _ = _read_manifest(deployment)
    postgres = raw.get("postgres") if isinstance(raw, dict) else None
    manifest_dsn = postgres.get("dsn") if isinstance(postgres, dict) else None
    if isinstance(manifest_dsn, str):
        for ref in _env_refs(manifest_dsn):
            env[ref] = dsn
    return env


def _next_action_after_failure(
    deployment: str,
    failed: Mapping[str, Any],
    pre_upgrade_archive: str | None,
    previous: Mapping[str, Any] | None,
    *,
    worker_stopped: bool,
    resumed: bool,
    deployment_dir: Path,
    resolve_dsn: Callable[[], str],
) -> str:
    step = failed.get("step")
    fix = failed.get("fix_hint") or (failed.get("error") or {}).get("fix_hint")
    parts = [
        f"{fix}, then run `okg up {deployment}` again" if fix
        else f"fix the cause of the failed {step} step, then run `okg up {deployment}` again"
    ]
    untouched = step in {"preflight", "stop_worker", "backup"} and not resumed
    if untouched:
        parts.append("the database was not changed")
    if pre_upgrade_archive and not untouched:
        parts.append(
            "to go back instead: with the okg installed now, run `okg "
            f"deployment backup restore {deployment} --archive "
            f"{pre_upgrade_archive}` to see the plan, then the same command "
            "with --apply; then "
            + _go_back_hint(previous.get("okg_version") if previous else None)
        )
    if worker_stopped:
        parts.append(
            "the runtime worker is stopped; the next `okg up` restarts it, or "
            "start it once the deployment is consistent with "
            + start_worker_command(
                deployment, deployment_dir=deployment_dir, resolve_dsn=resolve_dsn,
            )
        )
    return "; ".join(parts)


def start_worker_command(
    deployment: str, *, deployment_dir: Path, resolve_dsn: Callable[[], str],
) -> str:
    """How the operator starts the worker: its service, or by hand."""
    try:
        service = worker_service(
            deployment, deployment_dir=deployment_dir, resolve_dsn=resolve_dsn,
        )
    except Exception as exc:  # noqa: BLE001 - said in the advice, never hidden.
        return (
            f"`okg dogfood up --deployment {deployment} --apply` if this "
            "deployment has a worker service, otherwise `okg runtime worker "
            f"--deployment {deployment}` (okg could not check which: "
            f"{type(exc).__name__}: {_redacted_tail(str(exc))[-200:]})"
        )
    if service.get("available"):
        return f"`okg dogfood up --deployment {deployment} --apply`"
    return (
        f"`okg runtime worker --deployment {deployment}` (no worker service is "
        "installed, so start it the way you started it before)"
    )


def _command_row(step: str, result: Any) -> dict[str, Any]:
    returncode = getattr(result, "returncode", 1)
    row: dict[str, Any] = {
        "step": step,
        "ok": returncode == 0,
        "returncode": returncode,
    }
    payload = _json(result)
    if isinstance(payload, Mapping):
        row["result"] = {
            key: payload.get(key)
            for key in ("ok", "status", "applied", "verdict", "summary")
            if key in payload
        }
    if returncode != 0:
        row["stderr_tail"] = _redacted_tail(getattr(result, "stderr", ""))
        row["stdout_tail"] = _redacted_tail(getattr(result, "stdout", ""))
    return row


def _redacted_tail(text: str | None) -> str:
    from okg.substrate.db.dsn import redact_text

    return redact_text((text or "")[-2000:], dsn_flags=())


def _json(result: Any) -> Any:
    try:
        return json.loads(getattr(result, "stdout", "") or "")
    except ValueError:
        return None


def _error_summary(exc: BaseException) -> dict[str, Any]:
    from okg.substrate.db.dsn import redact_text

    data = getattr(exc, "data", None)
    summary: dict[str, Any] = {
        "type": type(exc).__name__,
        "message": redact_text(str(exc), dsn_flags=()),
    }
    if getattr(exc, "fix_hint", None):
        summary["fix_hint"] = str(exc.fix_hint)
    if isinstance(data, Mapping):
        for key in ("stage", "tool", "returncode", "target_state"):
            if data.get(key) is not None:
                summary[key] = data.get(key)
        for key in ("stderr", "stdout"):
            if data.get(key):
                summary[f"{key}_tail"] = redact_text(
                    str(data.get(key))[-2000:], dsn_flags=(),
                )
    return summary


def _okg_argv() -> list[str]:
    from okg.substrate.cli.child_argv import running_okg_argv

    return running_okg_argv()


def _default_runner() -> CommandRunner:
    from okg.substrate.deployment_upgrade import _default_command_runner

    return _default_command_runner


def _slug(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9.]+", "-", value).strip("-") or "unknown"


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")
