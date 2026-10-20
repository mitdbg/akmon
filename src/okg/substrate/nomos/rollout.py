"""Core Nomos rollout validation shared by runtime and operator surfaces."""
from __future__ import annotations

import datetime
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from okg.substrate import fastyaml

__all__ = [
    "audit_deferred_rollout",
    "deployment_dsn_key",
    "enforce_mode_dsn_keys",
    "runtime_enforce_mode_dsn_keys",
]


def _normalize_rollout_owner(owner: Any) -> dict[str, Any] | None:
    if not isinstance(owner, Mapping):
        return None
    name = ""
    for key in ("name", "team", "id"):
        value = owner.get(key)
        if isinstance(value, str) and value.strip():
            name = value.strip()
            break
    contact = owner.get("contact")
    contact = contact.strip() if isinstance(contact, str) and contact.strip() else ""
    if not name or not contact:
        return None
    return {"name": name, "contact": contact}


def _parse_rollout_expiry(value: Any) -> datetime.date | None:
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        return value
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, str) and value.strip():
        try:
            return datetime.date.fromisoformat(value.strip()[:10])
        except ValueError:
            return None
    return None


def audit_deferred_rollout(
    rollout: Mapping[str, Any],
    *,
    deployment_name: str,
    deployment_dsn_key: str | None,
    enforce_dsn_keys: Mapping[str, str] | None = None,
    today: datetime.date | None = None,
) -> list[dict[str, Any]]:
    """Audit a deferred rollout's owner, expiry, and database isolation."""
    if today is None:
        today = datetime.date.today()
    findings: list[dict[str, Any]] = []

    if _normalize_rollout_owner(rollout.get("owner")) is None:
        findings.append({
            "code": "nomos.rollout.deferred_owner_missing",
            "reason_code": "nomos.rollout.deferred_owner_missing",
            "severity": "error",
            "message": (
                "deferred rollout must declare a structured owner "
                "(name/team and contact)"
            ),
            "path": "nomos.rollout.owner",
        })

    raw_expiry = (
        rollout.get("expiry")
        if rollout.get("expiry") is not None
        else rollout.get("review_by")
    )
    expiry = _parse_rollout_expiry(raw_expiry)
    if raw_expiry is None or expiry is None:
        findings.append({
            "code": "nomos.rollout.deferred_expiry_missing",
            "reason_code": "nomos.rollout.deferred_expiry_missing",
            "severity": "error",
            "message": (
                "deferred rollout must declare an expiry/review_by date "
                "(YYYY-MM-DD)"
            ),
            "path": "nomos.rollout.expiry",
        })
    elif expiry < today:
        findings.append({
            "code": "nomos.rollout.deferred_expired",
            "reason_code": "nomos.rollout.deferred_expired",
            "severity": "error",
            "message": (
                f"deferred rollout expired on {expiry.isoformat()} "
                f"(today {today.isoformat()})"
            ),
            "path": "nomos.rollout.expiry",
            "expiry": expiry.isoformat(),
            "today": today.isoformat(),
        })

    if deployment_dsn_key and enforce_dsn_keys:
        shared = sorted(
            other
            for other, key in enforce_dsn_keys.items()
            if key == deployment_dsn_key and other != deployment_name
        )
        if shared:
            findings.append({
                "code": "nomos.rollout.deferred_shared_enforce_dsn",
                "reason_code": "nomos.rollout.deferred_shared_enforce_dsn",
                "severity": "error",
                "message": (
                    "deferred rollout is forbidden while sharing a "
                    "database/DSN with an enforce-mode deployment"
                ),
                "path": "postgres.dsn",
                "shared_with": shared,
                "dsn_key": deployment_dsn_key,
            })
    return findings


def deployment_dsn_key(manifest: Mapping[str, Any]) -> str | None:
    """Return the canonical database/DSN identity used for sharing checks."""
    postgres = manifest.get("postgres")
    if not isinstance(postgres, Mapping):
        return None
    database = postgres.get("database")
    raw_dsn = postgres.get("dsn")
    parts: list[str] = []
    if isinstance(raw_dsn, str) and raw_dsn.strip():
        parts.append(raw_dsn.strip())
    if isinstance(database, str) and database.strip():
        parts.append(f"db={database.strip()}")
    return "|".join(parts) or None


def enforce_mode_dsn_keys(*deployment_roots: Path) -> dict[str, str]:
    """Map enforce-mode deployment names to DSN keys across known roots."""
    return _scan_enforce_mode_dsn_keys(*deployment_roots, strict=False)[0]


def _scan_enforce_mode_dsn_keys(
    *deployment_roots: Path,
    strict: bool,
) -> tuple[dict[str, str], bool]:
    from okg.substrate.nomos.runtime_enforcement import (
        manifest_nomos_enforcement_mode,
    )

    keys: dict[str, str] = {}
    for root in dict.fromkeys(Path(root).resolve() for root in deployment_roots):
        if not root.exists():
            continue
        for manifest_path in sorted(root.glob("*/deployment.yaml")):
            try:
                raw = fastyaml.safe_load(manifest_path.read_text(encoding="utf-8"))
            except Exception:  # noqa: BLE001 - inventory reports malformed files elsewhere.
                if strict:
                    return keys, False
                continue
            if not isinstance(raw, Mapping):
                if strict:
                    return keys, False
                continue
            try:
                enforcement_mode = manifest_nomos_enforcement_mode(raw)
            except Exception:  # noqa: BLE001 - incomplete runtime authority fails closed.
                if strict:
                    return keys, False
                continue
            if enforcement_mode != "enforce":
                continue
            key = deployment_dsn_key(raw)
            if key is None:
                if strict:
                    return keys, False
                continue
            keys[str(raw.get("name") or manifest_path.parent.name)] = key
    return keys, True


def runtime_enforce_mode_dsn_keys(deployment_dir: Path) -> dict[str, str] | None:
    """Collect complete runtime DSN authority, or ``None`` on any uncertainty."""
    from okg.substrate.deployments import (
        find_deployments_root,
        user_state_deployments_root,
    )

    roots = [Path(deployment_dir).resolve().parent]
    discovered = find_deployments_root()
    if discovered is not None:
        roots.append(discovered)
    # okg's own deployments/. A fixed parents[N] walk from this file finds it
    # only in a source checkout; a wheel install (the runtime image) names its
    # tree through OKG_FRAMEWORK_TREE, which refuses if it is not there.
    from okg.substrate.framework_tree import framework_tree_root

    tree = framework_tree_root()
    if tree is not None and (tree / "deployments").is_dir():
        roots.append(tree / "deployments")
    user_root = user_state_deployments_root()
    if user_root.is_dir():
        roots.append(user_root)
    keys, complete = _scan_enforce_mode_dsn_keys(*roots, strict=True)
    return keys if complete else None
