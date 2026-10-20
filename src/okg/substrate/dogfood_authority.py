"""Machine-local authority records for self-hosted dogfood deployments.

The record is deliberately outside a checkout.  A worktree can therefore
consume the endpoint published by the dogfood service without inheriting a
different checkout's ``.okg/local.env`` or guessing an isolated database.
The file is a protected local credential source; callers must not include its
DSN in status or error payloads.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AUTHORITY_SCHEMA = "okg.dogfood.authority.v1"
AUTHORITY_VERSION = 1
_SAFE_TOKEN = re.compile(r"[^A-Za-z0-9_.-]+")


def authority_path(deployment: str) -> Path:
    """Return the machine-local authority path for ``deployment``."""
    override = os.environ.get("OKG_DOGFOOD_AUTHORITY_PATH")
    if override:
        return Path(override).expanduser().resolve()
    token = _SAFE_TOKEN.sub("_", deployment).strip("._") or "deployment"
    return (
        Path.home()
        / ".local"
        / "state"
        / "okg"
        / "dogfood-authority"
        / f"{token}.json"
    )


def read_authority(deployment: str) -> tuple[dict[str, Any] | None, str | None]:
    """Read and validate a local authority record without exposing its DSN."""
    path = authority_path(deployment)
    if not path.is_file():
        return None, None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001 - bounded operator-facing failure
        return None, f"authority record is unreadable: {exc}"
    if not isinstance(raw, dict):
        return None, "authority record must be a JSON object"
    if raw.get("schema") != AUTHORITY_SCHEMA:
        return None, "authority record schema is unsupported"
    if raw.get("version") != AUTHORITY_VERSION:
        return None, "authority record version is unsupported"
    if raw.get("deployment") != deployment:
        return None, "authority record deployment identity mismatches the request"
    for key in ("dsn", "database", "host", "port", "runtime_instance"):
        if not raw.get(key):
            return None, f"authority record is missing {key}"
    if not isinstance(raw.get("port"), int) or raw["port"] <= 0:
        return None, "authority record port is invalid"
    if not isinstance(raw.get("runtime_instance"), dict):
        return None, "authority record runtime_instance is invalid"
    return raw, None


def write_authority(
    deployment: str,
    *,
    dsn: str,
    database: str,
    host: str,
    port: int,
    runtime_instance: dict[str, str],
    descriptor_sha256: str,
    credential_source: str = "protected_record",
) -> Path:
    """Atomically publish a protected dogfood authority record."""
    path = authority_path(deployment)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema": AUTHORITY_SCHEMA,
        "version": AUTHORITY_VERSION,
        "deployment": deployment,
        "database": database,
        "host": host,
        "port": port,
        "dsn": dsn,
        "credential_source": credential_source,
        "runtime_instance": runtime_instance,
        "descriptor_sha256": descriptor_sha256,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent),
    )
    temp_path = Path(temp_name)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
        os.chmod(path, 0o600)
    except Exception:
        try:
            temp_path.unlink(missing_ok=True)
        except OSError:
            # Preserve the original write failure; cleanup is best-effort only.
            pass
        raise
    return path
