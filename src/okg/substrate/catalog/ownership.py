"""Catalog writer provenance and writable-DB ownership guards."""
from __future__ import annotations

import datetime
import hashlib
import json
import logging
import os
import platform
import shlex
import socket
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import re

import psycopg
from psycopg.types.json import Jsonb

from okg.substrate.catalog.modules_compose import ComposedCatalog
from okg.substrate.db.connection import redact as redact_dsn
from okg.substrate.errors import CatalogOwnershipMismatch, SubstrateError
from okg.substrate.support.user_hints import okg_prefix


@dataclass(frozen=True)
class CatalogIdentity:
    deployment_name: str
    catalog_fingerprint: str
    schema_fingerprint: str
    writer_fingerprint: str
    deployment_root_hash: str | None
    worktree_root_hash: str | None
    provenance: dict[str, Any]
    # Manifest content-hash over the publisher-stack + source-sync input
    # closure (#709, bind-deployment-identity). None only for pathless /
    # legacy callers whose deployment_dir carries no deployment.yaml;
    # ``manifest_block_hashes`` names the drifted block(s) in a refusal.
    manifest_fingerprint: str | None = None
    manifest_block_hashes: dict[str, str] | None = None


WRITER_SCOPE_ENVIRONMENT = "environment"
WRITER_SCOPE_DEPLOYMENT_ROOT = "deployment_root"

# Machine-readable discriminators for the scope refusals below. They ride
# in `data["reason_code"]` of a CatalogOwnershipMismatch, alongside the
# pre-existing `catalog_ownership_*` reasons.
WRITER_SCOPE_AMBIGUOUS = "catalog_writer_scope_ambiguous"
WRITER_SCOPE_CHANGE = "catalog_writer_scope_change"
WRITER_SCOPE_CONFLICT = "catalog_writer_scope_conflict"


def _writer_scope_error(
    message: str,
    *,
    reason_code: str,
    fix_hint: str,
    data: dict[str, Any] | None = None,
) -> SubstrateError:
    """Return a scope refusal with one stable public classification."""
    error = SubstrateError(
        message,
        data={"reason_code": reason_code, **(data or {})},
        fix_hint=fix_hint,
    )
    error.code = reason_code
    return error


def sha256_text(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )

def catalog_identity_for(
    *,
    composed: ComposedCatalog,
    deployment_dir: Path | str,
    command: str,
    writer_scope: str = WRITER_SCOPE_DEPLOYMENT_ROOT,
    environment_id: str | None = None,
) -> CatalogIdentity:
    deployment_root = Path(deployment_dir).expanduser().resolve()
    worktree_root = _git_output(["rev-parse", "--show-toplevel"], deployment_root)
    if worktree_root:
        worktree_root_path: Path | None = Path(worktree_root).resolve()
    else:
        worktree_root_path = _find_repo_root(deployment_root)
    content_payload = _catalog_content_payload(composed)
    catalog_fingerprint = sha256_text(canonical_json(content_payload))
    schema_payload = {
        "deployment_name": composed.deployment_name,
        "catalog_fingerprint": catalog_fingerprint,
        "modules": list(composed.modules),
    }
    schema_fingerprint = sha256_text(canonical_json(schema_payload))
    deployment_root_hash = sha256_text(str(deployment_root))
    worktree_root_hash = (
        sha256_text(str(worktree_root_path)) if worktree_root_path else None
    )
    # #1303: writer scope is caller authority, never ambient process state.
    # Deployment-root writers retain the checkout-path guard. Environment
    # writers bind to a stated environment id so a content-addressed reseal
    # keeps the same identity. OKG_ENVIRONMENT_ID is deliberately ignored.
    environment_id = (environment_id or "").strip()
    if writer_scope is None or not str(writer_scope).strip():
        raise _writer_scope_error(
            "catalog writer scope is required",
            reason_code="catalog_writer_scope_required",
            fix_hint=(
                "select writer_scope='deployment_root' or 'environment'"
            ),
        )
    if writer_scope not in {
        WRITER_SCOPE_DEPLOYMENT_ROOT,
        WRITER_SCOPE_ENVIRONMENT,
    }:
        raise _writer_scope_error(
            f"unsupported catalog writer scope: {writer_scope!r}",
            reason_code="catalog_writer_scope_invalid",
            fix_hint=(
                "select writer_scope='deployment_root' or 'environment'"
            ),
        )
    if writer_scope == WRITER_SCOPE_ENVIRONMENT and not environment_id:
        raise _writer_scope_error(
            "environment-scoped catalog identity requires an explicit "
            "environment id",
            reason_code="catalog_environment_identity_required",
            fix_hint="resolve and pass the selected environment id explicitly",
        )
    if writer_scope == WRITER_SCOPE_DEPLOYMENT_ROOT and environment_id:
        raise _writer_scope_error(
            "deployment-root catalog identity cannot carry an environment id",
            reason_code=WRITER_SCOPE_AMBIGUOUS,
            fix_hint="select one writer scope and identity",
        )
    # #2229: the writer is the deployment and its install claim, never the
    # schema. A schema change (a new module, any ontology edit) is a
    # catalog-version question answered by `catalog_fingerprint`; binding
    # it here made the deployment's own writer look foreign.
    writer_fingerprint = sha256_text(canonical_json(_writer_payload(
        deployment_name=composed.deployment_name,
        writer_scope=writer_scope,
        environment_id=environment_id,
        deployment_root_hash=deployment_root_hash,
        worktree_root_hash=worktree_root_hash,
    )))
    # #709: bind the manifest's behavior-bearing content into the identity.
    # Computed from the deployment_dir the process actually loaded, so a
    # branch flip to a retired manifest yields a different fingerprint and
    # the publish check refuses. A present-but-unreadable manifest raises
    # (ManifestFingerprintError, a SubstrateError) -- error-reading-a-
    # present-source is loud, never a benign default (ruling R3).
    from okg.substrate.catalog.manifest_identity import (
        manifest_fingerprint_for,
    )
    manifest_fp, manifest_blocks = manifest_fingerprint_for(deployment_root)
    provenance = {
        "deployment_name": composed.deployment_name,
        "command": command,
        # Plaintext roots (#828): a refusal must be able to NAME the
        # directory each side computed its identity from; the hashes alone
        # sent operators re-claiming from the wrong checkout.
        "deployment_root": str(deployment_root),
        "worktree_root": (
            str(worktree_root_path) if worktree_root_path else None
        ),
        "deployment_root_hash": deployment_root_hash,
        "worktree_root_hash": worktree_root_hash,
        # Which scope the writer fingerprint was built from, so a refusal
        # can say whether two sides disagree because they are different
        # checkouts or different environments. The resolved caller, not an
        # ambient variable, owns this value.
        "writer_scope": writer_scope,
        "environment_id": environment_id or None,
        "writer_scope_source": "explicit",
        "ambient_environment_id": None,
        "descriptor_environment_ids": [],
        "release_artifact_id": os.environ.get("OKG_RELEASE_ARTIFACT_ID"),
        "git_commit": _git_output(["rev-parse", "HEAD"], deployment_root),
        "git_branch": _git_output(
            ["rev-parse", "--abbrev-ref", "HEAD"], deployment_root,
        ),
        "git_dirty": _git_dirty(deployment_root),
        "hostname": socket.gethostname(),
        # Hashed stable machine id (#825): the same-host staleness probe
        # keys on this, because the hostname roams with DHCP/VPN.
        "machine_id": _machine_id(),
        "pid": os.getpid(),
        "platform": platform.platform(),
    }
    return CatalogIdentity(
        deployment_name=composed.deployment_name,
        catalog_fingerprint=catalog_fingerprint,
        schema_fingerprint=schema_fingerprint,
        writer_fingerprint=writer_fingerprint,
        deployment_root_hash=deployment_root_hash,
        worktree_root_hash=worktree_root_hash,
        provenance=provenance,
        manifest_fingerprint=manifest_fp,
        manifest_block_hashes=manifest_blocks or None,
    )


WRITER_FINGERPRINT_VERSION = 2

# Metadata key on the ownership row recording the one-time move from a v1
# (schema-derived) owner key to the v2 key: {from, to, command}.
WRITER_REKEY_METADATA_KEY = "writer_fingerprint_rekey"


def _writer_payload(
    *,
    deployment_name: str,
    writer_scope: str,
    environment_id: str | None,
    deployment_root_hash: str | None,
    worktree_root_hash: str | None,
) -> dict[str, Any]:
    if writer_scope == WRITER_SCOPE_ENVIRONMENT:
        return {
            "v": WRITER_FINGERPRINT_VERSION,
            "deployment_name": deployment_name,
            "environment_id": environment_id,
        }
    return {
        "v": WRITER_FINGERPRINT_VERSION,
        "deployment_name": deployment_name,
        "deployment_root_hash": deployment_root_hash,
        "worktree_root_hash": worktree_root_hash,
    }


def legacy_writer_fingerprint(
    *,
    deployment_name: str,
    schema_fingerprint: str | None,
    writer_scope: str | None,
    environment_id: str | None,
    deployment_root_hash: str | None,
    worktree_root_hash: str | None,
) -> str | None:
    """Return the v1 writer key, which also bound the schema fingerprint.

    Ownership rows and catalog versions written before #2229 carry this key.
    It is reproduced exactly so those rows are recognized as the same writer
    and re-keyed once; None when the inputs cannot name a v1 key.
    """
    if not schema_fingerprint:
        return None
    if writer_scope == WRITER_SCOPE_ENVIRONMENT:
        if not environment_id:
            return None
        payload: dict[str, Any] = {
            "deployment_name": deployment_name,
            "schema_fingerprint": schema_fingerprint,
            "environment_id": environment_id,
        }
    elif writer_scope == WRITER_SCOPE_DEPLOYMENT_ROOT:
        payload = {
            "deployment_name": deployment_name,
            "schema_fingerprint": schema_fingerprint,
            "deployment_root_hash": deployment_root_hash,
            "worktree_root_hash": worktree_root_hash,
        }
    else:
        return None
    return sha256_text(canonical_json(payload))


def legacy_writer_fingerprint_for(
    identity: CatalogIdentity,
    schema_fingerprint: str | None,
) -> str | None:
    """The v1 writer key this identity would have had under a given schema."""
    provenance = identity.provenance or {}
    return legacy_writer_fingerprint(
        deployment_name=identity.deployment_name,
        schema_fingerprint=schema_fingerprint,
        writer_scope=provenance.get("writer_scope"),
        environment_id=provenance.get("environment_id"),
        deployment_root_hash=identity.deployment_root_hash,
        worktree_root_hash=identity.worktree_root_hash,
    )


def writer_key_match(
    identity: CatalogIdentity,
    fingerprint: str | None,
    *,
    schema_fingerprint: str | None,
) -> str | None:
    """Classify a stored writer key against this identity.

    ``"current"`` for the v2 key, ``"legacy"`` for the v1 key built with the
    stored row's own ``schema_fingerprint``, None for a different writer.
    """
    if not fingerprint:
        return None
    if fingerprint == identity.writer_fingerprint:
        return "current"
    legacy = legacy_writer_fingerprint_for(identity, schema_fingerprint)
    if legacy is not None and fingerprint == legacy:
        return "legacy"
    return None


def _owner_key_match(
    identity: CatalogIdentity, owner: dict[str, Any] | None,
) -> str | None:
    if not owner:
        return None
    return writer_key_match(
        identity,
        owner.get("owner_fingerprint"),
        schema_fingerprint=owner.get("schema_fingerprint"),
    )


def _applied_writer_keys(identity: CatalogIdentity) -> list[str]:
    """Writer keys an applied catalog version of THIS process's catalog may
    carry: the v2 key, or the v1 key built from the process's own schema."""
    keys = [identity.writer_fingerprint]
    legacy = legacy_writer_fingerprint_for(identity, identity.schema_fingerprint)
    if legacy is not None:
        keys.append(legacy)
    return keys


def _rekey_audit(identity: CatalogIdentity, old: str) -> dict[str, Any]:
    return {
        "from": old,
        "to": identity.writer_fingerprint,
        "command": (identity.provenance or {}).get("command"),
    }


def _rekey_locked_owner(
    cur: psycopg.Cursor,
    *,
    identity: CatalogIdentity,
    old_fingerprint: str,
) -> dict[str, Any] | None:
    """Move a v1 owner key to the v2 key; the caller holds the row lock.

    Guarded on the old key so a concurrent change is never overwritten.
    Returns the audit entry, or None when the row no longer holds the key.
    """
    audit = _rekey_audit(identity, old_fingerprint)
    cur.execute(
        """
        UPDATE okg.deployment_write_ownership
           SET owner_fingerprint = %s,
               metadata = metadata || %s::jsonb
         WHERE deployment_name = %s
           AND owner_fingerprint = %s
        """,
        (
            identity.writer_fingerprint,
            Jsonb({WRITER_REKEY_METADATA_KEY: audit}),
            identity.deployment_name,
            old_fingerprint,
        ),
    )
    if cur.rowcount != 1:
        return None
    return audit


def rekey_legacy_owner(
    conn: psycopg.Connection,
    *,
    identity: CatalogIdentity,
) -> dict[str, Any]:
    """Re-key this writer's v1 ownership row once, under a row lock.

    Returns ``{"rekeyed": True, ...audit}`` after a move, ``{"rekeyed":
    False, "existing_rekey": <audit or None>}`` when the row already holds
    the v2 key, and raises ``catalog_ownership_foreign_writer`` when the
    locked row belongs to neither key (or is gone).

    ``existing_rekey`` is read under the row lock. A caller that saw the v1
    key in an earlier, unlocked read lost a race to another process that
    re-keyed first; it must carry THAT audit forward, not its stale
    snapshot's, or a metadata-replacing claim erases the only record of the
    move (#2229 review F2).
    """
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            """
            SELECT owner_fingerprint, schema_fingerprint, metadata -> %s
              FROM okg.deployment_write_ownership
             WHERE deployment_name = %s
               FOR UPDATE
            """,
            (WRITER_REKEY_METADATA_KEY, identity.deployment_name),
        )
        row = cur.fetchone()
        match = (
            writer_key_match(identity, row[0], schema_fingerprint=row[1])
            if row is not None else None
        )
        if match == "current":
            return {"rekeyed": False, "existing_rekey": row[2]}
        audit = (
            _rekey_locked_owner(cur, identity=identity, old_fingerprint=row[0])
            if match == "legacy" else None
        )
        if audit is None:
            _raise_mismatch(
                "catalog ownership is held by a different writer",
                reason="catalog_ownership_foreign_writer",
                identity=identity,
                observed={
                    "owner_fingerprint": row[0] if row else None,
                    "schema_fingerprint": row[1] if row else None,
                },
                fix_hint=(
                    "use an isolated worktree DB, or repair the shared DB "
                    "ownership before writing"
                ),
            )
        return {"rekeyed": True, **audit}


def legacy_catalog_fingerprint_for(composed: ComposedCatalog) -> str:
    """Return the pre-promotion content hash for in-place release migration."""
    payload = {
        "deployment_name": composed.deployment_name,
        **_catalog_content_payload(
            composed,
            normalize_environment_origins=False,
        ),
    }
    return sha256_text(canonical_json(payload))


def environment_bound_catalog_fingerprint_for(composed: ComposedCatalog) -> str:
    """Return the transitional hash with unnormalized environment origins."""
    payload = _catalog_content_payload(
        composed,
        normalize_environment_origins=False,
    )
    return sha256_text(canonical_json(payload))


def ensure_catalog_ownership_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            ALTER TABLE okg.catalog_versions
              ADD COLUMN IF NOT EXISTS deployment_name TEXT
            """
        )
        cur.execute(
            """
            ALTER TABLE okg.catalog_versions
              ADD COLUMN IF NOT EXISTS catalog_fingerprint TEXT
            """
        )
        cur.execute(
            """
            ALTER TABLE okg.catalog_versions
              ADD COLUMN IF NOT EXISTS schema_fingerprint TEXT
            """
        )
        cur.execute(
            """
            ALTER TABLE okg.catalog_versions
              ADD COLUMN IF NOT EXISTS writer_fingerprint TEXT
            """
        )
        cur.execute(
            """
            ALTER TABLE okg.catalog_versions
              ADD COLUMN IF NOT EXISTS writer_provenance JSONB
                NOT NULL DEFAULT '{}'::jsonb
            """
        )
        cur.execute(
            """
            ALTER TABLE okg.catalog_versions
              ADD COLUMN IF NOT EXISTS created_by_command TEXT
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS
              catalog_versions_deployment_fingerprint_idx
              ON okg.catalog_versions
              (deployment_name, status, catalog_fingerprint,
               catalog_version_id DESC)
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS okg.deployment_write_ownership (
              deployment_name TEXT PRIMARY KEY,
              owner_fingerprint TEXT NOT NULL,
              schema_fingerprint TEXT NOT NULL,
              catalog_fingerprint TEXT NOT NULL,
              deployment_root_hash TEXT,
              worktree_root_hash TEXT,
              status TEXT NOT NULL DEFAULT 'active'
                CHECK (status IN ('active','released')),
              claimed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              claimed_by_command TEXT,
              last_seen_catalog_version_id BIGINT,
              -- #709 bind-deployment-identity: manifest content-hash over
              -- the publisher-stack + source-sync input closure. NULL is a
              -- positively-established "claim predates this feature" state
              -- that warns + doctor-flags at publish and is backfilled only
              -- by claim-family commands (ruling R4); per-block hashes live
              -- in metadata->'manifest_block_hashes'.
              manifest_fingerprint TEXT,
              metadata JSONB NOT NULL DEFAULT '{}'::jsonb
            )
            """
        )
        # Additive migration for ownership rows that predate #709: the
        # column is NULL for existing claims (ruling R4). Gated on an
        # information_schema probe so a fresh table (whose CREATE already
        # carries the column) does not issue an ACCESS EXCLUSIVE ALTER on
        # every call -- unconditional DDL churn deadlocks on pg_class under
        # the shared-DB test load.
        cur.execute(
            """
            SELECT 1 FROM information_schema.columns
             WHERE table_schema = 'okg'
               AND table_name = 'deployment_write_ownership'
               AND column_name = 'manifest_fingerprint'
            """
        )
        if cur.fetchone() is None:
            cur.execute(
                """
                ALTER TABLE okg.deployment_write_ownership
                  ADD COLUMN IF NOT EXISTS manifest_fingerprint TEXT
                """
            )


def diagnose_catalog_ownership(
    conn: psycopg.Connection,
    *,
    identity: CatalogIdentity,
    limit: int = 12,
) -> dict[str, Any]:
    ensure_catalog_ownership_schema(conn)
    live_host, live_port = _live_host_port(conn.info)
    with conn.cursor() as cur:
        owner = _load_owner(cur, identity.deployment_name)
        cur.execute(
            """
            WITH recent AS (
              SELECT catalog_version_id, status, created_at, applied_at,
                     deployment_name, catalog_fingerprint,
                     schema_fingerprint, writer_fingerprint,
                     writer_provenance, created_by_command
                FROM okg.catalog_versions
               WHERE status = 'applied'
                 AND (deployment_name = %s OR deployment_name IS NULL)
               ORDER BY catalog_version_id DESC
               LIMIT %s
            ),
            narrowing_counts AS (
              SELECT r.catalog_version_id,
                     count(n.*)::bigint AS narrowing_count,
                     count(*) FILTER (
                       WHERE n.edge_type = 'view_cites_node'
                     )::bigint AS view_cites_count,
                     md5(coalesce(string_agg(
                       n.parent_subtype || '|' || n.edge_type || '|' ||
                       n.child_subtype, ','
                       ORDER BY n.parent_subtype, n.edge_type,
                                n.child_subtype
                     ), '')) AS narrowing_fingerprint
                FROM recent r
                LEFT JOIN okg.narrowings n
                  ON n.catalog_version_id = r.catalog_version_id
               GROUP BY r.catalog_version_id
            )
            SELECT r.catalog_version_id, r.status, r.created_at,
                   r.applied_at, r.deployment_name,
                   r.catalog_fingerprint, r.schema_fingerprint,
                   r.writer_fingerprint, r.writer_provenance,
                   r.created_by_command, nc.narrowing_count,
                   nc.view_cites_count, nc.narrowing_fingerprint
              FROM recent r
              JOIN narrowing_counts nc USING (catalog_version_id)
             ORDER BY r.catalog_version_id DESC
            """,
            (identity.deployment_name, limit),
        )
        rows = [_catalog_row_dict(r) for r in cur.fetchall()]

    applied_shapes = {
        r["narrowing_fingerprint"] for r in rows
        if int(r["narrowing_count"] or 0) > 0
    }
    latest = rows[0] if rows else None
    latest_compatible = bool(
        latest
        and latest.get("catalog_fingerprint")
        == identity.catalog_fingerprint
        and latest.get("writer_fingerprint") in _applied_writer_keys(identity)
    )
    has_legacy = any(not r.get("writer_fingerprint") for r in rows)
    mixed_recent_shapes = len(applied_shapes) > 1
    # #2229: the owner is this writer when its key is the v2 key or the v1
    # key rebuilt from the row's own schema. A schema difference is not a
    # different writer; it surfaces below as `latest_incompatible`.
    owner_key = _owner_key_match(identity, owner)
    owner_matches = owner_key is not None
    status = "ok"
    issue = None
    if owner is None and mixed_recent_shapes:
        status = "unclaimed_mixed_history"
        issue = (
            "recent applied catalog versions have multiple narrowing "
            "fingerprints and no ownership claim"
        )
    elif owner is not None and not owner_matches:
        status = "owner_mismatch"
        issue = "active ownership claim does not match this checkout"
    elif owner is not None and not latest_compatible:
        status = "latest_incompatible"
        issue = (
            "latest applied catalog does not match this checkout's catalog "
            "(catalog load needed) or was not written by this checkout"
        )
    elif has_legacy:
        status = "legacy_history"
        issue = "recent catalog rows predate ownership provenance"

    report = {
        "ok": status in {"ok", "legacy_history"},
        "status": status,
        "issue": issue,
        "deployment_name": identity.deployment_name,
        "expected": _identity_summary(identity),
        "owner": owner,
        "recent_catalog_versions": rows,
        "mixed_recent_shapes": mixed_recent_shapes,
        "legacy_rows_present": has_legacy,
        "latest_compatible": latest_compatible,
        # "legacy": this writer's pre-#2229 key; the next write re-keys it.
        "owner_key": owner_key,
        # The isolated-DSN recommendation targets the Postgres this
        # connection actually uses, not the hardcoded default (#825).
        "recommended_commands": _recommended_commands(
            identity, status, host=live_host, port=live_port,
        ),
    }
    # Side-by-side scope comparison (#1303). `owner_mismatch` alone never
    # said WHY two writer fingerprints differ; these two blocks let an
    # operator read "environment vs deployment_root" off the diagnosis
    # instead of hand-querying writer_provenance.
    report["writer_scope"] = {
        "incoming": {
            "writer_scope": (identity.provenance or {}).get("writer_scope"),
            "environment_id": (identity.provenance or {}).get("environment_id"),
            "source": (identity.provenance or {}).get("writer_scope_source"),
            "writer_fingerprint": identity.writer_fingerprint,
        },
        "recorded": writer_scope_baseline(report),
        "descriptor_environment_ids": list(
            (identity.provenance or {}).get("descriptor_environment_ids") or ()
        ),
    }
    # Reported, never used to flip `ok`. A scope difference that matters
    # already shows up as `owner_mismatch` (the fingerprint is derived
    # from the scope), and readiness gates read `ok`; a diagnostic must
    # not start failing healthy legacy deployments whose claims simply
    # predate `writer_scope`.
    report["writer_scope"]["problem"] = writer_scope_claim_problem(
        identity=identity, report=report,
    )
    return report


OWNER_STALE_TTL_HOURS = 72

# IOPlatformUUID shape ("XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX").
_UUID_RE = re.compile(
    r"[0-9A-Fa-f]{8}(-[0-9A-Fa-f]{4}){3}-[0-9A-Fa-f]{12}"
)

# Success-only cache for the local stable machine id (#825). A failed
# read is retried on the next call, never cached — caching a transient
# failure would silently pin the process to the hostname fallback.
_machine_id_cache: str | None = None
_machine_id_warned = False


def _read_raw_machine_id() -> str | None:
    """Raw stable machine identifier, or None when unreadable.

    Linux: /etc/machine-id (systemd) or /var/lib/dbus/machine-id.
    macOS: IOPlatformUUID via absolute /usr/sbin/ioreg — ioreg is NOT on
    the /usr/bin:/bin PATH that _process_start_time pins, so a bare
    "ioreg" would fail everywhere and silently disable the fix on the
    one platform with the roaming-hostname bug.

    Caveat: /etc/machine-id is duplicated across cloned VMs and baked
    into some container images; empty or "uninitialized" values are
    treated as unreadable so a known-dummy id never claims same-host.
    """
    for path in ("/etc/machine-id", "/var/lib/dbus/machine-id"):
        try:
            value = Path(path).read_text(encoding="utf-8").strip()
        except OSError:
            continue
        if value and value != "uninitialized":
            return value
    try:
        out = subprocess.run(
            ["/usr/sbin/ioreg", "-rd1", "-c", "IOPlatformExpertDevice"],
            capture_output=True, text=True, timeout=5, check=True,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return None
    for line in out.splitlines():
        if "IOPlatformUUID" in line:
            parts = line.split('"')
            if len(parts) >= 2 and _UUID_RE.fullmatch(parts[-2].strip()):
                # Shape-checked: deterministic garbage from malformed
                # ioreg output would hash equal across machines and
                # fabricate a false same-host verdict.
                return parts[-2].strip()
    return None


def _machine_id() -> str | None:
    """Hashed stable machine id for same-host tests, or None — loudly.

    The hash (never the raw hardware identifier) is what lands in claim
    provenance and shared databases. None degrades the same-host
    staleness test to the hostname comparison, which roams with the
    network (#825) — that degradation is warned once per process, never
    silent.
    """
    global _machine_id_cache, _machine_id_warned
    if _machine_id_cache is not None:
        return _machine_id_cache
    raw = _read_raw_machine_id()
    if raw is None:
        if not _machine_id_warned:
            _machine_id_warned = True
            _log.warning(
                "no stable machine id readable (/etc/machine-id, "
                "/var/lib/dbus/machine-id, IOPlatformUUID); same-host "
                "ownership staleness will key on the hostname, which "
                "can roam with the network"
            )
        return None
    _machine_id_cache = sha256_text(raw)
    return _machine_id_cache

# Slack between a pid's start time and the claim timestamp before the
# pid is judged recycled; same-host clocks, so this only absorbs the
# claim-then-fork ordering inside one process bring-up.
_PID_REUSE_SLACK_SECONDS = 60


def _process_start_time(pid: int) -> "datetime.datetime | None":
    """Local start time of ``pid``, or None when it can't be read.

    ``ps -o lstart=`` is the portable field (macOS ps has no
    ``etimes``); LC_ALL=C pins the day/month names for strptime.
    """
    import subprocess

    try:
        out = subprocess.run(
            ["ps", "-p", str(pid), "-o", "lstart="],
            capture_output=True, text=True, timeout=5, check=True,
            env={"LC_ALL": "C", "PATH": "/usr/bin:/bin"},
        ).stdout.strip()
        if not out:
            return None
        return datetime.datetime.strptime(
            out, "%a %b %d %H:%M:%S %Y"
        ).astimezone()
    except Exception:  # noqa: BLE001 - liveness probe, never crashes.
        return None


def owner_is_stale(owner: dict[str, Any]) -> tuple[bool, str | None]:
    """A write-ownership claim expires with its holder.

    standardize-deployment-authority req.deploy.isolation-lifecycle: a
    dead detached-HEAD worktree must never require a manual force-claim
    again. Definitive staleness: the holder pid on THIS host is gone,
    OR the pid is alive but started after the claim's last heartbeat —
    a recycled pid is not the holder (without this, os.kill(pid, 0)
    keeps a dead claim immortal on its own host and the TTL fallback
    never runs). Cross-host fallback: the claim is older than
    OWNER_STALE_TTL_HOURS with no later heartbeat (claims refresh
    claimed_at on every apply).

    Same-host is decided by the hashed stable machine id when both the
    claim and this process have one (#825) — the hostname roams with
    DHCP/VPN on macOS, so keying on it skipped the pid probe for a dead
    holder on the very machine probing it. The hostname comparison
    survives only as the legacy fallback when either side lacks a
    machine id. Caveat: cloned VMs / container images can duplicate
    /etc/machine-id; known-dummy values are treated as absent at read
    time (_read_raw_machine_id), so a duplicated id must be a real
    collision to misjudge same-host.
    """
    import os
    import socket

    meta = owner.get("metadata") or {}
    last = meta.get("last_seen_provenance") or meta
    hostname = last.get("hostname")
    pid = last.get("pid")
    recorded_machine = last.get("machine_id")
    local_machine = _machine_id()
    if (
        isinstance(recorded_machine, str)
        and recorded_machine.strip()
        and local_machine
    ):
        # Both sides have a stable id: definitive in both directions —
        # equal is same-host even if the hostname roamed; different is
        # cross-host even if the hostnames coincide.
        same_host = recorded_machine == local_machine
    else:
        same_host = hostname == socket.gethostname()
    if same_host and isinstance(pid, int):
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True, f"holder pid {pid} on this host is dead"
        except PermissionError:
            return False, None
        claimed_at = owner.get("claimed_at")
        started = _process_start_time(pid)
        if started is not None and isinstance(claimed_at, str):
            try:
                claimed = datetime.datetime.fromisoformat(claimed_at)
            except ValueError:
                return False, None
            if claimed.tzinfo is None:
                claimed = claimed.replace(tzinfo=datetime.timezone.utc)
            slack = datetime.timedelta(seconds=_PID_REUSE_SLACK_SECONDS)
            if started - claimed > slack:
                return True, (
                    f"holder pid {pid} on this host was started at "
                    f"{started.isoformat()}, after the claim's last "
                    f"heartbeat {claimed.isoformat()} - recycled pid, "
                    "not the holder"
                )
        return False, None
    claimed_at = owner.get("claimed_at")
    if isinstance(claimed_at, str):
        try:
            claimed = datetime.datetime.fromisoformat(claimed_at)
        except ValueError:
            return False, None
        age = datetime.datetime.now(datetime.timezone.utc) - claimed
        if age > datetime.timedelta(hours=OWNER_STALE_TTL_HOURS):
            # Report the age in the TTL's own unit (#825): "3d old
            # (TTL 72h)" invites should-have-expired-already misreads;
            # hours compare directly against the TTL.
            age_hours = age.total_seconds() / 3600.0
            return True, (
                f"claim from host {hostname!r} is {age_hours:.1f}h old "
                f"(TTL {OWNER_STALE_TTL_HOURS}h) with no heartbeat"
            )
    return False, None


def writer_scope_baseline(report: dict[str, Any]) -> dict[str, Any]:
    """Which scope this database is ALREADY written under, and per what.

    Two independent records answer it, in order of authority:

      1. the ownership row's `metadata` (the provenance of the claim that
         currently holds the database), and
      2. the most recent applied catalog version's `writer_provenance` —
         the writer of the last catalog version that published
         successfully, which is the record an operator can pull straight
         out of Postgres::

           select catalog_version_id,
                  writer_provenance->>'writer_scope',
                  writer_provenance->>'environment_id',
                  writer_fingerprint
             from okg.catalog_versions order by 1 desc;

    `source` is ``"none"`` when neither record carries a scope. That is a
    positively-established "we cannot tell", never an agreement: claims
    predating #1303 wrote no `writer_scope` at all, and reading their
    silence as "matches whatever I am about to write" is exactly the
    absence-dressed-as-agreement failure this guard exists to stop.
    """
    owner = report.get("owner") or {}
    metadata = owner.get("metadata") or {}
    scope = str(metadata.get("writer_scope") or "").strip()
    if scope:
        return {
            "writer_scope": scope,
            "environment_id": metadata.get("environment_id"),
            "writer_fingerprint": owner.get("owner_fingerprint"),
            "source": "ownership_row",
            "catalog_version_id": None,
        }
    for row in report.get("recent_catalog_versions") or []:
        provenance = row.get("writer_provenance") or {}
        row_scope = str(provenance.get("writer_scope") or "").strip()
        if row_scope:
            return {
                "writer_scope": row_scope,
                "environment_id": provenance.get("environment_id"),
                "writer_fingerprint": row.get("writer_fingerprint"),
                "source": "catalog_version",
                "catalog_version_id": row.get("catalog_version_id"),
            }
    return {
        "writer_scope": None,
        "environment_id": None,
        "writer_fingerprint": owner.get("owner_fingerprint") or None,
        "source": "none",
        "catalog_version_id": None,
    }


def _baseline_schema_fingerprint(
    report: dict[str, Any], baseline: dict[str, Any],
) -> str | None:
    """The schema stored beside the baseline's writer key."""
    if baseline.get("source") == "catalog_version":
        for row in report.get("recent_catalog_versions") or []:
            if row.get("catalog_version_id") == baseline.get("catalog_version_id"):
                return row.get("schema_fingerprint")
        return None
    return (report.get("owner") or {}).get("schema_fingerprint")


def writer_scope_claim_problem(
    *,
    identity: CatalogIdentity,
    report: dict[str, Any],
) -> dict[str, Any] | None:
    """Return the scope problem that should refuse this claim, or None.

    Pure so it can be tested without a database. Two problems:

    ``catalog_writer_scope_change``
        The database is already written under one scope and this claim
        would record a different one. Recording it makes every publish
        fail `catalog_ownership_foreign_writer` while the claim reports
        `claimed: true` and the ownership row looks healthy — the #1303
        production incident, which took hours because nothing compared
        the two scopes. A `--force` claim must NOT pass this: force is
        for taking a database away from a known other writer, not for
        re-scoping the writer you already are.

    ``catalog_writer_scope_ambiguous``
        Nothing states a scope, no prior scope is on record, and the
        deployment IS backed by an environment descriptor. Guessing
        `deployment_root` here is the guess that went wrong. Refuse and
        make the operator say which.
    """
    provenance = identity.provenance or {}
    incoming_scope = str(provenance.get("writer_scope") or "").strip() or None
    incoming_environment = provenance.get("environment_id")
    source = str(provenance.get("writer_scope_source") or "").strip()
    descriptors = tuple(provenance.get("descriptor_environment_ids") or ())
    baseline = writer_scope_baseline(report)

    # A matching writer fingerprint PROVES a matching scope: the
    # fingerprint payload is built from the scope. Checking it first
    # keeps claims that predate `writer_scope` (whose provenance records
    # no scope at all) from being refused as ambiguous when they are in
    # fact already the recorded writer.
    if (
        baseline.get("writer_fingerprint")
        and baseline["writer_fingerprint"] == identity.writer_fingerprint
    ):
        return None
    # The same proof holds for this writer's pre-#2229 (v1) key, rebuilt
    # from the schema recorded beside the key it is compared with.
    if baseline.get("writer_fingerprint") and writer_key_match(
        identity,
        baseline["writer_fingerprint"],
        schema_fingerprint=_baseline_schema_fingerprint(report, baseline),
    ):
        return None

    if baseline["source"] != "none":
        same_scope = baseline["writer_scope"] == incoming_scope
        same_environment = (
            incoming_scope != WRITER_SCOPE_ENVIRONMENT
            or baseline["environment_id"] == incoming_environment
        )
        if not (same_scope and same_environment):
            return {
                "reason": WRITER_SCOPE_CHANGE,
                "incoming_writer_scope": incoming_scope,
                "incoming_environment_id": incoming_environment,
                "incoming_writer_scope_source": source or None,
                "incoming_writer_fingerprint": identity.writer_fingerprint,
                "recorded": baseline,
                "descriptor_environment_ids": list(descriptors),
            }
        return None

    if (
        source == "default"
        and incoming_scope == WRITER_SCOPE_DEPLOYMENT_ROOT
        and descriptors
    ):
        return {
            "reason": WRITER_SCOPE_AMBIGUOUS,
            "incoming_writer_scope": incoming_scope,
            "incoming_environment_id": incoming_environment,
            "incoming_writer_scope_source": source,
            "incoming_writer_fingerprint": identity.writer_fingerprint,
            "recorded": baseline,
            "descriptor_environment_ids": list(descriptors),
        }
    return None


def _scope_problem_fix_hint(problem: dict[str, Any]) -> str:
    candidates = problem.get("descriptor_environment_ids") or []
    recorded = problem.get("recorded") or {}
    preferred = recorded.get("environment_id") or (
        candidates[0] if len(candidates) == 1 else None
    )
    if problem["reason"] == WRITER_SCOPE_CHANGE:
        if recorded.get("writer_scope") == WRITER_SCOPE_ENVIRONMENT:
            target = preferred or "<environment-id>"
            return (
                "this database is written under environment scope; rerun "
                "with `--writer-scope environment --environment-id "
                f"{target}` (or from the sealed release worker), or pass "
                "--allow-scope-change to "
                "deliberately re-scope every future publish"
            )
        return (
            "this database is written under deployment_root scope; rerun "
            "with `--writer-scope deployment_root`, or pass "
            "--allow-scope-change "
            "to deliberately re-scope every future publish"
        )
    target = preferred or "<environment-id>"
    listed = ", ".join(candidates) or "none"
    return (
        "name the writer scope: `--writer-scope environment "
        f"--environment-id {target}` if this claim writes as the environment "
        "the release worker publishes under, or `--writer-scope "
        "deployment_root` if it writes as this checkout "
        f"(environments hosting this deployment: {listed})"
    )


def claim_catalog_ownership(
    conn: psycopg.Connection,
    *,
    identity: CatalogIdentity,
    force: bool = False,
    allow_scope_change: bool = False,
) -> dict[str, Any]:
    report = diagnose_catalog_ownership(conn, identity=identity)
    # Runs BEFORE the foreign-writer check and is deliberately not gated
    # on `force`: the #1303 claim used --force, which bypassed every
    # other guard and let a deployment_root-scoped owner overwrite an
    # environment-scoped one.
    scope_problem = writer_scope_claim_problem(
        identity=identity, report=report,
    )
    if scope_problem is not None and not allow_scope_change:
        _raise_mismatch(
            "catalog ownership claim would record a writer scope that "
            "does not match how this database is written: incoming="
            f"{scope_problem['incoming_writer_scope']!r} recorded="
            f"{(scope_problem['recorded'] or {}).get('writer_scope')!r}",
            reason=scope_problem["reason"],
            identity=identity,
            observed=scope_problem,
            fix_hint=_scope_problem_fix_hint(scope_problem),
        )
    owner = report.get("owner")
    owner_key = _owner_key_match(identity, owner)
    rekey_audit: dict[str, Any] | None = None
    if owner and owner_key == "legacy":
        # This writer's own pre-#2229 key: re-key once under the row lock
        # (guarded on the old key) and carry the audit into the claim row.
        rekeyed = rekey_legacy_owner(conn, identity=identity)
        if rekeyed.get("rekeyed"):
            rekey_audit = {
                k: rekeyed[k] for k in ("from", "to", "command")
            }
        else:
            # Another process re-keyed between our unlocked read and the
            # lock. Its audit, read under the lock, is the one to keep; the
            # snapshot in `owner` predates it.
            rekey_audit = rekeyed.get("existing_rekey")
    elif owner and owner_key is None:
        stale, stale_reason = owner_is_stale(owner)
        if stale:
            # Loud auto-supersede, not a silent grab: the previous owner
            # and the staleness proof ride in the report.
            report["stale_owner_superseded"] = {
                "reason": stale_reason,
                "previous_owner": owner,
            }
        elif not force:
            _raise_mismatch(
                "catalog ownership is held by a different writer",
                reason="catalog_ownership_foreign_writer",
                identity=identity,
                observed=owner,
                fix_hint=(
                    "use an isolated DSN or rerun with the explicit "
                    "ownership claim/repair force option after review"
                ),
            )
    if (
        owner is None
        and report.get("mixed_recent_shapes")
        and not force
    ):
        _raise_mismatch(
            "catalog ownership is unclaimed but recent catalog history "
            "contains multiple incompatible shapes",
            reason="catalog_ownership_unclaimed",
            identity=identity,
            observed=report,
            fix_hint=(
                "use an isolated DSN or rerun the ownership claim with "
                "--force after confirming this checkout should own the DB"
            ),
        )

    ensure_catalog_ownership_schema(conn)
    # Record the manifest content-hash and its per-block breakdown so a
    # publish can compare identity continuity (#709). On a --force restamp
    # over a different manifest, the old fingerprint + provenance ride in
    # row metadata so the break-glass re-claim is audited (ruling R3).
    metadata: dict[str, Any] = dict(identity.provenance)
    metadata["manifest_block_hashes"] = identity.manifest_block_hashes or {}
    # The claim replaces row metadata; keep the one-time re-key audit.
    prior_rekey = rekey_audit or (
        (owner.get("metadata") or {}).get(WRITER_REKEY_METADATA_KEY)
        if owner and owner_key is not None else None
    )
    if prior_rekey:
        metadata[WRITER_REKEY_METADATA_KEY] = prior_rekey
    if scope_problem is not None:
        # Reached only via --allow-scope-change. A deliberate re-scope is
        # auditable in the row it produced, not just in a shell history.
        metadata["writer_scope_change"] = scope_problem
    prior_fp = owner.get("manifest_fingerprint") if owner else None
    if (
        prior_fp is not None
        and prior_fp != identity.manifest_fingerprint
    ):
        metadata["manifest_reclaim"] = {
            "previous_manifest_fingerprint": prior_fp,
            "new_manifest_fingerprint": identity.manifest_fingerprint,
            "previous_owner_fingerprint": owner.get("owner_fingerprint")
            if owner else None,
            "forced": bool(force),
        }
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.deployment_write_ownership
              (deployment_name, owner_fingerprint, schema_fingerprint,
               catalog_fingerprint, deployment_root_hash,
               worktree_root_hash, claimed_by_command,
               manifest_fingerprint, metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (deployment_name) DO UPDATE
              SET owner_fingerprint = EXCLUDED.owner_fingerprint,
                  schema_fingerprint = EXCLUDED.schema_fingerprint,
                  catalog_fingerprint = EXCLUDED.catalog_fingerprint,
                  deployment_root_hash = EXCLUDED.deployment_root_hash,
                  worktree_root_hash = EXCLUDED.worktree_root_hash,
                  status = 'active',
                  claimed_at = now(),
                  claimed_by_command = EXCLUDED.claimed_by_command,
                  manifest_fingerprint = EXCLUDED.manifest_fingerprint,
                  metadata = EXCLUDED.metadata
            """,
            (
                identity.deployment_name,
                identity.writer_fingerprint,
                identity.schema_fingerprint,
                identity.catalog_fingerprint,
                identity.deployment_root_hash,
                identity.worktree_root_hash,
                identity.provenance.get("command"),
                identity.manifest_fingerprint,
                Jsonb(metadata),
            ),
        )
    final = diagnose_catalog_ownership(conn, identity=identity)
    if scope_problem is not None:
        final["writer_scope_change_forced"] = scope_problem
    return final


def reclaim_same_writer_catalog_ownership(
    conn: psycopg.Connection,
    *,
    identity: CatalogIdentity,
) -> dict[str, Any]:
    """Refresh changed catalog content only for a proven same-root writer.

    Called by the sanctioned deployment-up mutation, never by a publisher.
    Lock the owner through diagnosis and claim so a concurrent takeover cannot
    turn the checked same-writer claim into a forced foreign-writer takeover.

    The reclaim covers ONTOLOGY drift only. ``claim_catalog_ownership``
    restamps ``manifest_fingerprint`` unconditionally, so a recorded
    fingerprint that differs from the live one must refuse here rather than
    be re-armed: that is the branch-flip hole ``_enforce_manifest_continuity``
    (#709/#828) exists to close, and this path is the takeover itself, so it
    is not owner-guarded the way ``claim --force`` is. A catalog-only edit is
    unaffected -- the ontology is classified OUT of the manifest fingerprint
    (``MANIFEST_FINGERPRINT_OUT``), so it leaves the recorded value equal.
    Intentional manifest changes still go through the audited
    ``claim_catalog_ownership`` / ``restamp_manifest_fingerprint`` commands.
    """
    with conn.transaction(), conn.cursor() as cur:
        # Diagnosis also ensures the schema. Acquire its table locks before
        # the owner row, matching other claim/apply transactions' lock order.
        ensure_catalog_ownership_schema(conn)
        cur.execute(
            "SELECT owner_fingerprint FROM okg.deployment_write_ownership "
            "WHERE deployment_name = %s FOR UPDATE",
            (identity.deployment_name,),
        )
        report = diagnose_catalog_ownership(conn, identity=identity)
        owner = report.get("owner")
        owner_key = _owner_key_match(identity, owner)
        if not owner:
            return {"reclaimed": False}
        if owner_key is not None:
            # #2229 review F1: this writer needs no claim to load a changed
            # catalog, so without this refusal `up` would load it, and the
            # NEXT `up` would find the catalog unchanged and restamp the
            # drifted manifest (the #709 R4 convergence path), re-arming a
            # retired manifest for publish. Before #2229 the schema change
            # made this writer look foreign and the same-root reclaim below
            # refused on the drift; keep that refusal and its reason code.
            _refuse_same_writer_manifest_drift(
                identity=identity, owner=owner, surface="okg up",
            )
        if owner_key == "current":
            return {"reclaimed": False}
        if owner_key == "legacy":
            # #2229: an ontology edit no longer changes the writer, so the
            # same writer needs no reclaim -- only its v1 key moved, once,
            # under the lock taken above.
            audit = _rekey_locked_owner(
                cur, identity=identity,
                old_fingerprint=owner["owner_fingerprint"],
            )
            if audit is None:
                _raise_mismatch(
                    "catalog ownership is held by a different writer",
                    reason="catalog_ownership_foreign_writer",
                    identity=identity,
                    observed=owner,
                    fix_hint=(
                        "use an isolated worktree DB, or repair the shared "
                        "DB ownership before writing"
                    ),
                )
            return {"reclaimed": False, "rekeyed": True, **audit}
        metadata = owner.get("metadata") or {}
        matching = (
            metadata.get("writer_scope") == WRITER_SCOPE_DEPLOYMENT_ROOT
            and identity.provenance.get("writer_scope") == WRITER_SCOPE_DEPLOYMENT_ROOT
            and bool(identity.deployment_root_hash)
            and owner.get("deployment_root_hash") == identity.deployment_root_hash
            and bool(identity.worktree_root_hash)
            and owner.get("worktree_root_hash") == identity.worktree_root_hash
            and bool(identity.provenance.get("machine_id"))
            and metadata.get("machine_id") == identity.provenance["machine_id"]
            # Manifest drift is never adopted here (see the docstring). A row
            # claimed before the column existed records None and stays
            # eligible; any recorded value must equal the live one.
            and _recorded_manifest_drift(identity, owner) is None
        )
        if not matching:
            # Terminal. Retain the existing named scope/foreign-writer
            # refusal; in particular, do not auto-adopt a stale foreign owner
            # or a drifted manifest here. The guard always raises for the
            # mismatched owner established above, and a future change that
            # made it return must not fall through into the forced claim.
            ensure_catalog_write_allowed(conn, identity=identity)
            raise AssertionError(
                "unreachable: ensure_catalog_write_allowed admitted a "
                "non-matching catalog owner during same-writer reclaim"
            )
        previous = owner.get("owner_fingerprint")
        claim_catalog_ownership(conn, identity=identity, force=True)
        return {"reclaimed": True, "previous_owner_fingerprint": previous,
                "owner_fingerprint": identity.writer_fingerprint}


def ensure_catalog_write_allowed(
    conn: psycopg.Connection,
    *,
    identity: CatalogIdentity,
) -> None:
    report = diagnose_catalog_ownership(conn, identity=identity)
    owner = report.get("owner")
    if owner is None:
        if _history_is_bootstrap_only(report):
            claim_catalog_ownership(
                conn,
                identity=identity,
                force=False,
            )
            return
        _raise_mismatch(
            "catalog ownership is unclaimed for a database with "
            "existing catalog history",
            reason="catalog_ownership_unclaimed",
            identity=identity,
            observed=report,
            fix_hint=(
                "use an isolated worktree DB, or intentionally claim/repair "
                "ownership with `okg catalog ownership claim --force`"
            ),
            )
    scope_problem = writer_scope_claim_problem(
        identity=identity,
        report=report,
    )
    if scope_problem is not None:
        _raise_mismatch(
            "catalog write authority does not match the recorded writer scope",
            reason=scope_problem["reason"],
            identity=identity,
            observed=scope_problem,
            fix_hint=_scope_problem_fix_hint(scope_problem),
        )
    owner_key = _owner_key_match(identity, owner)
    if owner_key is None:
        _raise_mismatch(
            "catalog ownership is held by a different writer",
            reason="catalog_ownership_foreign_writer",
            identity=identity,
            observed=owner,
            fix_hint=(
                "use an isolated worktree DB, or repair the shared DB "
                "ownership before writing"
            ),
        )
    # #2229 review: the write gate deliberately does NOT refuse manifest
    # drift. Guided `okg deployment add-source` saves the source definition
    # (changing the manifest), runs `catalog load --apply`, and only then
    # re-claims ownership; a drift refusal here broke that flow on every
    # claimed deployment. A load over a retired manifest is still stopped
    # downstream: the publish gate's manifest-continuity check refuses the
    # publish, and `okg up` refuses the reload in
    # `reclaim_same_writer_catalog_ownership`.
    if owner_key == "legacy":
        # This writer's own pre-#2229 key; move it once before writing.
        rekey_legacy_owner(conn, identity=identity)


def _recorded_manifest_drift(
    identity: CatalogIdentity, owner: dict[str, Any],
) -> str | None:
    """The recorded manifest fingerprint when it is set and differs from the
    live one, else None. A row claimed before the column existed records
    None and is not drift; nor is a recorded value equal to the live one."""
    recorded = owner.get("manifest_fingerprint")
    if recorded in (None, identity.manifest_fingerprint):
        return None
    return str(recorded)


def _refuse_same_writer_manifest_drift(
    *,
    identity: CatalogIdentity,
    owner: dict[str, Any],
    surface: str,
) -> None:
    """Refuse a catalog write by this writer over a drifted manifest.

    Used by the same-writer reclaim (`okg up`). The write gate
    (``ensure_catalog_write_allowed``) does not call it: guided add-source
    loads a changed manifest before re-claiming (#2229 review). Callers
    invoke it only once the owner key is proven to be this writer's
    (current or legacy); a foreign owner keeps its own refusal.
    """
    recorded = _recorded_manifest_drift(identity, owner)
    if recorded is None:
        return
    _raise_mismatch(
        "deployment manifest content differs from the one recorded by the "
        "catalog ownership claim (a branch flip or manifest edit); "
        f"{surface} will not load a catalog over it. "
        f"recorded={recorded} live={identity.manifest_fingerprint}",
        reason="catalog_ownership_foreign_writer",
        identity=identity,
        observed=owner,
        fix_hint=(
            "restore the checkout whose manifest the claim records, or, if "
            "the manifest change is intentional, re-claim ownership with "
            f"`{okg_prefix()} catalog ownership claim --deployment <dir> --json` "
            f"and rerun {surface}"
        ),
    )


def _validate_publish_identity(
    identity: CatalogIdentity,
    *,
    deployment_name: str | None,
) -> None:
    """Refuse forged, legacy, or cross-deployment publish authority.

    A non-null identity is not enough: direct ``Publisher`` callers can
    construct the dataclass themselves. Validate the explicit authority
    matrix before reading ownership or catalog state so a matching stored
    fingerprint cannot turn missing provenance into implicit permission.
    """
    provenance = identity.provenance or {}
    identity_deployment = str(identity.deployment_name or "").strip()
    requested_deployment = str(deployment_name or "").strip() or None
    provenance_deployment = str(
        provenance.get("deployment_name") or ""
    ).strip() or None
    if (
        not identity_deployment
        or (
            requested_deployment is not None
            and requested_deployment != identity_deployment
        )
        or provenance_deployment != identity_deployment
    ):
        raise _writer_scope_error(
            "catalog writer authority belongs to a different deployment",
            reason_code=WRITER_SCOPE_CONFLICT,
            data={
                "deployment_name": requested_deployment,
                "identity_deployment_name": identity_deployment or None,
                "provenance_deployment_name": provenance_deployment,
            },
            fix_hint=(
                "build catalog identity from the same resolved deployment "
                "passed to the publisher"
            ),
        )

    writer_scope = str(provenance.get("writer_scope") or "").strip()
    scope_source = str(
        provenance.get("writer_scope_source") or ""
    ).strip()
    environment_id = str(
        provenance.get("environment_id") or ""
    ).strip()
    if not writer_scope or scope_source != "explicit":
        raise _writer_scope_error(
            "publish requires explicit catalog writer authority",
            reason_code="catalog_writer_scope_required",
            data={
                "deployment_name": requested_deployment,
                "identity_deployment_name": identity_deployment,
                "writer_scope": writer_scope or None,
                "writer_scope_source": scope_source or None,
            },
            fix_hint=(
                "construct the publisher through build_deployment_publisher, "
                "or pass a CatalogIdentity whose writer scope was resolved "
                "explicitly"
            ),
        )
    if writer_scope not in {
        WRITER_SCOPE_DEPLOYMENT_ROOT,
        WRITER_SCOPE_ENVIRONMENT,
    }:
        raise _writer_scope_error(
            f"unsupported catalog writer scope: {writer_scope!r}",
            reason_code="catalog_writer_scope_invalid",
            data={
                "deployment_name": requested_deployment,
                "writer_scope": writer_scope,
            },
            fix_hint=(
                "select writer_scope='deployment_root' or 'environment'"
            ),
        )
    if writer_scope == WRITER_SCOPE_ENVIRONMENT and not environment_id:
        raise _writer_scope_error(
            "environment-scoped catalog identity requires an explicit "
            "environment id",
            reason_code="catalog_environment_identity_required",
            data={"deployment_name": requested_deployment},
            fix_hint="resolve and pass the selected environment id explicitly",
        )
    if writer_scope == WRITER_SCOPE_DEPLOYMENT_ROOT and environment_id:
        raise _writer_scope_error(
            "deployment-root catalog identity cannot carry an environment id",
            reason_code=WRITER_SCOPE_AMBIGUOUS,
            data={
                "deployment_name": requested_deployment,
                "environment_id": environment_id,
            },
            fix_hint="select one writer scope and identity",
        )


def resolve_catalog_for_publish(
    conn: psycopg.Connection,
    *,
    identity: CatalogIdentity | None,
    deployment_name: str | None = None,
) -> int:
    if identity is None:
        raise _writer_scope_error(
            "publish requires explicit catalog writer authority",
            reason_code="catalog_writer_scope_required",
            data={"deployment_name": deployment_name},
            fix_hint=(
                "construct the publisher through build_deployment_publisher, "
                "or pass a CatalogIdentity whose writer scope was resolved "
                "explicitly"
            ),
        )

    _validate_publish_identity(
        identity,
        deployment_name=deployment_name,
    )

    report = diagnose_catalog_ownership(conn, identity=identity)
    owner = report.get("owner")
    if owner is None:
        _raise_mismatch(
            "catalog ownership is unclaimed before publish",
            reason="catalog_ownership_unclaimed",
            identity=identity,
            observed=report,
            fix_hint=(
                "run `okg catalog ownership diagnose`, then use an "
                "isolated DSN or explicitly claim/repair ownership"
            ),
        )
    scope_problem = writer_scope_claim_problem(
        identity=identity,
        report=report,
    )
    if scope_problem is not None:
        _raise_mismatch(
            "publish authority does not match the recorded writer scope",
            reason=scope_problem["reason"],
            identity=identity,
            observed=scope_problem,
            fix_hint=_scope_problem_fix_hint(scope_problem),
        )
    # A legacy (v1) key of this writer is accepted without writing; the
    # next catalog load or claim re-keys it.
    if _owner_key_match(identity, owner) is None:
        _raise_mismatch(
            "catalog ownership is held by a different writer",
            reason="catalog_ownership_foreign_writer",
            identity=identity,
            observed=owner,
            fix_hint="use an isolated DSN or repair ownership before publish",
        )

    # #709 manifest-identity continuity (ruling R2/R3/R5). The fingerprint
    # on `identity` was freshly computed from the config THIS process
    # loaded; the owner row's is from claim time. They must agree, else a
    # branch flip to a retired manifest (same name, same path, same
    # writer) would publish silently.
    _enforce_manifest_continuity(identity=identity, owner=owner)

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT catalog_version_id
              FROM okg.catalog_versions
             WHERE status = 'applied'
               AND deployment_name = %s
               AND catalog_fingerprint = %s
               AND writer_fingerprint = ANY(%s)
             ORDER BY catalog_version_id DESC
             LIMIT 1
            """,
            (
                identity.deployment_name,
                identity.catalog_fingerprint,
                # v2 key, or the v1 key built from this process's own
                # schema: versions applied before #2229 stay publishable.
                _applied_writer_keys(identity),
            ),
        )
        row = cur.fetchone()
        compatible_cv = row[0] if row else None
        cur.execute(
            """
            SELECT catalog_version_id, deployment_name,
                   catalog_fingerprint, writer_fingerprint
             FROM okg.catalog_versions
             WHERE status = 'applied'
               AND (deployment_name = %s OR deployment_name IS NULL)
             ORDER BY catalog_version_id DESC
             LIMIT 1
            """,
            (identity.deployment_name,),
        )
        latest = cur.fetchone()

    if compatible_cv is None:
        _raise_mismatch(
            "no applied catalog version matches this checkout",
            reason="catalog_applied_version_absent",
            identity=identity,
            observed=report,
            fix_hint=(
                "run `okg catalog load --apply` from this checkout "
                "after claiming ownership"
            ),
        )
    latest_cv = int(latest[0]) if latest else None
    if latest_cv != int(compatible_cv):
        _raise_mismatch(
            "latest applied catalog version is incompatible with this checkout",
            reason="catalog_applied_version_incompatible",
            identity=identity,
            observed={
                "latest_catalog_version_id": latest_cv,
                "latest_deployment_name": latest[1] if latest else None,
                "latest_catalog_fingerprint": latest[2] if latest else None,
                "latest_writer_fingerprint": latest[3] if latest else None,
                "compatible_catalog_version_id": int(compatible_cv),
                "claimed_at": owner.get("claimed_at"),
                "claimed_by_command": owner.get("claimed_by_command"),
            },
            fix_hint=(
                "stop the other writer or switch to an isolated DSN; "
                "reload this checkout's catalog only after resolving ownership"
            ),
        )
    return int(compatible_cv)


def mark_catalog_version_seen(
    conn: psycopg.Connection,
    *,
    identity: CatalogIdentity,
    catalog_version_id: int,
) -> None:
    """Record an owned apply on the ownership row (#709 R4, hardened #828).

    Two guards close the paths that could silently poison or disarm an
    armed row:

    * Owner-scoped: a caller whose writer fingerprint does not match the
      row's owner raises instead of restamping catalog/schema fingerprints
      on a row it does not own. No row at all stays permissive -- that is
      the legacy/fresh-DB state where ``ensure_catalog_write_allowed``
      deliberately claimed nothing.
    * Manifest fingerprint is BACKFILL-ONLY: it stamps rows that predate
      #709 (NULL) and is otherwise left exactly as claimed. The previous
      ``coalesce(live, recorded)`` overwrote the recorded hash on every
      owned apply, so `okg catalog load --apply` from a flipped branch
      re-armed the row with the drifted manifest and the next publish
      sailed through -- the exact hole `_enforce_manifest_continuity`
      exists to close. Intentional manifest changes go through
      ``claim_catalog_ownership`` / ``restamp_manifest_fingerprint``,
      which are owner-guarded and audited.
    """
    ensure_catalog_ownership_schema(conn)
    # One transaction so the FOR UPDATE row lock spans the owner check and
    # the restamp (the caller may hand us an autocommit connection).
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            """
            SELECT owner_fingerprint, manifest_fingerprint,
                   claimed_at, claimed_by_command, schema_fingerprint
              FROM okg.deployment_write_ownership
             WHERE deployment_name = %s
               FOR UPDATE
            """,
            (identity.deployment_name,),
        )
        row = cur.fetchone()
        if row is None:
            return
        owner_fp, recorded_manifest_fp = row[0], row[1]
        owner_key = writer_key_match(
            identity, owner_fp, schema_fingerprint=row[4],
        )
        if owner_key is None:
            _raise_mismatch(
                "catalog apply cannot restamp an ownership row held by a "
                "different writer",
                identity=identity,
                observed={
                    "owner_fingerprint": owner_fp,
                    "claimed_at": row[2].isoformat() if row[2] else None,
                    "claimed_by_command": row[3],
                },
                fix_hint=(
                    "use an isolated worktree DB, or repair ownership with "
                    "`okg catalog ownership claim` from the owning checkout"
                ),
            )
        backfill_manifest = recorded_manifest_fp is None
        metadata: dict[str, Any] = {
            "last_seen_provenance": identity.provenance,
        }
        if backfill_manifest:
            # Block hashes ride with the fingerprint they describe; never
            # overwrite the claim-time breakdown with live blocks while
            # keeping the claim-time fold (that would corrupt
            # drifted-block reporting).
            metadata["manifest_block_hashes"] = (
                identity.manifest_block_hashes or {}
            )
        if owner_key == "legacy":
            # #2229: this writer's pre-#2229 key moves to the v2 key once,
            # in the same guarded update, with its audit entry.
            metadata[WRITER_REKEY_METADATA_KEY] = _rekey_audit(
                identity, owner_fp,
            )
        cur.execute(
            """
            UPDATE okg.deployment_write_ownership
               SET last_seen_catalog_version_id = %s,
                   owner_fingerprint = %s,
                   catalog_fingerprint = %s,
                   schema_fingerprint = %s,
                   manifest_fingerprint = coalesce(
                     manifest_fingerprint, %s
                   ),
                   metadata = metadata || %s::jsonb
             WHERE deployment_name = %s
               AND owner_fingerprint = %s
            """,
            (
                catalog_version_id,
                identity.writer_fingerprint,
                identity.catalog_fingerprint,
                identity.schema_fingerprint,
                identity.manifest_fingerprint,
                Jsonb(metadata),
                identity.deployment_name,
                owner_fp,
            ),
        )


def restamp_manifest_fingerprint(
    conn: psycopg.Connection,
    *,
    identity: CatalogIdentity,
) -> dict[str, Any]:
    """Refresh an owning writer's recorded manifest fingerprint (#709 R4).

    ``okg deployment up`` skips catalog apply when the ontology fingerprint
    is unchanged (deployment_upgrade reconcile-on-change), so an
    enricher-only manifest edit would otherwise never restamp the ownership
    row and every subsequent publish would refuse. This claim-family
    restamp runs on that path: it updates ``manifest_fingerprint`` (and its
    per-block breakdown) only for the writer that already owns the row, so
    it is safe and idempotent. No-op (``{"restamped": False}``) when there
    is no row or a different writer owns it -- restamping a foreign claim is
    the caller's explicit ``claim --force`` job, not this convergence step.
    """
    ensure_catalog_ownership_schema(conn)
    with conn.cursor() as cur:
        owner = _load_owner(cur, identity.deployment_name)
    if owner is None:
        return {"restamped": False, "reason": "no ownership row"}
    owner_key = _owner_key_match(identity, owner)
    if owner_key is None:
        return {"restamped": False, "reason": "held by a different writer"}
    if owner_key == "legacy":
        # Claim-family write for this writer: move its pre-#2229 key first
        # so the owner-guarded update below matches the v2 key.
        rekey_legacy_owner(conn, identity=identity)
    prior_fp = owner.get("manifest_fingerprint")
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE okg.deployment_write_ownership
               SET manifest_fingerprint = %s,
                   metadata = metadata || %s::jsonb
             WHERE deployment_name = %s
               AND owner_fingerprint = %s
            """,
            (
                identity.manifest_fingerprint,
                Jsonb({
                    # The restamp is a claim-equivalent for the fingerprint,
                    # so record the provenance that produced it alongside;
                    # a later drift refusal reports these as the "recorded"
                    # commit (#1791).
                    **{
                        key: (identity.provenance or {}).get(key)
                        for key in ("git_commit", "git_branch", "deployment_root")
                    },
                    "manifest_block_hashes": identity.manifest_block_hashes
                    or {},
                }),
                identity.deployment_name,
                identity.writer_fingerprint,
            ),
        )
    return {
        "restamped": True,
        "previous_manifest_fingerprint": prior_fp,
        "manifest_fingerprint": identity.manifest_fingerprint,
        "changed": prior_fp != identity.manifest_fingerprint,
    }


def suggested_worktree_dsn(
    *,
    deployment_name: str,
    start: Path | None = None,
    host: str = "127.0.0.1",
    port: int = 5433,
) -> str:
    root = _find_repo_root(start or Path.cwd()) or (start or Path.cwd())
    digest = hashlib.sha256(str(root.resolve()).encode("utf-8")).hexdigest()
    suffix = digest[:10]
    safe_name = "".join(
        ch if ch.isalnum() else "_"
        for ch in deployment_name.replace("-", "_")
    )
    return f"postgres://postgres:okg@{host}:{port}/{safe_name}_{suffix}"


def _catalog_content_payload(
    composed: ComposedCatalog,
    *,
    normalize_environment_origins: bool = True,
) -> dict[str, Any]:
    def _invariant_payload(inv: Any) -> Any:
        if hasattr(inv, "__dict__"):
            payload = dict(inv.__dict__)
            # Issue #3624: an invariant without requires_sources hashes as it
            # did before the field existed, so upgrading okg does not change
            # every deployment's catalog fingerprint (which would refuse
            # publishes until the catalog is reloaded).
            if payload.get("requires_sources") is None:
                payload.pop("requires_sources", None)
            return payload
        return str(inv)

    def _origin(value: str | None) -> str | None:
        if (
            normalize_environment_origins
            and value == f"deployment:{composed.deployment_name}"
        ):
            return "deployment"
        return value

    return {
        "modules": list(composed.modules),
        "subtypes": [
            {
                "name": name,
                "parent": composed.subtype_parent.get(name),
                "metadata": composed.subtype_metadata.get(name, {}),
                "origin": _origin(composed.subtype_origin.get(name)),
            }
            for name in sorted(composed.subtypes)
        ],
        "narrowings": sorted(
            {
                (
                    n.parent_subtype,
                    n.edge_archetype,
                    n.child_subtype,
                    _origin(n.origin),
                )
                for n in composed.narrowings
            }
        ),
        "aliases": sorted(composed.aliases, key=canonical_json),
        "identifier_patterns": sorted(
            composed.identifier_patterns,
            key=canonical_json,
        ),
        "concept_seeds": sorted(composed.concept_seeds, key=canonical_json),
        "invariants": sorted(
            (_invariant_payload(inv) for inv in composed.invariants),
            key=canonical_json,
        ),
        "archetype_kind_overrides": composed.archetype_kind_overrides,
    }


def _load_owner(cur: psycopg.Cursor, deployment_name: str) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT deployment_name, owner_fingerprint, schema_fingerprint,
               catalog_fingerprint, deployment_root_hash,
               worktree_root_hash, status, claimed_at,
               claimed_by_command, last_seen_catalog_version_id, metadata,
               manifest_fingerprint
          FROM okg.deployment_write_ownership
         WHERE deployment_name = %s
        """,
        (deployment_name,),
    )
    row = cur.fetchone()
    if row is None:
        return None
    return {
        "deployment_name": row[0],
        "owner_fingerprint": row[1],
        "schema_fingerprint": row[2],
        "catalog_fingerprint": row[3],
        "deployment_root_hash": row[4],
        "worktree_root_hash": row[5],
        "status": row[6],
        "claimed_at": row[7].isoformat() if row[7] else None,
        "claimed_by_command": row[8],
        "last_seen_catalog_version_id": row[9],
        "metadata": row[10] or {},
        "manifest_fingerprint": row[11],
    }


def _catalog_row_dict(row: tuple[Any, ...]) -> dict[str, Any]:
    return {
        "catalog_version_id": int(row[0]),
        "status": row[1],
        "created_at": row[2].isoformat() if row[2] else None,
        "applied_at": row[3].isoformat() if row[3] else None,
        "deployment_name": row[4],
        "catalog_fingerprint": row[5],
        "schema_fingerprint": row[6],
        "writer_fingerprint": row[7],
        "writer_provenance": row[8] or {},
        "created_by_command": row[9],
        "narrowing_count": int(row[10] or 0),
        "view_cites_count": int(row[11] or 0),
        "narrowing_fingerprint": row[12],
    }


def _identity_summary(identity: CatalogIdentity) -> dict[str, Any]:
    return {
        "deployment_name": identity.deployment_name,
        "catalog_fingerprint": identity.catalog_fingerprint,
        "schema_fingerprint": identity.schema_fingerprint,
        "writer_fingerprint": identity.writer_fingerprint,
        "deployment_root_hash": identity.deployment_root_hash,
        "worktree_root_hash": identity.worktree_root_hash,
        "manifest_fingerprint": identity.manifest_fingerprint,
        "provenance": identity.provenance,
    }


def _history_is_bootstrap_only(report: dict[str, Any]) -> bool:
    rows = report.get("recent_catalog_versions") or []
    if not rows:
        return True
    nonempty = [
        r for r in rows
        if int(r.get("narrowing_count") or 0) > 0
    ]
    return not nonempty


_log = logging.getLogger("okg.catalog.ownership")

# The one-command backfill named in the legacy-row warning and doctor flag.
MANIFEST_BACKFILL_COMMAND = (
    f"{okg_prefix()} catalog ownership claim --deployment "
    "deployments/{name} --json"
)


def _drifted_manifest_blocks(
    identity: CatalogIdentity,
    owner: dict[str, Any],
) -> list[str]:
    """Names of behavior-bearing blocks that differ, for the refusal msg."""

    current = identity.manifest_block_hashes or {}
    recorded = (owner.get("metadata") or {}).get("manifest_block_hashes") or {}
    names = set(current) | set(recorded)
    return sorted(
        name for name in names if current.get(name) != recorded.get(name)
    )


def _enforce_manifest_continuity(
    *,
    identity: CatalogIdentity,
    owner: dict[str, Any],
) -> None:
    """Refuse a publish whose live manifest drifted from the claim (#709).

    Postures (rulings R2/R3/R4):
      * recorded fingerprint is NULL -> legacy row: warn loudly (naming the
        backfill command) and proceed. The one warn state.
      * recorded fingerprint present but live is None -> compute failure /
        legacy caller against a stamped row: refuse (never a warn).
      * fingerprints differ -> identity break: refuse with a typed
        CatalogOwnershipMismatch naming the drifted block(s) and the
        recorded-vs-current git diagnostics.
      * equal -> publish.
    """

    recorded_fp = owner.get("manifest_fingerprint")
    live_fp = identity.manifest_fingerprint

    if recorded_fp is None:
        _log.warning(
            "catalog ownership row for %s predates manifest-identity "
            "binding; publishing with an UNVERIFIED manifest. "
            "Backfill with: %s",
            identity.deployment_name,
            MANIFEST_BACKFILL_COMMAND.format(name=identity.deployment_name),
        )
        return

    if live_fp is None:
        _raise_mismatch(
            "publish could not compute a manifest identity but the "
            "ownership row is armed with a manifest fingerprint",
            identity=identity,
            observed={
                "recorded_manifest_fingerprint": recorded_fp,
                "reason": "no manifest fingerprint on the live identity",
                "claimed_at": owner.get("claimed_at"),
                "claimed_by_command": owner.get("claimed_by_command"),
            },
            fix_hint=(
                "publish from a checkout whose deployment.yaml is readable, "
                "or re-claim ownership from this checkout"
            ),
        )

    if live_fp != recorded_fp:
        prov = identity.provenance or {}
        meta = owner.get("metadata") or {}
        # Two provenances live on the row and they answer different
        # questions. The top-level keys were written by the CLAIM
        # (`claim_catalog_ownership` stores `identity.provenance` as the
        # row metadata) and describe the checkout whose manifest fingerprint
        # is recorded. `last_seen_provenance` is refreshed by every owned
        # `okg catalog load --apply` (`mark_catalog_version_seen`), which
        # leaves the fingerprint alone. Reporting the refreshed one as
        # "recorded" made the refusal print recorded == live commits while
        # the fingerprints differed, which reads as "already re-claimed"
        # and misdirects the operator (#1791).
        claimed_prov = {
            key: meta.get(key)
            for key in ("git_commit", "git_branch", "deployment_root")
        }
        last_seen_prov = meta.get("last_seen_provenance") or {}
        # Rows backfilled by `mark_catalog_version_seen` (pre-#709 claims)
        # carry no claim-time keys; fall back to the last-seen provenance
        # rather than printing None, and say which one was used.
        recorded_prov_source = "claim"
        recorded_prov = claimed_prov
        if not claimed_prov.get("git_commit"):
            recorded_prov = last_seen_prov
            recorded_prov_source = "last_seen"
        drifted = _drifted_manifest_blocks(identity, owner)
        live_dir = prov.get("deployment_root")
        # Everything an operator needs goes in the MESSAGE: DBOS and the
        # worker log persist only str(exc), and #828 was misdiagnosed for
        # hours because the fingerprints and drifted blocks lived in the
        # dropped data dict.
        _raise_mismatch(
            "deployment manifest content differs from the claimed identity "
            "(a branch flip or manifest edit changed what publishes); "
            "publish refused. "
            f"recorded={recorded_fp} live={live_fp} "
            f"drifted_blocks={drifted or ['<unknown>']} "
            f"recorded_git_commit={recorded_prov.get('git_commit')} "
            f"last_seen_git_commit={last_seen_prov.get('git_commit')} "
            f"live_git_commit={prov.get('git_commit')} "
            f"live_deployment_dir={live_dir}",
            reason="catalog_manifest_drifted",
            identity=identity,
            observed={
                "recorded_manifest_fingerprint": recorded_fp,
                "live_manifest_fingerprint": live_fp,
                "drifted_blocks": drifted,
                "recorded_git_branch": recorded_prov.get("git_branch"),
                "recorded_git_commit": recorded_prov.get("git_commit"),
                "recorded_provenance_source": recorded_prov_source,
                "last_seen_git_branch": last_seen_prov.get("git_branch"),
                "last_seen_git_commit": last_seen_prov.get("git_commit"),
                "live_git_branch": prov.get("git_branch"),
                "live_git_commit": prov.get("git_commit"),
                "live_deployment_root": live_dir,
                "claimed_at": owner.get("claimed_at"),
                "claimed_by_command": owner.get("claimed_by_command"),
            },
            fix_hint=(
                "if this manifest edit is intentional, re-claim ownership "
                "FROM THE DIRECTORY THE WORKER PUBLISHES FROM: "
                f"{okg_prefix()} catalog ownership claim --deployment "
                + (str(live_dir) if live_dir else
                   f"deployments/{identity.deployment_name}")
                + " --json (add --force to override a differing prior "
                "claim); otherwise restore the correct checkout before "
                "publishing. `okg catalog load --apply` alone does NOT "
                "refresh a drifted manifest fingerprint -- it refreshes the "
                "recorded git provenance only, so the refusal survives it"
            ),
        )


def _refusal_context(
    *,
    deployment_name: str,
    observed: dict[str, Any] | None,
) -> str:
    """Self-dating suffix appended to every ownership refusal message.

    DBOS workflow_status and worker logs persist only ``str(exc)`` -- the
    ``data`` dict is dropped -- and terminal ERROR rows replay a refusal
    long after it stopped being true. #828 was a multi-hour misdiagnosis
    of exactly such a stale row, so the message itself must carry the
    refusal time, the deployment, and the recorded claim's provenance.
    """
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    parts = [f"refused_at={stamp}", f"deployment={deployment_name}"]
    owner = observed if isinstance(observed, dict) else {}
    if "claimed_at" not in owner and isinstance(owner.get("owner"), dict):
        owner = owner["owner"]
    claimed_at = owner.get("claimed_at")
    if claimed_at is not None:
        parts.append(f"recorded_claim_at={claimed_at}")
    claimed_by = owner.get("claimed_by_command")
    if claimed_by:
        parts.append(f"recorded_claim_via={claimed_by!r}")
    return " [" + " ".join(str(p) for p in parts) + "]"


def _raise_mismatch(
    message: str,
    *,
    identity: CatalogIdentity,
    observed: dict[str, Any] | None,
    fix_hint: str,
    reason: str = "catalog_ownership_unspecified",
) -> None:
    """Refuse, naming WHICH ownership condition applies.

    `catalog_ownership_mismatch` is one error code covering at least three
    conditions that need different repairs:

      catalog_ownership_unclaimed  — nobody owns this database yet
      catalog_ownership_foreign_writer — someone else owns it (the genuine
          cross-worktree safety refusal; the one you must NOT force past
          without understanding it)
      catalog_applied_version_absent — ownership is fine, but no applied
          catalog version matches this writer
      catalog_manifest_drifted — the live deployment manifest no longer
          matches the claimed fingerprint (branch flip or manifest edit);
          re-claim ownership, since `catalog load --apply` will not clear it

    Callers that see only the shared code cannot tell a routine
    "re-apply the catalog" from "another checkout is writing to your
    production database". `reason` is the machine-readable discriminator;
    the message still carries the human sentence.
    """
    error = CatalogOwnershipMismatch(
        message
        + _refusal_context(
            deployment_name=identity.deployment_name,
            observed=observed,
        ),
        data={
            "deployment_name": identity.deployment_name,
            "reason": reason,
            "reason_code": reason,
            "writer_scope": (identity.provenance or {}).get("writer_scope"),
            "environment_id": (identity.provenance or {}).get("environment_id"),
            "expected": _identity_summary(identity),
            "observed": observed,
        },
        fix_hint=fix_hint,
    )
    # Publish results retain the actionable ownership condition at the
    # public error boundary. The exception type remains catch-compatible,
    # while callers that serialize only ``exc.code`` no longer collapse an
    # unclaimed database, a foreign writer, and an absent catalog version
    # into the misleading generic ``catalog_ownership_mismatch`` label.
    if reason != "catalog_ownership_unspecified":
        error.code = reason
    raise error


def _live_host_port(conn_info: Any) -> tuple[str, int]:
    """Host/port of the live connection, for DSN recommendations (#825).

    A unix-socket connection reports its socket directory (or nothing)
    as the host; those keep the loopback default while the port stays
    whatever the server actually listens on — recommending the
    hardcoded 5433 targeted the wrong Postgres on hosts where 5433 is a
    scratch instance.
    """
    host = getattr(conn_info, "host", None)
    if not host or str(host).startswith("/"):
        host = "127.0.0.1"
    port_raw = getattr(conn_info, "port", None)
    try:
        port = int(port_raw) if port_raw else 5433
    except (TypeError, ValueError):
        port = 5433
    return str(host), port


def _recommended_commands(
    identity: CatalogIdentity,
    status: str,
    *,
    host: str = "127.0.0.1",
    port: int = 5433,
) -> list[str]:
    provenance = identity.provenance or {}
    writer_scope = provenance.get("writer_scope")
    environment_id = provenance.get("environment_id")
    authority_args = ""
    if writer_scope == WRITER_SCOPE_ENVIRONMENT and environment_id:
        authority_args = (
            " --writer-scope environment --environment-id "
            f"{shlex.quote(str(environment_id))}"
        )
    elif writer_scope == WRITER_SCOPE_DEPLOYMENT_ROOT:
        authority_args = " --writer-scope deployment_root"
    commands = [
        f"{okg_prefix()} catalog ownership diagnose --deployment "
        f"deployments/{identity.deployment_name}{authority_args} --json",
    ]
    if status in {"unclaimed_mixed_history", "owner_mismatch"}:
        commands.append(
            f"{okg_prefix()} catalog ownership claim --deployment "
            f"deployments/{identity.deployment_name}{authority_args} "
            "--force --json",
        )
    # The suggested DSN carries a password. `recommended_commands` is JSON an
    # operator pastes into tickets and chat, so the password is stripped here
    # and the operator supplies it (#2213). `suggested_worktree_dsn` itself
    # still returns the connectable form -- deployment_isolation connects with
    # it -- so only this display path is redacted.
    commands.append(
        f"{okg_prefix()} deployment env "
        f"{identity.deployment_name} --write-local --dsn "
        + redact_dsn(
            suggested_worktree_dsn(
                deployment_name=identity.deployment_name,
                host=host,
                port=port,
            )
        )
    )
    return commands


def _git_output(args: list[str], cwd: Path) -> str | None:
    try:
        proc = subprocess.run(
            ["git", *args],
            cwd=cwd,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
    except OSError:
        return None
    if proc.returncode != 0:
        return None
    value = proc.stdout.strip()
    return value or None


def _git_dirty(cwd: Path) -> bool | None:
    out = _git_output(["status", "--short"], cwd)
    if out is None:
        return None
    return bool(out.strip())


def _find_repo_root(start: Path) -> Path | None:
    cur = start.resolve()
    for candidate in (cur, *cur.parents):
        if (candidate / ".git").exists() or (
            candidate / "pyproject.toml"
        ).is_file():
            return candidate
    return None
