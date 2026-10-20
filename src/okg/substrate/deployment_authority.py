"""Side-effect-free deployment DSN authority resolution."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

from okg.substrate import fastyaml
from okg.substrate.dsn_form import (
    is_keyword_form_dsn,
    keyword_form_refusal,
)
from okg.substrate.errors import SubstrateError


def _canon_host(host: str | None) -> str:
    """Fold benign localhost aliases to one token so they never mis-compare.

    A unix-socket / empty / loopback host (``None``, ``""``, ``localhost``,
    ``127.0.0.1``, ``::1``) all denote the same local server. Any other host
    is returned unchanged so a genuinely different host (the fork server vs
    the canonical host - the 2026-06-10 incident shape) still compares unequal.
    """
    if host is None or host in ("", "localhost", "127.0.0.1", "::1"):
        return "localhost"
    return host


#: Every value :func:`resolved_target` can put in ``identity``. This is the
#: DOMAIN, declared once, because downstream classifiers enumerate over it --
#: `publish_provenance_verdict`'s exhaustiveness proof is only exhaustive if it
#: covers every member. Adding an identity means adding it here; a test asserts
#: that no literal assigned to ``identity`` inside `resolved_target` is absent
#: from this set, so the two cannot drift apart silently.
IDENTITY_VALUES: frozenset[str] = frozenset({
    "ok",
    "isolated",
    "mismatch",
    "undeclared",
    "unresolved",
})


def resolved_target(
    dsn: str | None,
    *,
    deployment: str,
    authority_mode: str = "unknown",
    expected_database: str | None = None,
    expected_host: str | None = None,
    expected_port: int | None = None,
    manifest_readable: bool = True,
) -> dict[str, Any]:
    """Name WHICH database a DSN denotes, and prove the name is in the domain.

    Thin wrapper over :func:`_resolved_target` that checks the returned
    ``identity`` against :data:`IDENTITY_VALUES`. Downstream classifiers -- most
    sharply ``publish_provenance_verdict``, whose exhaustiveness proof
    enumerates that set -- are only exhaustive over the values it declares, so
    an identity that escapes the declaration would quietly make the proof cover
    less than it claims. Checking here means a new identity cannot be added
    without also being declared, in ANY syntactic form.
    """
    target = _resolved_target(
        dsn,
        deployment=deployment,
        authority_mode=authority_mode,
        expected_database=expected_database,
        expected_host=expected_host,
        expected_port=expected_port,
        manifest_readable=manifest_readable,
    )
    identity = target.get("identity")
    if identity not in IDENTITY_VALUES:
        raise SubstrateError(
            f"deployment authority produced identity {identity!r}, which is "
            "not in the declared domain",
            data={
                "identity": identity,
                "declared": sorted(IDENTITY_VALUES),
                "deployment": deployment,
            },
            fix_hint=(
                "add the value to deployment_authority.IDENTITY_VALUES and "
                "widen every classifier that enumerates it"
            ),
        )
    return target


def _resolved_target(
    dsn: str | None,
    *,
    deployment: str,
    authority_mode: str = "unknown",
    expected_database: str | None = None,
    expected_host: str | None = None,
    expected_port: int | None = None,
    manifest_readable: bool = True,
) -> dict[str, Any]:
    """Name WHICH database a DSN denotes - never report success blind.

    Per `standardize-deployment-authority` req.deploy.single-authority:
    every state-changing surface attaches this to its output. `identity` is
    `ok` (matches the manifest's declared database), `isolated` (declared
    name plus an isolation suffix, e.g. okg_workspace_51b8fac220), `mismatch`
    (some other database - the 2026-06-10 incident class), `undeclared`
    (manifest does not pin a database name yet), or `unresolved` (no DSN, or
    the manifest could not be read so its declared identity is unknown).

    Identity is the (host, port, database) triple, not the database name
    alone: when the manifest pins a LITERAL DSN, callers thread its
    ``expected_host`` / ``expected_port`` so a same-named database on the
    WRONG host or port (e.g. the fork server :5433 vs canonical :55434 - the
    2026-06-10 incident shape) is refused as `mismatch`, not accepted as `ok`.
    Hosts are compared after :func:`_canon_host` so a benign
    localhost/127.0.0.1/socket alias is not a false mismatch; ports compare as
    integers. When ``expected_host`` / ``expected_port`` are both ``None``
    (the interpolated-DSN deployments that declare only a database name, e.g.
    okg-workspace pinning ``${OKG_DSN}``), identity stays name-only -
    unchanged behavior.

    ``manifest_readable=False`` means the manifest file existed but failed to
    parse: we cannot trust ``expected_database is None`` to mean "operator
    declared no database", so the identity fails closed to `unresolved`
    rather than the permissive `undeclared`.
    """
    from urllib.parse import urlsplit

    if not dsn:
        return {
            "deployment": deployment,
            "authority_mode": authority_mode,
            "host": None,
            "port": None,
            "database": None,
            "identity": "unresolved",
        }
    parts = urlsplit(dsn)
    database = (parts.path or "/").lstrip("/") or None
    if not manifest_readable:
        return {
            "deployment": deployment,
            "authority_mode": authority_mode,
            "host": parts.hostname,
            "port": parts.port,
            "database": database,
            "identity": "unresolved",
        }
    if expected_database is None:
        identity = "undeclared"
    elif database == expected_database:
        identity = "ok"
        # Name matches - now check the rest of the triple. A literal-DSN
        # manifest pins an expected host/port; a same-named database on a
        # different server is the 2026-06-10 incident, not an `ok` target.
        if expected_host is not None and (
            _canon_host(expected_host) != _canon_host(parts.hostname)
        ):
            identity = "mismatch"
        elif expected_port is not None and expected_port != parts.port:
            identity = "mismatch"
    elif database and database.startswith(expected_database + "_"):
        identity = "isolated"
    else:
        identity = "mismatch"
    return {
        "deployment": deployment,
        "authority_mode": authority_mode,
        "host": parts.hostname,
        "port": parts.port,
        "database": database,
        "identity": identity,
        **(
            {"expected_database": expected_database}
            if expected_database is not None
            else {}
        ),
    }


def resolve_deployment_dsn_authority(
    deployment: str,
    *,
    explicit_dsn: str | None = None,
    local_profile_authoritative: bool = False,
) -> dict[str, Any]:
    """Resolve a deployment DSN without mutating ``os.environ``.

    Generic deployment commands still let process env satisfy manifest
    interpolation. Dogfood surfaces set ``local_profile_authoritative=True`` so
    `.okg/local.env` wins over stale agent-session process env.

    A resolved DSN written in libpq's keyword/value spelling is turned into a
    refusal envelope here rather than returned: it has no URL scheme, so
    Chronos would route it to SQLite and the publish path would write a FILE
    named after the connection string (issue #1971). Every caller of this
    function -- the read side, the MCP surfaces, the deployment commands --
    gets that refusal at resolution instead of deep inside the first write
    (issue #2001).
    """
    resolution = _resolve_deployment_dsn_authority(
        deployment,
        explicit_dsn=explicit_dsn,
        local_profile_authoritative=local_profile_authoritative,
    )
    dsn = resolution.get("dsn")
    if not isinstance(dsn, str) or not is_keyword_form_dsn(dsn):
        return resolution
    authority = dict(resolution.get("authority") or {})
    refused = dict(resolution)
    refused["dsn"] = None
    refused["reason"] = keyword_form_refusal(
        _authority_dsn_source(deployment, authority), dsn,
    )
    refused["next_action"] = "fix_deployment_dsn_form"
    refused["authority"] = {**authority, "dsn_form": "libpq_keyword"}
    # `resolved_target` parsed the refused string as a URL, so it put the
    # WHOLE keyword DSN -- password included -- in `target["database"]`.
    # That block describes a DSN we are refusing, and every field in it is
    # meaningless for one; drop it rather than try to redact inside it.
    refused["target"] = None
    return refused


#: How each authority mode reads back to the operator: what they would edit
#: to change the DSN that mode produced. Used only to phrase a refusal.
_AUTHORITY_SOURCE_LABEL = {
    "explicit": "the --dsn value",
    "sealed_environment": "OKG_DSN (the sealed environment's launcher)",
    "process_env": "OKG_DSN",
    "deployment_manifest_literal": "the manifest's literal `postgres.dsn`",
    "dogfood_runtime_record": "the dogfood runtime authority record",
    "worktree_main_local_env": (
        "OKG_DSN in the main checkout's .okg/local.env"
    ),
}


def _authority_dsn_source(deployment: str, authority: dict[str, Any]) -> str:
    """Name the setting that produced this deployment's DSN."""
    mode = str(authority.get("mode") or "")
    label = _AUTHORITY_SOURCE_LABEL.get(mode)
    if label is None:
        # The interpolating modes (`deployment_manifest`,
        # `dogfood_local_profile`): name the variables the manifest
        # references, which is what the operator actually set.
        raw, _readable = _read_manifest(deployment)
        postgres = raw.get("postgres") if isinstance(raw, dict) else None
        raw_dsn = postgres.get("dsn") if isinstance(postgres, dict) else None
        refs = _env_refs(raw_dsn) if isinstance(raw_dsn, str) else []
        label = (
            f"{', '.join(refs)} (interpolated into the manifest's "
            "`postgres.dsn`)"
            if refs
            else "the manifest's `postgres.dsn`"
        )
    manifest_path = authority.get("manifest_path")
    where = f" for deployment {deployment!r}"
    if manifest_path:
        where += f" ({manifest_path})"
    return f"{label}{where}"


def _resolve_deployment_dsn_authority(
    deployment: str,
    *,
    explicit_dsn: str | None = None,
    local_profile_authoritative: bool = False,
) -> dict[str, Any]:
    """The resolution itself; see the public wrapper above."""
    if explicit_dsn:
        from urllib.parse import urlsplit

        raw, manifest_readable = _read_manifest(deployment)
        expected_database = None
        expected_host: str | None = None
        expected_port: int | None = None
        if raw is not None:
            postgres = raw.get("postgres") if isinstance(raw, dict) else None
            declared = (
                postgres.get("database") if isinstance(postgres, dict) else None
            )
            expected_database = (
                declared if isinstance(declared, str) and declared else None
            )
            # When the manifest pins a LITERAL DSN (no ${...} refs), it
            # declares an expected host/port too: an explicit DSN on a
            # different server is the 2026-06-10 incident, refuse it. An
            # interpolated manifest declares no host/port - stay name-only.
            manifest_dsn = (
                postgres.get("dsn") if isinstance(postgres, dict) else None
            )
            if (
                isinstance(manifest_dsn, str)
                and manifest_dsn.strip()
                and not _env_refs(manifest_dsn)
            ):
                literal_parts = urlsplit(manifest_dsn.strip())
                expected_host = literal_parts.hostname
                expected_port = literal_parts.port
        target = resolved_target(
            explicit_dsn,
            deployment=deployment,
            authority_mode="explicit",
            expected_database=expected_database,
            expected_host=expected_host,
            expected_port=expected_port,
            manifest_readable=manifest_readable,
        )
        record, record_error = _dogfood_runtime_authority_record(
            deployment,
            enabled=(
                isinstance(raw, dict)
                and isinstance(raw.get("postgres"), dict)
                and raw["postgres"].get("self_hosted_authoritative") is True
            ),
        )
        if record_error:
            return {
                "dsn": None,
                "reason": record_error,
                "next_action": "refresh_dogfood_authority",
                "authority": {
                    "mode": "dogfood_runtime_record_invalid",
                    "deployment": deployment,
                },
            }
        if record is not None:
            target = resolved_target(
                explicit_dsn,
                deployment=deployment,
                authority_mode="dogfood_runtime_record",
                expected_database=expected_database,
                expected_host=str(record["host"]),
                expected_port=int(record["port"]),
            )
            if target["identity"] != "ok":
                return {
                    "dsn": None,
                    "reason": (
                        "explicit DSN does not match the active dogfood "
                        f"authority (identity={target['identity']})"
                    ),
                    "next_action": "fix_deployment_database_identity",
                    "authority": {
                        "mode": "dogfood_runtime_record",
                        "deployment": deployment,
                    },
                    "target": target,
                }
        if target["identity"] == "mismatch":
            return {
                "dsn": None,
                "reason": (
                    f"deployment {deployment} declares database "
                    f"{target['expected_database']!r} but explicit DSN "
                    f"points at {target['database']!r}"
                ),
                "next_action": "fix_deployment_database_identity",
                "authority": {
                    "mode": "explicit",
                    "deployment": deployment,
                },
                "target": target,
            }
        return {
            "dsn": explicit_dsn,
            "authority": {
                "mode": "explicit",
                "deployment": deployment,
            },
            "target": target,
        }
    from okg.substrate.deployments import find_deployment_dir

    deployment_dir = find_deployment_dir(deployment)
    if deployment_dir is None:
        return {"dsn": None}
    manifest_path = deployment_dir / "deployment.yaml"
    if not manifest_path.is_file():
        return {"dsn": None}
    try:
        raw = fastyaml.safe_load(manifest_path.read_text(encoding="utf-8")) or {}
    except Exception as exc:  # noqa: BLE001 - callers surface status envelopes.
        return {
            "dsn": None,
            "reason": str(exc),
            "next_action": "fix_deployment_manifest",
            "authority": {
                "mode": "deployment_manifest_unreadable",
                "deployment": deployment,
                "manifest_path": str(manifest_path),
            },
        }
    postgres = raw.get("postgres") if isinstance(raw, dict) else {}
    raw_dsn = postgres.get("dsn") if isinstance(postgres, dict) else None
    if not isinstance(raw_dsn, str) or not raw_dsn.strip():
        return {"dsn": None}
    refs = _env_refs(raw_dsn)
    if not refs:
        expected_database = (
            postgres.get("database") if isinstance(postgres, dict) else None
        )
        # This branch resolves the manifest's OWN literal DSN, so host/port
        # are trivially self-consistent - no expected_host/port to thread.
        # The candidate-vs-manifest host/port refusal happens in the
        # explicit_dsn branch above (an external DSN compared to the literal).
        target = resolved_target(
            raw_dsn.strip(),
            deployment=deployment,
            authority_mode="deployment_manifest_literal",
            expected_database=(
                expected_database if isinstance(expected_database, str) else None
            ),
        )
        if target["identity"] == "mismatch":
            return {
                "dsn": None,
                "reason": (
                    f"deployment {deployment} declares database "
                    f"{target['expected_database']!r} but the manifest DSN "
                    f"points at {target['database']!r}"
                ),
                "next_action": "fix_deployment_database_identity",
                "authority": {
                    "mode": "deployment_manifest_literal",
                    "deployment": deployment,
                    "manifest_path": str(manifest_path),
                },
                "target": target,
            }
        return {
            "dsn": raw_dsn.strip(),
            "authority": {
                "mode": "deployment_manifest_literal",
                "deployment": deployment,
                "manifest_path": str(manifest_path),
            },
            "target": target,
        }
    local_path, local_values = _local_env_values()
    values: dict[str, str] = {}
    conflicts: list[dict[str, Any]] = []
    missing: list[str] = []
    for name in refs:
        process_value = os.environ.get(name)
        local_value = local_values.get(name)
        if local_profile_authoritative and local_value:
            values[name] = local_value
            if process_value and process_value != local_value:
                conflicts.append({
                    "name": name,
                    "process_env_ignored": True,
                    "reason": "dogfood_local_profile_authority",
                })
        elif process_value:
            values[name] = process_value
        elif local_value:
            values[name] = local_value
        else:
            missing.append(name)
    authority = {
        "mode": (
            "dogfood_local_profile"
            if local_profile_authoritative else "deployment_manifest"
        ),
        "deployment": deployment,
        "manifest_path": str(manifest_path),
        "local_env_path": str(local_path) if local_path else None,
        "local_profile_authoritative": local_profile_authoritative,
        "conflict_count": len(conflicts),
        "conflicts": conflicts,
    }
    if missing == ["OKG_DSN"]:
        # A self-hosted dogfood runtime publishes the endpoint outside any
        # checkout.  Prefer that record over worktree-local state: the record
        # is the owner, while local.env is only a compatibility projection.
        if (
            isinstance(postgres, dict)
            and postgres.get("self_hosted_authoritative") is True
            and not (
                os.environ.get("PYTEST_CURRENT_TEST")
                and os.environ.get("OKG_ENABLE_LOCAL_ENV_FOR_TESTS") != "1"
            )
        ):
            authority_record, authority_error = _dogfood_runtime_authority_record(
                deployment,
                enabled=True,
            )
            if authority_error:
                return {
                    "dsn": None,
                    "reason": authority_error,
                    "next_action": "refresh_dogfood_authority",
                    "authority": {
                        **authority,
                        "mode": "dogfood_runtime_record_invalid",
                    },
                }
            if authority_record is not None:
                expected_database = (
                    postgres.get("database")
                    if isinstance(postgres.get("database"), str)
                    else None
                )
                target = resolved_target(
                    str(authority_record["dsn"]),
                    deployment=deployment,
                    authority_mode="dogfood_runtime_record",
                    expected_database=expected_database,
                    expected_host=str(authority_record["host"]),
                    expected_port=int(authority_record["port"]),
                )
                if target["identity"] != "ok":
                    return {
                        "dsn": None,
                        "reason": (
                            "dogfood authority record does not match the "
                            f"deployment identity (identity={target['identity']})"
                        ),
                        "next_action": "refresh_dogfood_authority",
                        "authority": {
                            **authority,
                            "mode": "dogfood_runtime_record_mismatch",
                        },
                        "target": target,
                    }
                return {
                    "dsn": str(authority_record["dsn"]),
                    "authority": {
                        **authority,
                        "mode": "dogfood_runtime_record",
                        "record_path": str(
                            _dogfood_runtime_authority_path(deployment)
                        ),
                        "runtime_instance": authority_record[
                            "runtime_instance"
                        ],
                    },
                    "target": target,
                }
        # Tier R (worktree-access-tiers req.tiers.read-default): a cold
        # worktree resolves the MAIN checkout's local.env before giving up,
        # so every surface - CLI, MCP serve, dogfood status - reads the
        # canonical graph with zero per-worktree setup. Test runs opt in
        # explicitly, mirroring _local_env_values' hygiene guard.
        if not (
            os.environ.get("PYTEST_CURRENT_TEST")
            and os.environ.get("OKG_ENABLE_LOCAL_ENV_FOR_TESTS") != "1"
        ):
            worktree_dsn = worktree_main_local_env_dsn()
            if worktree_dsn:
                resolved = raw_dsn.strip().replace(
                    "${OKG_DSN}", worktree_dsn,
                )
                if not _env_refs(resolved):
                    return {
                        "dsn": resolved,
                        "authority": {
                            **authority,
                            "mode": "worktree_main_local_env",
                        },
                        "target": resolved_target(
                            resolved,
                            deployment=deployment,
                            authority_mode="worktree_main_local_env",
                            expected_database=(
                                postgres.get("database")
                                if isinstance(postgres, dict) else None
                            ),
                        ),
                    }
    if missing:
        reason = (
            "OKG_DSN is not set"
            if missing == ["OKG_DSN"]
            else "deployment DSN environment variables are not set: "
            + ", ".join(missing)
        )
        return {
            "dsn": None,
            "reason": reason,
            "next_action": "configure_okg_dsn",
            "authority": {
                **authority,
                "missing": missing,
            },
        }
    resolved = raw_dsn.strip()
    for name, value in values.items():
        resolved = resolved.replace(f"${{{name}}}", value)
    if _env_refs(resolved):
        return {
            "dsn": None,
            "reason": "deployment DSN still has unresolved variables",
            "next_action": "configure_okg_dsn",
            "authority": authority,
        }
    expected_database = (
        postgres.get("database") if isinstance(postgres, dict) else None
    )
    target = resolved_target(
        resolved,
        deployment=deployment,
        authority_mode=authority["mode"],
        expected_database=(
            expected_database if isinstance(expected_database, str) else None
        ),
    )
    authority_record, authority_error = _dogfood_runtime_authority_record(
        deployment,
        enabled=(
            isinstance(postgres, dict)
            and postgres.get("self_hosted_authoritative") is True
        ),
    )
    if authority_error:
        return {
            "dsn": None,
            "reason": authority_error,
            "next_action": "refresh_dogfood_authority",
            "authority": {
                **authority,
                "mode": "dogfood_runtime_record_invalid",
            },
        }
    if authority_record is not None:
        target = resolved_target(
            resolved,
            deployment=deployment,
            authority_mode="dogfood_runtime_record",
            expected_database=(
                expected_database if isinstance(expected_database, str) else None
            ),
            expected_host=str(authority_record["host"]),
            expected_port=int(authority_record["port"]),
        )
        authority = {
            **authority,
            "runtime_record_path": str(
                _dogfood_runtime_authority_path(deployment)
            ),
            "runtime_instance": authority_record["runtime_instance"],
        }
        if target["identity"] != "ok":
            return {
                "dsn": None,
                "reason": (
                    "resolved DSN does not match the active dogfood authority "
                    f"(identity={target['identity']})"
                ),
                "next_action": "fix_deployment_database_identity",
                "authority": authority,
                "target": target,
            }
    if target["identity"] == "mismatch":
        # The 2026-06-10 incident class: env/local.env steered the
        # deployment at a database that is not the declared one (and not an
        # isolation clone of it). Refuse - never report success blind.
        return {
            "dsn": None,
            "reason": (
                f"deployment {deployment} declares database "
                f"{target['expected_database']!r} but the environment "
                f"resolves to {target['database']!r}"
            ),
            "next_action": "fix_deployment_database_identity",
            "authority": authority,
            "target": target,
        }
    return {
        "dsn": resolved,
        "authority": authority,
        "target": target,
    }


def deployment_authority_subprocess_env(deployment: str) -> dict[str, str] | None:
    resolution = resolve_deployment_dsn_authority(
        deployment,
        local_profile_authoritative=deployment_self_hosted_authoritative(deployment),
    )
    dsn = resolution.get("dsn")
    if not isinstance(dsn, str) or not dsn:
        return None
    env = os.environ.copy()
    env["OKG_DSN"] = dsn
    return env


def worktree_main_local_env_path() -> Path | None:
    """Return the MAIN checkout's local.env path from any worktree."""
    import subprocess

    try:
        common = subprocess.run(
            ["git", "rev-parse", "--git-common-dir"],
            capture_output=True, text=True, timeout=5, check=True,
        ).stdout.strip()
    except Exception:  # noqa: BLE001 - absence of git is absence of fallback
        return None
    if not common:
        return None
    return Path(common).resolve().parent / ".okg" / "local.env"


def worktree_main_local_env_dsn() -> str | None:
    """OKG_DSN from the MAIN checkout's .okg/local.env, from any worktree.

    Core copy of the fallback first shipped in pact.decisions (finding #2);
    promoted here per worktree-access-tiers req.tiers.read-default so the
    whole CLI/MCP surface shares it, not just the pact ledger chain.
    """
    env_path = worktree_main_local_env_path()
    if env_path is None:
        return None
    if not env_path.is_file():
        return None
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.startswith("OKG_DSN="):
            value = line.split("=", 1)[1].strip().strip("'\"")
            return value or None
    return None


def publish_provenance_verdict(
    *,
    sync_root: str | None,
    checkout: str | None,
    target: dict[str, Any],
    file_derived: bool = True,
    manifest_readable: bool = True,
    sync_root_unresolved: tuple[str, ...] = (),
    sync_root_detail: str | None = None,
) -> dict[str, Any]:
    """Tier W guard core (worktree-access-tiers req.tiers.write-guard).

    Trunk projects trunk: file-derived publishes that land on the
    CANONICAL database must come from the registered sync root. Isolated
    clones (Tier C) are always legal - that is what they are for. Tier E
    event-ledger appends are shared across checkouts and are not file-derived
    truth, so callers may mark a selected source set as ``file_derived=False``.
    Pure function so the policy is testable without git or a manifest.

    ``manifest_readable=False`` (the manifest file exists but failed to parse)
    fails closed BEFORE the ``sync_root is None`` allow-with-warning branch: an
    unparsable manifest yields no trustworthy sync_root, and treating it as
    "operator declared none" would let a foreign checkout project onto the
    canonical database (the fail-open hole this guard closes).

    ``sync_root_unresolved`` (the manifest DECLARES a sync root this machine
    cannot resolve -- an unset ``${VAR}``) sits in the same position and for the
    same reason (issue #1014). Declared-but-unresolvable is not "operator
    declared none": the operator named an authoritative checkout and we cannot
    tell which one it is, so the allow-with-warning branch below must not see
    it.
    """
    identity = target.get("identity")
    if identity == "isolated":
        return {"allowed": True, "mode": "isolated_clone"}
    if not file_derived:
        return {"allowed": True, "mode": "event_plane"}
    if not manifest_readable:
        return {
            "allowed": False,
            "mode": "manifest_unreadable",
            "reason": (
                "deployment manifest exists but could not be parsed; refusing "
                "a canonical-database publish because its sync_root and "
                "declared database identity cannot be trusted"
            ),
        }
    if sync_root_unresolved:
        return {
            "allowed": False,
            "mode": "sync_root_unresolved",
            "reason": (
                "deployment declares a sync_root that cannot be resolved on "
                "this machine, so the registered checkout is unknown; refusing "
                "a canonical-database publish rather than treating a declared "
                "sync root as an undeclared one. "
                + (sync_root_detail or "")
            ).strip(),
            "unresolved_variables": list(sync_root_unresolved),
            "legal_moves": [
                (
                    "set the variable(s) in the environment, or in the "
                    "deployment checkout's .okg/local.env (OKG_-prefixed keys "
                    "only)"
                ),
                "use an isolated clone: okg deployment up <name> --target dev",
            ],
        }
    if sync_root is None:
        return {
            "allowed": True,
            "mode": "sync_root_undeclared",
            "warning": (
                "deployment declares no sync_root; any checkout may "
                "project file-derived truth onto the canonical database"
            ),
        }
    if checkout is None:
        return {
            "allowed": False,
            "mode": "checkout_unknown",
            "reason": (
                "cannot determine the producing checkout; refusing a "
                "canonical-database publish"
            ),
        }
    if str(Path(checkout).resolve()) == str(Path(sync_root).resolve()):
        return {"allowed": True, "mode": "sync_root"}
    return {
        "allowed": False,
        "mode": "foreign_checkout",
        "reason": (
            f"checkout {checkout} is not the registered sync root "
            f"{sync_root}; publishing file-derived sources from here "
            "would make this worktree's unmerged tree the canonical "
            "projection (last-publisher-wins)"
        ),
        "legal_moves": [
            "merge this branch, then publish from the sync root "
            + "(the runtime worker does this automatically)",
            "use an isolated clone: okg deployment up <name> --target dev",
        ],
    }


def check_publish_provenance(
    deployment: str,
    dsn: str | None,
    *,
    repo_start: Path | None = None,
    file_derived: bool = True,
) -> dict[str, Any]:
    """Gather sync_root + producing checkout and apply the Tier W guard."""
    import subprocess

    from okg.substrate.deployments.sync_root import resolve_declared_sync_root

    raw, manifest_readable, deployment_dir = _read_manifest_with_dir(deployment)
    # One resolver for every consumer (issue #1014): path anchors expand, a
    # `${VAR}` resolves from the environment or the deployment checkout's
    # .okg/local.env, and an unresolvable declaration stays distinguishable
    # from an absent one.
    resolution = resolve_declared_sync_root(raw, deployment_dir=deployment_dir)
    sync_root: str | None = resolution.path_str
    expected_database = None
    if raw is not None:
        postgres = raw.get("postgres") if isinstance(raw, dict) else None
        declared = postgres.get("database") if isinstance(postgres, dict) else None
        expected_database = declared if isinstance(declared, str) and declared else None
    target = resolved_target(
        dsn,
        deployment=deployment,
        expected_database=expected_database,
        manifest_readable=manifest_readable,
    )

    # A launcher-verified sealed process deliberately runs from an immutable
    # artifact without Git metadata. For file-derived work, its producing
    # checkout is the composed environment's declared sync root, but only when
    # that checkout still names the exact revision sealed into the active
    # artifact. This preserves both authorities: executable bytes come from the
    # sealed release and source bytes come from the registered checkout.
    sealed_authority: dict[str, Any] | None = None
    if os.environ.get("OKG_RELEASE_ARTIFACT_ID"):
        from okg.substrate.errors import SealedReleaseInvalid
        from okg.substrate.runtime.release_authority import (
            verify_worker_release_authority,
        )

        try:
            sealed_authority = verify_worker_release_authority(
                deployment=deployment,
                dsn=dsn or "",
            )
        except SealedReleaseInvalid as exc:
            return {
                "allowed": False,
                "mode": "sealed_release_invalid",
                "reason": f"sealed release authority is invalid: {exc}",
                "target": target,
                "sync_root": sync_root,
                "checkout": None,
            }
        if sealed_authority is None:
            return {
                "allowed": False,
                "mode": "sealed_release_invalid",
                "reason": "sealed release marker did not produce verified authority",
                "target": target,
                "sync_root": sync_root,
                "checkout": None,
            }
        if sealed_authority.get("sync_root") != sync_root:
            return {
                "allowed": False,
                "mode": "sealed_sync_root_mismatch",
                "reason": "sealed release and composed manifest disagree on sync root",
                "target": target,
                "sync_root": sync_root,
                "checkout": sealed_authority.get("sync_root"),
            }

    checkout: str | None = None
    checkout_revision: str | None = None
    try:
        checkout_output = subprocess.run(
            ["git", "rev-parse", "--show-toplevel", "HEAD"],
            capture_output=True, text=True, timeout=5, check=True,
            cwd=(
                str(sync_root)
                if sealed_authority is not None and sync_root is not None
                else str(repo_start) if repo_start else None
            ),
        ).stdout.strip().splitlines()
        if len(checkout_output) == 2:
            checkout, checkout_revision = checkout_output
    except Exception:  # noqa: BLE001
        checkout = None
    if sealed_authority is not None and (
        checkout_revision != sealed_authority.get("code_revision")
    ):
        return {
            "allowed": False,
            "mode": "sealed_sync_root_revision_mismatch",
            "reason": (
                "declared sync root revision does not match the active sealed release"
            ),
            "target": target,
            "sync_root": sync_root,
            "checkout": checkout,
            "checkout_revision": checkout_revision,
            "sealed_code_revision": sealed_authority.get("code_revision"),
        }
    verdict = publish_provenance_verdict(
        sync_root=sync_root,
        checkout=checkout,
        target=target,
        file_derived=file_derived,
        manifest_readable=manifest_readable,
        sync_root_unresolved=resolution.missing,
        sync_root_detail=resolution.detail,
    )
    return {
        **verdict,
        "target": target,
        "sync_root": sync_root,
        # Where the Tier W authority value came from. A `local_profile` value
        # is untracked: unlike editing the manifest it moves no fingerprint and
        # shows in no diff, so it must be visible in the payload.
        "sync_root_source": resolution.source,
        "checkout": checkout,
        "checkout_revision": checkout_revision,
        **(
            {"sealed_artifact_id": sealed_authority.get("artifact_id")}
            if sealed_authority is not None
            else {}
        ),
    }


def _read_manifest(deployment: str) -> tuple[dict[str, Any] | None, bool]:
    """Read a deployment manifest, distinguishing absent from unreadable.

    Thin wrapper over :func:`_read_manifest_with_dir` for callers that do not
    need the manifest's directory.
    """
    raw, readable, _dir = _read_manifest_with_dir(deployment)
    return raw, readable


def _read_manifest_with_dir(
    deployment: str,
) -> tuple[dict[str, Any] | None, bool, Path | None]:
    """Read a deployment manifest plus the directory that contains it.

    The directory is what lets a caller resolve ``sync_root`` the same way the
    typed loader does -- path anchors (``${repo_root}``) and relative values
    are both anchored on it. Returns ``(raw, readable, deployment_dir)``:

    - ``(None, True)`` when no deployment dir / manifest file exists - the
      legitimate "not found" case the resolver already handles cleanly.
    - ``(None, False)`` when the manifest file EXISTS but ``safe_load``
      raises (a corrupt/unparsable manifest). Callers must fail closed:
      a corrupt manifest is NOT the same as an operator omitting a key.
    - ``(dict, True)`` when the manifest parsed (``{}`` for an empty file).
    """
    if os.environ.get("OKG_ENVIRONMENT_DESCRIPTOR"):
        try:
            from okg.substrate.deployments import resolve_deployment

            manifest = resolve_deployment(deployment)
        except Exception:  # noqa: BLE001 - sealed authority must fail closed.
            return None, False, None
        return dict(manifest.effective_raw), True, manifest.deployment_dir

    from okg.substrate.deployments import find_deployment_dir

    deployment_dir = find_deployment_dir(deployment)
    if deployment_dir is None:
        return None, True, None
    manifest_path = deployment_dir / "deployment.yaml"
    if not manifest_path.is_file():
        return None, True, deployment_dir
    try:
        raw = fastyaml.safe_load(manifest_path.read_text(encoding="utf-8")) or {}
    except Exception:  # noqa: BLE001 - unreadable manifest fails closed.
        return None, False, deployment_dir
    return (raw if isinstance(raw, dict) else {}), True, deployment_dir


def deployment_self_hosted_authoritative(deployment: str) -> bool:
    """Whether a deployment's local profile (`.okg/local.env`) is authoritative.

    Reads ``postgres.self_hosted_authoritative`` (bool) from the deployment's
    manifest via :func:`_read_manifest`. When true, the dogfood/self-hosted
    posture makes ``.okg/local.env`` win over a stale process-env ``$OKG_DSN``
    (the dogfood database is the deployment's own authority, while agent shells
    can inherit stale values from earlier sessions).

    Returns ``False`` when the manifest is absent or unreadable, or when the key
    is missing / not literally ``true``. Pure: never mutates ``os.environ``.
    Replaces the historical hardcoded ``deployment == "okg-workspace"`` check so
    any self-hosted deployment can opt in via its manifest, and no second
    deployment silently loses the protection.
    """
    raw, readable = _read_manifest(deployment)
    if raw is None or not readable:
        return False
    postgres = raw.get("postgres") if isinstance(raw, dict) else None
    flag = postgres.get("self_hosted_authoritative") if isinstance(postgres, dict) else None
    return flag is True


def _declared_database(deployment: str) -> str | None:
    """The manifest's declared database name, if the deployment pins one.

    A corrupt manifest is reported as ``None`` here, but callers that gate on
    identity must consult :func:`_read_manifest`'s ``readable`` flag so an
    unreadable manifest fails closed rather than reading as "undeclared".
    """
    raw, _readable = _read_manifest(deployment)
    if raw is None:
        return None
    postgres = raw.get("postgres") if isinstance(raw, dict) else None
    database = postgres.get("database") if isinstance(postgres, dict) else None
    return database if isinstance(database, str) and database else None


def _env_refs(value: str) -> list[str]:
    refs = re.findall(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", value)
    return sorted(set(refs))


def _dogfood_runtime_authority_record(
    deployment: str,
    *,
    enabled: bool,
) -> tuple[dict[str, Any] | None, str | None]:
    """Read the runtime owner while preserving hermetic pytest isolation."""
    if not enabled:
        return None, None
    if (
        os.environ.get("PYTEST_CURRENT_TEST")
        and os.environ.get("OKG_ENABLE_LOCAL_ENV_FOR_TESTS") != "1"
    ):
        return None, None
    from okg.substrate.dogfood_authority import read_authority

    return read_authority(deployment)


def _dogfood_runtime_authority_path(deployment: str) -> Path:
    from okg.substrate.dogfood_authority import authority_path

    return authority_path(deployment)


def _local_env_values() -> tuple[Path | None, dict[str, str]]:
    from okg.substrate.local_env import configured_local_env_path, parse_local_env

    if (
        os.environ.get("PYTEST_CURRENT_TEST")
        and not os.environ.get("OKG_LOCAL_ENV")
        and os.environ.get("OKG_ENABLE_LOCAL_ENV_FOR_TESTS") != "1"
    ):
        return None, {}
    path = configured_local_env_path()
    if not path.is_file():
        return path, {}
    try:
        return path, parse_local_env(path)
    except Exception:  # noqa: BLE001 - callers report unresolved authority.
        return path, {}
