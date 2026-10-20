"""Shared CLI DSN authority helpers for read-side commands."""
from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from typing import Any

from okg.substrate.deployment_authority import (
    deployment_self_hosted_authoritative,
    resolve_deployment_dsn_authority,
)
from okg.substrate.dsn_form import (
    is_keyword_form_dsn,
    keyword_form_refusal,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.support.user_hints import okg_prefix


@dataclass(frozen=True)
class ReadDsnResolution:
    dsn: str
    deployment: str | None
    authority: dict[str, Any]
    target: dict[str, Any] | None


def _refuse_keyword_form_dsn(source: str, dsn: str, *, surface: str) -> None:
    """Refuse a libpq keyword/value DSN, naming where it came from.

    The deployment branch below gets this refusal from
    `resolve_deployment_dsn_authority`; these are the two exits that read a
    DSN directly, so they need it here (issue #2001).
    """
    if not is_keyword_form_dsn(dsn):
        return
    raise SubstrateError(
        keyword_form_refusal(source, dsn),
        data={
            "surface": surface,
            "dsn_form": "libpq_keyword",
            "dsn_source": source,
        },
        fix_hint=(
            "write the DSN in URL form: "
            "postgresql://user:password@host:port/dbname"
        ),
    )


def resolve_read_dsn(
    args: argparse.Namespace,
    *,
    surface: str,
) -> ReadDsnResolution:
    """Resolve a read-side DSN without bypassing deployment authority.

    Deployment-scoped reads must use the same authority chain as readiness and
    dogfood surfaces. Non-deployment reads remain explicit DBA mode:
    ``--dsn`` then ``OKG_DSN``.
    """

    requested_deployment = getattr(args, "deployment", None)
    active_deployment = os.environ.get("OKG_DEPLOYMENT")
    deployment = requested_deployment or active_deployment
    explicit_dsn = getattr(args, "dsn", None)
    if os.environ.get("OKG_ENVIRONMENT_DESCRIPTOR"):
        if (
            requested_deployment
            and active_deployment
            and requested_deployment != active_deployment
        ):
            raise SubstrateError(
                "sealed read requested a deployment outside its active environment",
                data={
                    "surface": surface,
                    "requested": requested_deployment,
                    "active": active_deployment,
                },
            )
        deployment = active_deployment or requested_deployment
        sealed_dsn = os.environ.get("OKG_DSN")
        if not sealed_dsn:
            raise SubstrateError(
                "sealed environment did not provide its authoritative OKG_DSN",
                data={
                    "surface": surface,
                    "deployment": deployment,
                    "authority": {
                        "mode": "sealed_environment",
                        "deployment": deployment,
                    },
                },
                fix_hint="repair the active environment descriptor and launcher",
            )
        _refuse_keyword_form_dsn(
            "OKG_DSN (the sealed environment's launcher)",
            sealed_dsn,
            surface=surface,
        )
        return ReadDsnResolution(
            dsn=sealed_dsn,
            deployment=deployment,
            authority={
                "mode": "sealed_environment",
                "deployment": deployment,
            },
            target=None,
        )
    if deployment:
        resolution = resolve_deployment_dsn_authority(
            deployment,
            explicit_dsn=explicit_dsn,
            local_profile_authoritative=deployment_self_hosted_authoritative(deployment),
        )
        dsn = resolution.get("dsn")
        if isinstance(dsn, str) and dsn:
            return ReadDsnResolution(
                dsn=dsn,
                deployment=deployment,
                authority=dict(resolution.get("authority") or {}),
                target=(
                    dict(resolution["target"])
                    if isinstance(resolution.get("target"), dict)
                    else None
                ),
            )
        authority = dict(resolution.get("authority") or {
            "mode": "unresolved",
            "deployment": deployment,
        })
        missing = authority.get("missing")
        raise SubstrateError(
            (
                resolution.get("reason")
                or f"deployment {deployment!r} DSN authority is unresolved"
            ),
            data={
                "surface": surface,
                "deployment": deployment,
                "authority": authority,
                "target": resolution.get("target"),
                **({"missing": missing} if missing else {}),
            },
            fix_hint=(
                f"{okg_prefix()} deployment env doctor {deployment} --json"
            ),
        )

    if explicit_dsn:
        _refuse_keyword_form_dsn(
            "the --dsn value", explicit_dsn, surface=surface,
        )
        return ReadDsnResolution(
            dsn=explicit_dsn,
            deployment=None,
            authority={"mode": "explicit", "deployment": None},
            target=None,
        )
    env_dsn = os.environ.get("OKG_DSN")
    if env_dsn:
        _refuse_keyword_form_dsn("OKG_DSN", env_dsn, surface=surface)
        return ReadDsnResolution(
            dsn=env_dsn,
            deployment=None,
            authority={"mode": "process_env", "deployment": None},
            target=None,
        )
    raise SubstrateError(
        (
            "no DSN: pass --deployment <name> so the shared authority "
            "chain can resolve the deployment, or pass --dsn for DBA mode"
        ),
        data={
            "surface": surface,
            "authority": {"mode": "unresolved", "deployment": None},
        },
        fix_hint=(
            "for deployment reads: --deployment <name>; "
            "for DBA reads: --dsn postgres://..."
        ),
    )


__all__ = ["ReadDsnResolution", "resolve_read_dsn"]
