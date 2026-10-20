"""HTTP-only CLI client and standalone launcher for external-action review."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import quote, urlsplit


_MAX_BODY_BYTES = 1_048_576


def _cmd_external_action(args: argparse.Namespace, *, as_json: bool) -> int:
    """Dispatch ``okg external-action`` without a local transition path."""

    if args.external_action_subcmd == "serve":
        from okg.substrate.external_actions.server import run_server

        return int(run_server(args))

    try:
        response = _request_for_args(args)
    except Exception as exc:  # noqa: BLE001 - normalized without secret values.
        return _emit_failure(
            code="external_action_api_unavailable",
            message=f"external-action API request failed: {type(exc).__name__}",
            as_json=as_json,
        )

    try:
        payload = response.json()
    except ValueError:
        return _emit_failure(
            code="external_action_invalid_response",
            message="external-action API returned a non-JSON response",
            as_json=as_json,
        )
    if not isinstance(payload, Mapping):
        return _emit_failure(
            code="external_action_invalid_response",
            message="external-action API returned a non-object response",
            as_json=as_json,
        )
    safe_payload = dict(payload)
    if response.is_success:
        _emit_payload(safe_payload, as_json=as_json)
        return 0
    code = str(safe_payload.get("code") or "external_action_request_refused")
    message = str(safe_payload.get("message") or code)
    return _emit_failure(
        code=code,
        message=message,
        data={
            key: value
            for key, value in safe_payload.items()
            if key in {"code", "proposal_id", "lifecycle_version", "service_mode"}
        },
        as_json=as_json,
    )


def _request_for_args(args: argparse.Namespace) -> Any:
    """Build one bounded HTTP request; redirects are always refused."""

    import requests

    api_url = _validated_api_url(
        getattr(args, "api_url", None)
        or os.environ.get("OKG_EXTERNAL_ACTION_API_URL", "")
    )
    method, path = _route_for_args(args)
    headers = {"Accept": "application/json"}
    # A client certificate is itself the direct sender constraint.  Do not
    # add token headers in this mode: the API deliberately rejects a mixed
    # mTLS + Authorization/DPoP request.  This also makes restore CLI calls
    # executable against the standalone certificate-only route.
    client_cert = getattr(args, "client_cert", None)
    client_key = getattr(args, "client_key", None)
    certificate_only = bool(client_cert)
    if certificate_only:
        if client_key is not None and not isinstance(client_key, (str, os.PathLike)):
            raise ValueError("client key path is invalid")
    else:
        authorization_name = getattr(args, "authorization_env", "OKG_EXTERNAL_ACTION_AUTHORIZATION")
        dpop_name = getattr(args, "dpop_env", "OKG_EXTERNAL_ACTION_DPOP")
        authorization = os.environ.get(str(authorization_name), "")
        dpop = os.environ.get(str(dpop_name), "")
        if not authorization:
            raise ValueError("authorization environment variable is unset")
        headers["Authorization"] = authorization
        if dpop:
            headers["DPoP"] = dpop
    if getattr(args, "idempotency_key", None):
        headers["Idempotency-Key"] = str(args.idempotency_key)
    if getattr(args, "if_match", None):
        headers["If-Match"] = str(args.if_match)

    body = _load_body(args)
    if body is not None:
        headers["Content-Type"] = "application/json"

    cert: str | tuple[str, str] | None = None
    if client_cert and client_key:
        cert = (str(client_cert), str(client_key))
    elif client_cert:
        cert = str(client_cert)
    ca_bundle = getattr(args, "ca_bundle", None)
    verify: bool | str = str(ca_bundle) if ca_bundle else True
    return requests.request(
        method,
        f"{api_url}{path}",
        headers=headers,
        json=body,
        timeout=float(args.timeout_seconds),
        allow_redirects=False,
        cert=cert,
        verify=verify,
    )


def _route_for_args(args: argparse.Namespace) -> tuple[str, str]:
    command = str(args.external_action_subcmd)
    deployment = quote(str(args.deployment), safe="")
    base = f"/v1/deployments/{deployment}/external-actions"
    proposal = quote(str(getattr(args, "proposal_id", "")), safe="")
    execution = quote(str(getattr(args, "execution_id", "")), safe="")
    restore_id = quote(str(getattr(args, "restore_id", "")), safe="")
    batch_token = (
        str(int(args.batch_index)) if command == "restore-batch" else ""
    )
    routes: dict[str, tuple[str, str]] = {
        "list": ("GET", base),
        "show": ("GET", f"{base}/{proposal}"),
        "timeline": ("GET", f"{base}/{proposal}/timeline"),
        "submit": ("POST", base),
        "edit": ("POST", f"{base}/{proposal}/edits"),
        "decide": ("POST", f"{base}/{proposal}/decisions"),
        "cancel": ("POST", f"{base}/{proposal}/cancel"),
        "claim": ("POST", f"{base}/{proposal}/execution-claims"),
        "attest-owner-unavailable": (
            "POST",
            f"{base}/{proposal}/execution-claims/{execution}/owner-unavailability-attestations",
        ),
        "outcome": (
            "POST",
            f"{base}/{proposal}/execution-claims/{execution}/outcome",
        ),
        "recovery": (
            "GET",
            f"{base}/recovery-records/{quote(str(args.effect_instance_key), safe='')}",
        ),
        "restore-create": ("POST", f"{base}/restore-reconciliations"),
        "restore-batch": (
            "PUT",
            f"{base}/restore-reconciliations/{restore_id}/checkpoint-batches/{batch_token}",
        ),
        "restore-finalize": (
            "POST",
            f"{base}/restore-reconciliations/{restore_id}/finalize",
        ),
    }
    method, path = routes[command]
    if command == "list":
        query: list[str] = []
        if getattr(args, "state", None):
            query.append(f"state={quote(str(args.state), safe='')}")
        query.append(f"limit={max(1, min(int(args.limit), 500))}")
        path = f"{path}?{'&'.join(query)}"
    return method, path


def _load_body(args: argparse.Namespace) -> dict[str, Any] | None:
    path = getattr(args, "body_file", None)
    if not path:
        return None
    body_path = Path(path)
    raw = body_path.read_bytes()
    if len(raw) > _MAX_BODY_BYTES:
        raise ValueError("request body exceeds the CLI limit")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("request body must be a JSON object")
    return value


def _validated_api_url(value: str) -> str:
    if not value:
        raise ValueError("OKG_EXTERNAL_ACTION_API_URL is unset")
    parsed = urlsplit(value)
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("API URL may not contain credentials, query, or fragment")
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("API URL must be an absolute HTTP(S) URL")
    if parsed.scheme != "https" and parsed.hostname not in {"127.0.0.1", "::1", "localhost"}:
        raise ValueError("non-loopback API URLs require HTTPS")
    return value.rstrip("/")


def _emit_payload(payload: dict[str, Any], *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, sort_keys=True, default=str))
    else:
        print(json.dumps(payload, indent=2, sort_keys=True, default=str))


def _emit_failure(
    *,
    code: str,
    message: str,
    as_json: bool,
    data: Mapping[str, Any] | None = None,
) -> int:
    payload = {"ok": False, "code": code, "message": message}
    if data:
        payload.update(data)
    _emit_payload(payload, as_json=as_json)
    return 1
