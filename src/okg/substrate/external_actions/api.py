"""Closed ASGI transport for the external-action review state machine.

The transport is deliberately provider blind. It owns the HTTP grammar,
authentication boundary, route-to-command mapping, and safe response headers;
all durable state changes are delegated to a supplied review service.
"""

from __future__ import annotations

import inspect
import re
from contextlib import nullcontext
import secrets
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Iterable, Mapping
from urllib.parse import unquote

from .auth import (
    ASGIMTLSPrincipalAdapter,
    AuthAudit,
    AuthenticationError,
    VerifiedPrincipal,
    authenticate_request,
    build_auth_audit,
    clear_principal,
    validate_verified_principal,
)
from .models import Actor
from .action_contracts import transition_request_hash
from .signing import SigningError, domain_digest, strict_json_loads


class APIError(Exception):
    def __init__(self, status_code: int, code: str, *, message: str | None = None, headers: Mapping[str, str] | None = None) -> None:
        super().__init__(message or code)
        self.status_code = status_code
        self.code = code
        self.message = message or code
        self.headers = dict(headers or {})


@dataclass(frozen=True)
class APIRequest:
    method: str
    path: str
    raw_path: bytes
    headers: tuple[tuple[str, str], ...]
    body: bytes
    json: Any = None
    principal: VerifiedPrincipal | None = None
    request_id: str = ""
    query: Mapping[str, str] = None  # type: ignore[assignment]
    operation: str = ""
    # This is the redacted, deferred audit envelope.  It deliberately has no
    # persisted audit id: the service appends it only after the same-transaction
    # Nomos decision has been recorded.
    auth_audit: Mapping[str, Any] | AuthAudit | None = None


_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9._:+\-]{1,128}$")
_IDEMPOTENCY = re.compile(r"^[A-Za-z0-9._~:/+\-]{16,128}$")
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_ULID = re.compile(r"^[0-9A-HJKMNP-TV-Z]{26}$")
_DEPLOYMENT = re.compile(r"^[a-z][a-z0-9-]{0,63}$")
_LIST_STATES = frozenset({"pending", "edited", "approved", "rejected", "expired", "cancelled"})


def validate_safe_identifier(value: Any, *, field: str = "identifier") -> str:
    if not isinstance(value, str) or not _SAFE_IDENTIFIER.fullmatch(value):
        raise APIError(422, "invalid_proposal")
    return value


def validate_idempotency_key(value: Any) -> str:
    if not isinstance(value, str) or not _IDEMPOTENCY.fullmatch(value):
        raise APIError(422, "invalid_proposal")
    return value


def parse_if_match(value: Any) -> tuple[int, int, str]:
    if not isinstance(value, str):
        raise APIError(428, "proposal_precondition_required")
    match = re.fullmatch(r'"lifecycle:(0|[1-9][0-9]*);payload-version:(0|[1-9][0-9]*);payload:(sha256:[0-9a-f]{64})"', value)
    if match is None:
        raise APIError(428, "proposal_precondition_required")
    return int(match.group(1)), int(match.group(2)), match.group(3)


def validate_path_identifier(value: Any, *, kind: str = "resource") -> str:
    if not isinstance(value, str) or not value or any(ord(c) <= 0x20 or ord(c) >= 0x7f for c in value) or any(x in value for x in ("/", "\\", "%")):
        raise APIError(400, "invalid_request")
    if kind in {"proposal_id", "decision_id", "execution_id", "request_id"} and not _ULID.fullmatch(value):
        raise APIError(400, "invalid_request")
    if kind == "deployment" and not _DEPLOYMENT.fullmatch(value):
        raise APIError(400, "invalid_request")
    if kind == "effect_instance_key" and not _DIGEST.fullmatch(value):
        raise APIError(400, "invalid_request")
    if kind == "restore_id" and not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,63}", value):
        raise APIError(400, "invalid_request")
    return value


def _ulid() -> str:
    alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
    value = (int(time.time() * 1000) << 80) | secrets.randbits(80)
    chars = []
    for _ in range(26):
        chars.append(alphabet[value & 31])
        value >>= 5
    return "".join(reversed(chars))


def _headers(scope: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    pairs: list[tuple[str, str]] = []
    for raw_key, raw_value in scope.get("headers", []):
        try:
            key = raw_key.decode("ascii") if isinstance(raw_key, bytes) else str(raw_key)
            value = raw_value.decode("ascii") if isinstance(raw_value, bytes) else str(raw_value)
        except (UnicodeDecodeError, ValueError) as exc:
            raise APIError(400, "invalid_request") from exc
        if not key or "\r" in key or "\n" in key or "\r" in value or "\n" in value:
            raise APIError(400, "invalid_request")
        pairs.append((key.lower(), value))
    return tuple(pairs)


def _one_header(headers: Iterable[tuple[str, str]], name: str) -> str | None:
    values = [v for k, v in headers if k == name.lower()]
    if len(values) > 1 or any("," in v or "\r" in v or "\n" in v for v in values):
        raise APIError(400, "invalid_request")
    return values[0] if values else None


def _safe_path(raw_path: bytes) -> str:
    if not isinstance(raw_path, bytes):
        raise APIError(400, "invalid_request")
    try:
        path = raw_path.decode("ascii")
    except UnicodeDecodeError as exc:
        raise APIError(400, "invalid_request") from exc
    if not path.startswith("/") or "//" in path or "/./" in path or "/../" in path or "%" in path or "\\" in path or any(ord(ch) <= 0x20 or ord(ch) >= 0x7f for ch in path):
        raise APIError(400, "invalid_request")
    return path


def _query(scope: Mapping[str, Any]) -> dict[str, str]:
    raw = scope.get("query_string", b"")
    if isinstance(raw, str):
        try:
            raw = raw.encode("ascii")
        except UnicodeEncodeError as exc:
            raise APIError(400, "invalid_request") from exc
    if not isinstance(raw, bytes):
        raise APIError(400, "invalid_request")
    if not raw:
        return {}
    try:
        text = raw.decode("ascii")
    except UnicodeDecodeError as exc:
        raise APIError(400, "invalid_request") from exc
    result: dict[str, str] = {}
    for part in text.split("&"):
        if not part or "=" not in part:
            raise APIError(400, "invalid_request")
        key, value = part.split("=", 1)
        if not key or key in result or any(c in key for c in "?#"):
            raise APIError(400, "invalid_request")
        try:
            key, value = unquote(key, errors="strict"), unquote(value, errors="strict")
        except (UnicodeDecodeError, ValueError) as exc:
            raise APIError(400, "invalid_request") from exc
        if key not in {"state", "limit"} or not value:
            raise APIError(400, "invalid_request")
        result[key] = value
    if "limit" in result and (not result["limit"].isdecimal() or not 1 <= int(result["limit"]) <= 1000):
        raise APIError(400, "invalid_request")
    if "state" in result:
        validate_safe_identifier(result["state"], field="state")
        if result["state"] not in _LIST_STATES:
            raise APIError(400, "invalid_request")
    return result


async def _read_body(receive: Callable[[], Awaitable[Mapping[str, Any]]], *, limit: int, content_length: str | None) -> bytes:
    if content_length is not None:
        if not content_length.isdecimal() or (len(content_length) > 1 and content_length.startswith("0")):
            raise APIError(400, "invalid_request")
        if int(content_length) > limit:
            raise APIError(413, "review_request_too_large")
    chunks: list[bytes] = []
    total = 0
    while True:
        event = await receive()
        if event.get("type") != "http.request":
            raise APIError(400, "invalid_request")
        chunk = event.get("body", b"")
        if not isinstance(chunk, bytes):
            raise APIError(400, "invalid_request")
        total += len(chunk)
        if total > limit:
            raise APIError(413, "review_request_too_large")
        chunks.append(chunk)
        if not event.get("more_body", False):
            break
    raw = b"".join(chunks)
    if content_length is not None and int(content_length) != len(raw):
        raise APIError(400, "invalid_request")
    return raw


def _response(status: int, payload: Mapping[str, Any], *, headers: Mapping[str, str] | None = None) -> tuple[int, list[tuple[bytes, bytes]], bytes]:
    from .signing import canonical_json_bytes
    body = canonical_json_bytes(dict(payload))
    out_headers = [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode("ascii"))]
    if headers:
        out_headers.extend((key.lower().encode("ascii"), value.encode("ascii")) for key, value in headers.items())
    return status, out_headers, body


@dataclass(frozen=True)
class _Route:
    operation: str
    record_id: str | None = None
    proposal_id: str | None = None
    execution_id: str | None = None
    restore_id: str | None = None
    batch_index: int | None = None
    effect_instance_key: str | None = None


class ExternalActionAPI:
    """Strict ASGI route adapter; it never executes a provider effect."""

    def __init__(self, service: Any = None, *, config: Any = None, authenticator: Callable[..., VerifiedPrincipal] | None = None, mode_provider: Callable[[], str] | None = None, nonce_store: Any = None, principal_mapping: Mapping[str, Any] | Callable[[str, str], Any] | None = None, action_contracts: Mapping[str, Any] | None = None, auth_audit_writer: Callable[..., Any] | None = None, auth_config_digest: str | None = None, require_auth_audit: bool = False, static_bearer_secret: str | None = None, static_bearer_principal: VerifiedPrincipal | Mapping[str, Any] | None = None, mtls_principal_adapter: Callable[..., VerifiedPrincipal] | None = None, mtls_certificate_mapping: Mapping[str, Any] | Callable[..., Any] | None = None, require_identity_barrier: bool = False) -> None:
        self.service = service
        self.config = config
        self.authenticator = authenticator
        self.mode_provider = mode_provider
        self.nonce_store = nonce_store
        self.principal_mapping = principal_mapping
        self.mtls_principal_adapter = mtls_principal_adapter
        self.mtls_certificate_mapping = mtls_certificate_mapping
        self.require_identity_barrier = require_identity_barrier
        self.action_contracts = self._compile_action_contracts(action_contracts)
        self.auth_audit_writer = auth_audit_writer
        # Development-only bearer material is injected out of band.  The
        # configuration contains only the opt-in boolean; never place a
        # secret in config-derived hashes, logs, or request bodies.
        self.static_bearer_secret = static_bearer_secret
        self.static_bearer_principal = static_bearer_principal
        if static_bearer_secret is not None:
            auth_config = getattr(config, "authentication", None)
            if getattr(auth_config, "static_bearer", False) is not True or getattr(auth_config, "development_loopback_only", True) is not True:
                raise ValueError("static bearer requires explicit loopback development opt-in")
            if not isinstance(static_bearer_secret, str) or not static_bearer_secret or len(static_bearer_secret) > 4096:
                raise ValueError("static bearer secret is invalid")
            if static_bearer_principal is None:
                raise ValueError("static bearer principal is required")
        if auth_config_digest is not None and not _DIGEST.fullmatch(auth_config_digest):
            raise ValueError("auth_config_digest must be a lowercase sha256 digest")
        self.auth_config_digest = auth_config_digest
        self.require_auth_audit = require_auth_audit
        self.bound = True

    @staticmethod
    def _compile_action_contracts(source: Mapping[str, Any] | None) -> dict[str, Any]:
        """Compile an exact registry; never skip an invalid entry."""

        if source is None:
            return {}
        if not isinstance(source, Mapping):
            raise ValueError("action contract registry must be a mapping")
        if "actions" in source:
            actions = source.get("actions")
            if set(source) != {"schema", "actions"} or source.get("schema") != "okg.external_action_contracts/v1" or not isinstance(actions, list):
                raise ValueError("action contract registry has invalid envelope")
            normalized: dict[str, Any] = {}
            for item in actions:
                if not isinstance(item, Mapping) or not isinstance(item.get("action_type"), str) or item["action_type"] in normalized:
                    raise ValueError("action contract registry has an invalid or duplicate action")
                normalized[item["action_type"]] = item
            source = normalized
        from .action_contracts import ActionContract

        compiled: dict[str, Any] = {}
        for key, value in source.items():
            if not isinstance(key, str) or key == "actions":
                raise ValueError("action contract registry has invalid keys")
            if not hasattr(value, "action_contract_digest"):
                value = ActionContract.from_mapping(value)
            if not isinstance(getattr(value, "action_type", None), str) or value.action_type != key:
                raise ValueError("action contract registry key does not match action type")
            if key in compiled:
                raise ValueError("action contract registry has duplicate action types")
            compiled[key] = value
        return compiled

    async def __call__(self, scope: Mapping[str, Any], receive: Callable[[], Awaitable[Mapping[str, Any]]], send: Callable[[Mapping[str, Any]], Awaitable[None]]) -> None:
        status = 500
        out_headers: list[tuple[bytes, bytes]] = []
        body = b""
        request_id = _ulid()
        try:
            if scope.get("type") != "http":
                raise APIError(404, "not_found")
            method = str(scope.get("method", "")).upper()
            raw_path = scope.get("raw_path")
            if raw_path is None:
                raw_path = str(scope.get("path", "/")).encode("ascii")
            path = _safe_path(raw_path)
            headers = _headers(scope)
            query = _query(scope)
            for singleton in ("authorization", "dpop", "idempotency-key", "if-match", "content-length", "content-type", "host"):
                _one_header(headers, singleton)
            limit = getattr(getattr(self.config, "limits", None), "request_body_bytes_maximum", 262_144)
            raw_body = await _read_body(receive, limit=limit, content_length=_one_header(headers, "content-length"))
            route = self._route(method, path)
            if query and route.operation != "list":
                raise APIError(400, "invalid_request")
            if route.operation == "health":
                mode = self._mode()
                status, out_headers, body = _response(200, {"status": "quarantined" if mode == "restore_only" else "ok", "service_mode": mode, "payload_available": mode != "restore_only", "claim_available": mode != "restore_only"})
            else:
                principal = await self._authenticate(headers, method, scope, path)
                if route.operation in {"restore_create", "restore_batch", "restore_finalize"} and principal.auth_method != "mtls":
                    raise AuthenticationError("sender_constraint_required")
                parsed: Any = None
                if route.operation in {"read", "read_payload", "list", "timeline", "recovery_record"} and raw_body:
                    raise APIError(400, "invalid_request")
                if raw_body:
                    try:
                        parsed = strict_json_loads(raw_body)
                    except SigningError as exc:
                        raise APIError(422, "invalid_proposal") from exc
                    self._enforce_json_limits(parsed)
                if route.operation not in {"read", "read_payload", "list", "timeline", "recovery_record", "run_read"} and _one_header(headers, "content-type") != "application/json":
                    raise APIError(422, "invalid_proposal")
                # Authentication evidence is intentionally deferred.  The
                # lifecycle service binds it to the Nomos decision and appends
                # it after authorization, in the same transaction.
                audit, audit_ref = self._auth_audit(principal, request_id)
                request = APIRequest(method=method, path=path, raw_path=raw_path, headers=headers, body=raw_body, json=parsed, principal=principal, request_id=request_id, query=query, operation=route.operation, auth_audit=audit)
                status, result_headers, result = await self.dispatch(request, route, audit_ref=audit_ref)
                status, out_headers, body = _response(status, result, headers=result_headers)
        except AuthenticationError as exc:
            status, out_headers, body = _response(
                exc.status_code,
                {"code": exc.code, "request_id": request_id},
                headers={
                    "www-authenticate": (
                        "DPoP" if exc.code == "sender_constraint_required" else "Bearer"
                    )
                },
            )
        except APIError as exc:
            status, out_headers, body = _response(
                exc.status_code,
                {"code": exc.code, "request_id": request_id},
                headers=exc.headers,
            )
        except Exception as exc:
            code = getattr(exc, "code", "review_authority_unavailable")
            error_status = getattr(exc, "status", getattr(exc, "status_code", 503))
            if not isinstance(code, str) or not re.fullmatch(r"[a-z][a-z0-9_]{1,63}", code):
                code = "review_authority_unavailable"
            if not isinstance(error_status, int) or error_status < 400 or error_status > 599:
                error_status = 503
            error: dict[str, Any] = {"code": code, "request_id": request_id}
            # Service errors carry only closed, non-sensitive response
            # annotations.  Preserve the restore quarantine marker so a
            # coverage refusal has the typed contract shape; never forward
            # arbitrary exception attributes or database text.
            detail = getattr(exc, "detail", None)
            if isinstance(detail, Mapping):
                for field in ("quarantine_state", "classification"):
                    value = detail.get(field)
                    if isinstance(value, str) and value in {
                        "blocked",
                        "success",
                        "failure",
                        "database_restore_terminal_conflict",
                    }:
                        error[field] = value
            status, out_headers, body = _response(error_status, error)
        finally:
            clear_principal()
        await send({"type": "http.response.start", "status": status, "headers": out_headers})
        await send({"type": "http.response.body", "body": body})

    def _mode(self) -> str:
        mode = self.mode_provider() if self.mode_provider else getattr(self.config, "service_mode", None) or "normal"
        if mode not in {"normal", "restore_only"}:
            raise APIError(503, "review_authority_unavailable")
        return mode

    def _route(self, method: str, path: str) -> _Route:
        if path != "/" and path.endswith("/"):
            raise APIError(404, "not_found")
        if path == "/health" and method == "GET":
            return _Route("health")
        configured = getattr(self.config, "deployment", None)
        segments = path.split("/")[1:]
        if len(segments) < 4 or segments[:2] != ["v1", "deployments"] or segments[3] not in {"external-actions", "external-automation-runs"}:
            raise APIError(404, "not_found")
        deployment = validate_path_identifier(segments[2], kind="deployment")
        if configured is not None and (not isinstance(configured, str) or deployment != configured):
            raise APIError(404, "not_found")
        tail = segments[4:]
        if segments[3] == "external-automation-runs":
            if not tail and method == "POST":
                return _Route("run_register")
            if tail:
                record_id = validate_path_identifier(tail[0], kind="proposal_id")
                if len(tail) == 1 and method == "GET":
                    return _Route("run_read", record_id=record_id)
                if len(tail) == 2 and tail[1] == "proposals" and method == "POST":
                    return _Route("run_link", record_id=record_id)
            raise APIError(404, "not_found")
        if not tail:
            if method == "POST":
                return _Route("submit")
            if method == "GET":
                return _Route("list")
            raise APIError(405, "method_not_allowed")
        if tail[:1] == ["recovery-records"]:
            if method == "GET" and len(tail) == 2:
                return _Route("recovery_record", effect_instance_key=validate_path_identifier(tail[1], kind="effect_instance_key"))
            raise APIError(404, "not_found")
        if tail == ["restore-reconciliations"]:
            if method == "POST":
                return _Route("restore_create")
            raise APIError(405, "method_not_allowed")
        if len(tail) == 4 and tail[0] == "restore-reconciliations" and tail[2] == "checkpoint-batches":
            if method != "PUT" or not re.fullmatch(r"0|[1-9][0-9]*", tail[3]) or int(tail[3]) > 2**31 - 1:
                raise APIError(404, "not_found")
            return _Route("restore_batch", restore_id=validate_path_identifier(tail[1], kind="restore_id"), batch_index=int(tail[3]))
        if len(tail) == 3 and tail[0] == "restore-reconciliations" and tail[2] == "finalize":
            if method != "POST":
                raise APIError(405, "method_not_allowed")
            return _Route("restore_finalize", restore_id=validate_path_identifier(tail[1], kind="restore_id"))
        proposal_id = validate_path_identifier(tail[0], kind="proposal_id")
        if len(tail) == 1 and method == "GET":
            return _Route("read", proposal_id=proposal_id)
        if len(tail) == 2 and tail[1] == "payload" and method == "GET":
            return _Route("read_payload", proposal_id=proposal_id)
        if len(tail) == 2 and tail[1] == "timeline" and method == "GET":
            return _Route("timeline", proposal_id=proposal_id)
        if len(tail) == 2 and tail[1] in {"edits", "decisions", "cancel", "execution-claims"} and method == "POST":
            return _Route({"edits": "edit", "decisions": "decide", "cancel": "cancel", "execution-claims": "claim"}[tail[1]], proposal_id=proposal_id)
        if len(tail) == 4 and tail[1] == "execution-claims" and tail[3] in {"owner-unavailability-attestations", "outcome"} and method == "POST":
            execution_id = validate_path_identifier(tail[2], kind="execution_id")
            return _Route("attest_owner_unavailable" if tail[3] == "owner-unavailability-attestations" else "report", proposal_id=proposal_id, execution_id=execution_id)
        raise APIError(404, "not_found")

    @staticmethod
    def _blocked_in_restore_only(route: _Route) -> bool:
        return route.operation not in {"read", "list", "timeline", "recovery_record", "restore_create", "restore_batch", "restore_finalize", "attest_owner_unavailable", "report"}

    @staticmethod
    def _restore_replay_key_hash(value: str) -> str | None:
        """Hash an already validated Idempotency-Key for replay discovery.

        This is only a read of the durable snapshot.  The service/SQL command
        remains authoritative for the request-hash comparison and response;
        this preflight merely lets an exact historical replay reach that
        command while restore-only blocks new lifecycle mutations.
        """

        if not _IDEMPOTENCY.fullmatch(value):
            return None
        try:
            return domain_digest("okg-external-action-idempotency-key-v1", value)
        except SigningError:
            return None

    def _has_restore_only_replay_candidate(self, route: _Route, request: APIRequest) -> bool:
        """Return whether a durable snapshot has a key that may be replayed.

        A positive answer does not authorize a request or declare it exact.
        It only defers the final key/request-hash comparison to the fixed
        service boundary.  New requests still fail there (or at the database
        restore trigger) without a lifecycle mutation.
        """

        if not self._blocked_in_restore_only(route):
            return True
        store = getattr(self.service, "store", None)
        if getattr(store, "is_durable", False) is not True or request.principal is None:
            return False
        raw_key = _one_header(request.headers, "idempotency-key")
        if raw_key is None:
            return False
        key_hash = self._restore_replay_key_hash(raw_key)
        if key_hash is None:
            return False
        deployment = request.path.split("/")[3]
        subject = request.principal.separation_subject
        if route.operation == "submit":
            records = getattr(store, "submit_idempotency", None)
            if records is None or not callable(getattr(records, "get", None)):
                return False
            return records.get((deployment, subject, key_hash)) is not None
        if route.operation in {"edit", "decide", "cancel"}:
            records = getattr(store, "transition_idempotency", None)
            if records is None or not callable(getattr(records, "get", None)) or route.proposal_id is None:
                return False
            return any(
                records.get(candidate) is not None
                for candidate in (
                    (deployment, route.proposal_id, subject, key_hash),
                    (deployment, route.proposal_id, subject + ":" + key_hash),
                )
            )
        if route.operation == "claim":
            getter = getattr(store, "get_execution", None)
            if not callable(getter) or route.proposal_id is None:
                return False
            try:
                execution = getter(route.proposal_id)
            except Exception:
                return False
            return (
                execution is not None
                and getattr(execution, "worker_separation_subject", None) == subject
                and getattr(execution, "claim_idempotency_key_hash", None) == key_hash
            )
        # Full-payload reads are intentionally never replayable in a
        # quarantine.  Outcomes and owner reports are allowlisted routes.
        return False

    async def _authenticate(self, headers: tuple[tuple[str, str], ...], method: str, scope: Mapping[str, Any], path: str) -> VerifiedPrincipal:
        if self.authenticator is not None:
            value = self.authenticator(headers=headers, method=method, scope=scope, path=path)
            if inspect.isawaitable(value):
                value = await value
            if not isinstance(value, VerifiedPrincipal):
                raise AuthenticationError("authentication_required")
            if self.require_identity_barrier and value.auth_method == "trusted_proxy":
                # Trusted-proxy identity does not carry a credential
                # issuance/notBefore fact that can be rechecked against the
                # durable provider/binding barrier.  Restore and lifecycle
                # routes must use direct JWT+DPoP or mTLS evidence.
                raise AuthenticationError("authentication_required")
            validate_verified_principal(
                value,
                require_identity_barrier=self.require_identity_barrier,
            )
            if self.require_auth_audit and (value.auth_method not in {"jwt_dpop", "mtls", "trusted_proxy"} or not value.sender_key_thumbprint):
                raise AuthenticationError("sender_constraint_required")
            return value
        if self.config is None:
            raise AuthenticationError("authentication_required")
        auth = getattr(self.config, "authentication", None)
        extensions = scope.get("extensions", {})
        if not isinstance(extensions, Mapping):
            extensions = {}
        mtls = scope.get("verified_mtls_principal", extensions.get("verified_mtls_principal"))
        if mtls is None:
            # A direct TLS connection may expose only the verified peer
            # certificate. Resolve it before the generic verifier so the
            # certificate-only route does not require Authorization/DPoP.
            adapter = self.mtls_principal_adapter
            if adapter is None:
                adapter = ASGIMTLSPrincipalAdapter(
                    principal_mapping=self.principal_mapping,
                    certificate_mapping=self.mtls_certificate_mapping,
                    issuer=getattr(auth, "issuer", None),
                    require_identity_barrier=self.require_identity_barrier,
                )
            has_tls_material = (
                scope.get("ssl_object") is not None
                or isinstance(extensions.get("tls"), Mapping)
                or isinstance(scope.get("tls"), Mapping)
            )
            if has_tls_material:
                try:
                    value = adapter(scope)
                    if inspect.isawaitable(value):
                        value = await value
                    mtls = value
                except AuthenticationError:
                    raise
                except Exception as exc:
                    raise AuthenticationError("sender_constraint_required") from exc
        proxy = scope.get("trusted_proxy_principal", extensions.get("trusted_proxy_principal"))
        proxy_verified = scope.get("trusted_proxy_verified", extensions.get("trusted_proxy_verified", False)) is True
        if proxy_verified and not getattr(auth, "trusted_proxy_names", ()):
            raise AuthenticationError("authentication_required")
        if proxy_verified:
            proxy_name = scope.get("trusted_proxy_name", extensions.get("trusted_proxy_name"))
            if not isinstance(proxy_name, str) or proxy_name not in getattr(auth, "trusted_proxy_names", ()):
                raise AuthenticationError("authentication_required")
            if self.require_identity_barrier:
                raise AuthenticationError("authentication_required")
        static_enabled = getattr(auth, "static_bearer", False) is True and getattr(auth, "development_loopback_only", True) is True
        return authenticate_request(headers=headers, method=method, uri=self._request_uri(scope, path), deployment=getattr(self.config, "deployment", ""), jwks=getattr(auth, "jwks", {}), issuer=getattr(auth, "issuer", None), audience=getattr(auth, "audience", "deployment_scoped_review_api"), principal_mapping=self.principal_mapping, allowed_algorithms=getattr(auth, "allowed_algorithms", ("EdDSA",)), max_token_lifetime_seconds=getattr(auth, "max_token_lifetime_seconds", 300), clock_skew_seconds=getattr(auth, "clock_skew_seconds", 30), nonce_store=self.nonce_store, mtls_principal=mtls, trusted_proxy_principal=proxy, trusted_proxy=proxy_verified, static_bearer=self.static_bearer_secret if static_enabled else None, static_principal=self.static_bearer_principal if static_enabled else None, is_loopback=self._is_loopback(scope), require_identity_barrier=self.require_identity_barrier)

    @staticmethod
    def _is_loopback(scope: Mapping[str, Any]) -> bool:
        client = scope.get("client")
        return isinstance(client, (tuple, list)) and bool(client) and str(client[0]) in {"127.0.0.1", "::1", "localhost"}

    @staticmethod
    def _request_uri(scope: Mapping[str, Any], path: str) -> str:
        scheme = str(scope.get("scheme", "https"))
        host = ""
        for key, value in scope.get("headers", []):
            key_text = key.decode("ascii").lower() if isinstance(key, bytes) else str(key).lower()
            if key_text == "host":
                host = value.decode("ascii") if isinstance(value, bytes) else str(value)
                break
        if not host:
            server = scope.get("server")
            if isinstance(server, (tuple, list)) and server:
                host = str(server[0])
                if len(server) > 1 and server[1] is not None:
                    host += f":{server[1]}"
        if not host or "@" in host or any(ord(ch) < 0x21 or ord(ch) > 0x7e for ch in host):
            raise AuthenticationError("authentication_required")
        # Keep authority parsing in the verifier's URL normalizer, but reject
        # userinfo, path delimiters, and malformed ports before constructing it.
        if any(ch in host for ch in "/?#\\"):
            raise AuthenticationError("authentication_required")
        return f"{scheme}://{host}{path}"

    def _auth_audit(self, principal: VerifiedPrincipal, request_id: str) -> tuple[Mapping[str, Any], None]:
        """Build safe authentication evidence for deferred service append.

        The API transport must not call an audit writer before authorization:
        doing so leaves an unactionable audit row when Nomos denies the
        request, and cannot bind the row to the resulting decision.  Keep the
        writer argument on the constructor for compatibility with launchers,
        but intentionally do not invoke it here.  ``ExternalActionService``
        consumes this mapping after its same-transaction Nomos evaluation.
        """

        audit = build_auth_audit(principal).as_dict()
        audit["request_id"] = request_id
        auth_config_digest = self.auth_config_digest or self._derived_auth_config_digest()
        if auth_config_digest is not None:
            audit["auth_config_digest"] = auth_config_digest
        elif self.require_auth_audit:
            raise APIError(503, "review_authority_unavailable")
        return audit, None

    def _derived_auth_config_digest(self) -> str | None:
        """Hash only deployment trust metadata, never credentials or tokens."""

        auth = getattr(self.config, "authentication", None)
        if auth is None:
            return None
        projection = {
            "issuer": getattr(auth, "issuer", None),
            "audience": getattr(auth, "audience", None),
            "allowed_algorithms": list(getattr(auth, "allowed_algorithms", ())),
            "max_token_lifetime_seconds": getattr(auth, "max_token_lifetime_seconds", None),
            "clock_skew_seconds": getattr(auth, "clock_skew_seconds", None),
            "jwks": dict(getattr(auth, "jwks", {}) or {}),
            "trust_roots": list(getattr(auth, "trust_roots", ())),
            "trusted_proxy_names": list(getattr(auth, "trusted_proxy_names", ())),
            "static_bearer": bool(getattr(auth, "static_bearer", False)),
            "development_loopback_only": bool(getattr(auth, "development_loopback_only", True)),
        }
        try:
            return domain_digest("okg-external-action-auth-config-v1", projection)
        except SigningError as exc:
            raise APIError(503, "review_authority_unavailable") from exc

    def _enforce_json_limits(self, value: Any) -> None:
        limits = getattr(self.config, "limits", None)
        if limits is None:
            return
        members = arrays = scalars = max_string = max_depth = 0

        def visit(item: Any, depth: int) -> None:
            nonlocal members, arrays, scalars, max_string, max_depth
            max_depth = max(max_depth, depth)
            if max_depth > getattr(limits, "json_nesting_depth_maximum", 32):
                raise APIError(422, "invalid_proposal")
            if isinstance(item, Mapping):
                members += len(item)
                if members > getattr(limits, "total_object_members_maximum", 512):
                    raise APIError(422, "invalid_proposal")
                for key, child in item.items():
                    if not isinstance(key, str):
                        raise APIError(422, "invalid_proposal")
                    max_string = max(max_string, len(key.encode("utf-8")))
                    visit(child, depth + 1)
            elif isinstance(item, list):
                arrays += len(item)
                if arrays > getattr(limits, "total_array_items_maximum", 512):
                    raise APIError(422, "invalid_proposal")
                for child in item:
                    visit(child, depth + 1)
            elif isinstance(item, str):
                max_string = max(max_string, len(item.encode("utf-8")))
                scalars += 1
            else:
                scalars += 1
            if scalars > getattr(limits, "total_scalar_values_maximum", 4096) or max_string > getattr(limits, "individual_string_utf8_bytes_maximum", 16384):
                raise APIError(422, "invalid_proposal")

        visit(value, 0)

    @staticmethod
    def _body(value: Any, *, allowed: set[str], required: set[str] | None = None) -> dict[str, Any]:
        required = required or set()
        if not isinstance(value, Mapping):
            raise APIError(422, "invalid_proposal")
        keys = set(value)
        if not all(isinstance(key, str) for key in keys) or keys - allowed or required - keys:
            raise APIError(422, "invalid_proposal")
        return dict(value)

    def _actor(self, request: APIRequest, audit_ref: str | None) -> Actor:
        if request.principal is None:
            raise AuthenticationError("authentication_required")
        # Identity and sender binding are verifier output.  The caller cannot
        # override either field through the body or an audit reference.
        del audit_ref
        return Actor(
            principal=request.principal.principal,
            separation_subject=request.principal.separation_subject,
            nomos_provider_id=request.principal.provider_id,
            nomos_subject_binding_id=request.principal.subject_binding_id,
            authentication_audit=request.auth_audit,
        )

    def _contract(self, body: Mapping[str, Any], *, action_type: str | None = None) -> Any:
        selected = action_type or body.get("action_type")
        if not isinstance(selected, str) or selected not in self.action_contracts:
            raise APIError(422, "invalid_proposal")
        contract = self.action_contracts[selected]
        supplied = body.get("action_contract_digest")
        expected = getattr(contract, "action_contract_digest", None)
        if supplied is not None and supplied != expected:
            raise APIError(422, "invalid_proposal")
        return contract

    @staticmethod
    def _read_resource(request: APIRequest, route: _Route) -> str:
        """Return the route's canonical resource identifier for authorization.

        Query filtering is deliberately not reconstructed by SQL.  The
        deployment/resource identity is bound in the transition digest and
        the fixed read RPC receives that digest as-is.
        """

        if route.operation == "list":
            return request.path.split("/")[3]
        if route.operation in {"read", "read_payload", "timeline"}:
            if route.proposal_id is None:
                raise APIError(404, "not_found")
            return route.proposal_id
        if route.operation == "recovery_record":
            if route.effect_instance_key is None:
                raise APIError(404, "not_found")
            return route.effect_instance_key
        raise APIError(404, "not_found")

    @classmethod
    def _read_request_hash(cls, request: APIRequest, route: _Route) -> str:
        operation = route.operation
        include_payload = operation == "read_payload"
        try:
            return transition_request_hash(
                {
                    "operation": operation,
                    "resource": cls._read_resource(request, route),
                    "include_payload": include_payload,
                }
            )
        except Exception as exc:
            raise APIError(503, "review_authority_unavailable") from exc

    def _authorize_read(
        self,
        request: APIRequest,
        operation: str,
        audit_ref: str | None = None,
        *,
        request_hash: str | None = None,
    ) -> None:
        # Lifecycle services own the strict Nomos + deferred-audit boundary.
        # Reads must use that same path; calling ``service.authorize``
        # directly would evaluate Nomos without appending the audit evidence
        # after the decision, and would permit a read path to drift from
        # mutation authorization semantics.
        service_boundary = getattr(self.service, "_authorize", None)
        if callable(service_boundary):
            actor = self._actor(request, audit_ref)
            context = {
                "deployment": getattr(self.config, "deployment", ""),
                "request_id": request.request_id,
                "auth_config_digest": self.auth_config_digest or self._derived_auth_config_digest(),
                "surface": "external_actions",
            }
            if request_hash is not None:
                context["request_hash"] = request_hash
            try:
                service_boundary(operation, actor, context=context)
            except Exception as exc:
                if getattr(exc, "code", None) == "unauthorized":
                    # Reads do not disclose whether an id exists.  A denied
                    # read is intentionally indistinguishable from an
                    # unknown proposal, while authority outages remain 503.
                    raise APIError(404, "proposal_not_found") from exc
                raise APIError(503, "review_authority_unavailable") from exc
            return
        authorizer = getattr(self.service, "authorize", None)
        if not callable(authorizer):
            evaluator = getattr(authorizer, "evaluate_runtime_boundary_with_active_policy", None)
            if callable(evaluator):
                from .authorization import authorize as authorize_with_nomos

                store = getattr(self.service, "store", None)
                try:
                    result = authorize_with_nomos(
                        authorizer,
                        operation=operation,
                        principal=self._actor(request, audit_ref),
                        deployment=getattr(self.config, "deployment", ""),
                        transaction=getattr(store, "connection", None),
                        storage_adapter=getattr(store, "nomos_storage_adapter", None),
                        context={
                            "request_id": request.request_id,
                            "surface": "external_actions",
                            **({"request_hash": request_hash} if request_hash is not None else {}),
                        },
                    )
                except Exception as exc:
                    if getattr(exc, "code", None) == "unauthorized":
                        raise APIError(404, "proposal_not_found") from exc
                    raise APIError(503, "review_authority_unavailable") from exc
            elif self.require_auth_audit or getattr(getattr(self.service, "store", None), "is_durable", False) is True:
                raise APIError(503, "review_authority_unavailable")
            else:
                return
        else:
            actor = self._actor(request, audit_ref)
            store = getattr(self.service, "store", None)
            try:
                parameters = inspect.signature(authorizer).parameters
            except (TypeError, ValueError):
                parameters = {}
            accepts_kwargs = any(parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in parameters.values())
            kwargs: dict[str, Any] = {}
            optional = {
                "transaction": getattr(store, "connection", None),
                "storage_adapter": getattr(store, "nomos_storage_adapter", None),
                "context": {
                    "request_id": request.request_id,
                    "surface": "external_actions",
                    **({"request_hash": request_hash} if request_hash is not None else {}),
                },
            }
            for key, value in optional.items():
                if accepts_kwargs or key in parameters:
                    kwargs[key] = value
            try:
                result = authorizer(operation, actor, **kwargs)
            except TypeError:
                if kwargs:
                    result = authorizer(operation, actor)
                else:
                    raise
            if inspect.isawaitable(result):
                raise APIError(503, "review_authority_unavailable")
        if result is True and not (self.require_auth_audit or getattr(getattr(self.service, "store", None), "is_durable", False) is True):
            return
        if isinstance(result, Mapping):
            allowed = result.get("allowed")
            effect = result.get("effect", result.get("decision_effect"))
            obligations = result.get("obligations_performed", False)
        else:
            allowed = getattr(result, "allowed", False)
            effect = getattr(result, "effect", None)
            obligations = getattr(result, "obligations_performed", False)
        if allowed is True and effect == "allow" and obligations is True:
            return
        if allowed is False:
            raise APIError(404, "proposal_not_found")
        raise APIError(503, "review_authority_unavailable")

    async def dispatch(self, request: APIRequest, route: _Route | None = None, *, audit_ref: str | None = None) -> tuple[int, Mapping[str, str], Mapping[str, Any]]:
        if self.service is None:
            raise APIError(503, "review_authority_unavailable")
        route = route or self._route(request.method, request.path)
        if route.operation in {"run_register", "run_read", "run_link"}:
            if request.query or (route.operation == "run_read" and request.body):
                raise APIError(422, "invalid_run_record")
            # Run SQL owns quarantine/replay decisions after both fresh auth
            # cycles; never perform an unauthenticated existence lookup here.
            return self._invoke("run_record", {
                "operation": route.operation.removeprefix("run_"),
                "deployment": request.path.split("/")[3],
                "actor": self._actor(request, audit_ref),
                "record_id": route.record_id,
                "body": request.json,
                "idempotency_key": self._required_header(request, "idempotency-key") if route.operation != "run_read" else None,
            }, request)
        if self._mode() == "restore_only" and self._blocked_in_restore_only(route):
            # The durable command is allowed to resolve one exact committed
            # replay (and only its redacted response) before the quarantine
            # refusal.  New keys never reach a lifecycle method.
            if not self._has_restore_only_replay_candidate(route, request):
                raise APIError(409, "external_action_restore_quarantined")
        if route.operation in {"read", "read_payload", "list", "timeline", "recovery_record"}:
            read_hash = self._read_request_hash(request, route)
            read_operation = (
                "external_action.read_payload"
                if route.operation == "read_payload"
                else "external_action.read"
            )
            authorized_read = getattr(self.service, "authorized_read", None)
            if callable(authorized_read):
                query = request.query or {}
                actor = self._actor(request, audit_ref)
                args = {
                    "operation": route.operation,
                    "resource": self._read_resource(request, route),
                    "request_hash": read_hash,
                    "deployment": getattr(self.config, "deployment", None) or request.path.split("/")[3],
                    "actor": actor,
                    "proposal_id": route.proposal_id,
                    "effect_instance_key": route.effect_instance_key,
                    "include_payload": route.operation == "read_payload",
                    "state": query.get("state"),
                    "limit": int(query["limit"]) if "limit" in query else None,
                }
                try:
                    return self._invoke("authorized_read", args, request)
                except Exception as exc:
                    if getattr(exc, "code", None) == "unauthorized":
                        # Reads do not disclose whether an id exists.
                        raise APIError(404, "proposal_not_found") from exc
                    raise
            # Keep strict Nomos evaluation, its deferred authentication audit,
            # expiry materialization, and the returned projection on the same
            # durable transaction.  Otherwise a policy/identity mutation could
            # race between an allow decision and a full-payload read.
            store = getattr(self.service, "store", None)
            lock = getattr(store, "lock", None)
            if lock is None and getattr(store, "is_durable", False) is True:
                raise APIError(503, "review_authority_unavailable")
            with (lock if lock is not None else nullcontext()):
                self._authorize_read(
                    request,
                    read_operation,
                    audit_ref,
                    request_hash=read_hash,
                )
                return self._dispatch_authorized_read(request, route)
        actor = self._actor(request, audit_ref)
        body = request.json
        if route.operation == "submit":
            submit_method = getattr(self.service, "submit", None)
            if submit_method is not None and "deployment" not in inspect.signature(submit_method).parameters:
                return self._invoke("submit", {}, request)
            payload = self._body(body, allowed={"automation_id", "automation_version", "run_id", "correlation_id", "action_type", "target_ref", "risk_tier", "action_schema_digest", "action_contract_digest", "requested_review_expires_at", "summary_code", "payload", "evidence_digests"}, required={"automation_id", "automation_version", "run_id", "correlation_id", "action_type", "target_ref", "risk_tier", "action_schema_digest", "requested_review_expires_at", "summary_code", "payload"})
            # Pass the closed request mapping through to the service.  The
            # service resolves the deployment-pinned registry only for a new
            # key; a committed replay uses its retained contract record so a
            # registry rotation cannot turn a safe retry into an outage.
            args = {**payload, "deployment": request.path.split("/")[3], "actor": actor, "idempotency_key": self._required_header(request, "idempotency-key"), "action_contract": payload}
            return self._invoke("submit", args, request)
        if route.operation == "edit":
            payload = self._body(body, allowed={"replacement_payload", "payload", "action_contract_digest"})
            if "replacement_payload" not in payload and "payload" not in payload:
                raise APIError(422, "invalid_proposal")
            contract_request = {**payload, "action_type": self._stored_action_type(route.proposal_id)}
            args = {"proposal_id": route.proposal_id, "actor": actor, "idempotency_key": self._required_header(request, "idempotency-key"), "replacement_payload": payload.get("replacement_payload", payload.get("payload")), "action_contract": contract_request, "if_match": self._required_header(request, "if-match")}
            return self._invoke("edit", args, request)
        if route.operation == "decide":
            payload = self._body(body, allowed={"verdict", "execute_before", "reason_code", "replacement_payload", "payload", "action_contract_digest"}, required={"verdict", "reason_code"})
            contract_request = {**payload, "action_type": self._stored_action_type(route.proposal_id)}
            args = {"proposal_id": route.proposal_id, "actor": actor, "idempotency_key": self._required_header(request, "idempotency-key"), "verdict": payload["verdict"], "action_contract": contract_request, "execute_before": payload.get("execute_before"), "reason_code": payload["reason_code"], "replacement_payload": payload.get("replacement_payload", payload.get("payload")), "if_match": self._required_header(request, "if-match")}
            return self._invoke("decide", args, request)
        if route.operation == "cancel":
            payload = self._body(body, allowed={"reason_code", "expected_decision_id", "expected_decision_binding_hash"}, required={"reason_code"})
            contract_request = {**payload, "action_type": self._stored_action_type(route.proposal_id)}
            args = {"proposal_id": route.proposal_id, "actor": actor, "idempotency_key": self._required_header(request, "idempotency-key"), "action_contract": contract_request, "reason_code": payload["reason_code"], "expected_decision_id": payload.get("expected_decision_id"), "expected_decision_binding_hash": payload.get("expected_decision_binding_hash"), "if_match": self._required_header(request, "if-match")}
            return self._invoke("cancel", args, request)
        if route.operation == "claim":
            self._required_header(request, "if-match")
            allowed = {"decision_id", "approved_lifecycle_version", "approved_payload_version", "approved_payload_hash", "supported_action_schema_digest", "supported_action_contract_digest", "supported_executor_contract_digest", "supported_payload_renderer_digest", "supported_governed_tool_binding_digest", "supported_provider_connection_contract_digest", "supported_provider_capability_digest", "supported_provider_operation_id", "supported_provider_operation_descriptor_digest"}
            payload = self._body(body, allowed=allowed, required=allowed)
            return self._invoke("claim", {**payload, "proposal_id": route.proposal_id, "actor": actor, "idempotency_key": self._required_header(request, "idempotency-key"), "if_match": self._required_header(request, "if-match")}, request)
        if route.operation == "report":
            self._required_header(request, "if-match")
            payload = self._body(body, allowed={"effect_instance_key", "outcome", "outcome_reason_code", "provider_receipt_digest", "recovery_journal_digest", "owner_unavailability_attestation_digest"}, required={"outcome", "outcome_reason_code", "recovery_journal_digest"})
            args = {**payload, "proposal_id": route.proposal_id, "execution_id": route.execution_id, "actor": actor, "idempotency_key": self._required_header(request, "idempotency-key"), "if_match": self._required_header(request, "if-match")}
            args.setdefault("provider_receipt_digest", None)
            if "effect_instance_key" in args:
                args["effect_instance_key_value"] = args.pop("effect_instance_key")
            # Reconciler status is derived from the retained claim owner, not
            # from a caller-controlled body flag.  The service still applies
            # its own attestation, outcome-vocabulary, and Nomos checks.
            owner = self._execution_owner(route.proposal_id, route.execution_id)
            args["reconciler"] = owner is not None and owner != actor.separation_subject
            return self._invoke("report_outcome", args, request)
        if route.operation == "attest_owner_unavailable":
            payload = self._opaque_body(body)
            if {"principal", "actor", "separation_subject", "deployment", "execution_id"} & set(payload):
                raise APIError(422, "invalid_proposal")
            return self._invoke("create_owner_unavailability_attestation", {"proposal_id": route.proposal_id, "execution_id": route.execution_id, "actor": actor, "attestation": payload, "idempotency_key": self._required_header(request, "idempotency-key")}, request)
        if route.operation == "restore_create":
            payload = self._body(body, allowed={"restore_id", "old_epoch_lease_envelope_ref", "new_epoch_lease_envelope_ref", "worker_quiescence_envelope_ref", "provider_authority_fence_envelope_ref", "ledger_fence_envelope_ref", "restore_attestation_envelope_ref"}, required={"restore_id", "old_epoch_lease_envelope_ref", "new_epoch_lease_envelope_ref", "worker_quiescence_envelope_ref", "provider_authority_fence_envelope_ref", "ledger_fence_envelope_ref", "restore_attestation_envelope_ref"})
            validate_path_identifier(payload["restore_id"], kind="restore_id")
            return self._invoke_any(("create_restore_reconciliation", "restore_create"), {"actor": actor, "request": payload, "idempotency_key": self._required_header(request, "idempotency-key")}, request)
        if route.operation == "restore_batch":
            payload = self._body(body, allowed={"batch_index", "checkpoints"}, required={"batch_index", "checkpoints"})
            if payload["batch_index"] != route.batch_index:
                raise APIError(422, "invalid_proposal")
            return self._invoke_any(("upload_checkpoint_batch", "restore_batch"), {"restore_id": route.restore_id, "batch_index": route.batch_index, "actor": actor, "request": payload, "idempotency_key": self._required_header(request, "idempotency-key")}, request)
        if route.operation == "restore_finalize":
            payload = self._body(body, allowed={"restore_attestation_envelope_digest", "expected_inventory_digest", "expected_inventory_count", "expected_checkpoint_set_digest", "expected_checkpoint_count"}, required={"restore_attestation_envelope_digest", "expected_inventory_digest", "expected_inventory_count", "expected_checkpoint_set_digest", "expected_checkpoint_count"})
            return self._invoke_any(("finalize_restore_reconciliation", "restore_finalize"), {"restore_id": route.restore_id, "actor": actor, "request": payload, "idempotency_key": self._required_header(request, "idempotency-key")}, request)
        raise APIError(404, "not_found")

    def _dispatch_authorized_read(
        self,
        request: APIRequest,
        route: _Route,
    ) -> tuple[int, Mapping[str, str], Mapping[str, Any]]:
        """Return one read while the caller holds the durable store lock."""

        if route.operation == "list":
            query = request.query or {}
            deployment = getattr(self.config, "deployment", None) or request.path.split("/")[3]
            rows = self.service.list(deployment=deployment, state=query.get("state"))
            if "limit" in query:
                rows = tuple(rows[: int(query["limit"])])
            return 200, {}, {"proposals": list(rows)}
        if route.operation in {"read", "read_payload"}:
            return self._invoke(
                "read",
                {
                    "proposal_id": route.proposal_id,
                    "include_payload": route.operation == "read_payload",
                },
                request,
            )
        if route.operation == "timeline":
            return 200, {}, {"events": list(self.service.timeline(route.proposal_id))}
        if route.operation == "recovery_record":
            return self._invoke_any(
                ("recovery_record", "get_recovery_record"),
                {"effect_instance_key": route.effect_instance_key},
                request,
            )
        raise APIError(404, "not_found")

    def _stored_action_type(self, proposal_id: str | None) -> str | None:
        store = getattr(self.service, "store", None)
        proposal = getattr(store, "get", lambda _id: None)(proposal_id) if store is not None else None
        return getattr(proposal, "action_type", None)

    def _execution_owner(self, proposal_id: str | None, execution_id: str | None) -> str | None:
        if not proposal_id or not execution_id:
            return None
        store = getattr(self.service, "store", None)
        getter = getattr(store, "get_execution", None)
        try:
            execution = getter(proposal_id) if callable(getter) else None
        except Exception:
            # Let the typed report call perform the authoritative lookup and
            # return its safe lifecycle error; do not expose store failures.
            return None
        if execution is None or getattr(execution, "execution_id", None) != execution_id:
            return None
        owner = getattr(execution, "worker_separation_subject", None)
        return owner if isinstance(owner, str) else None

    @staticmethod
    def _required_header(request: APIRequest, name: str) -> str:
        value = _one_header(request.headers, name)
        if value is None:
            raise APIError(428 if name == "if-match" else 422, "proposal_precondition_required" if name == "if-match" else "invalid_proposal")
        if name == "if-match":
            parse_if_match(value)
            return value
        return validate_idempotency_key(value)

    @staticmethod
    def _opaque_body(value: Any) -> dict[str, Any]:
        if not isinstance(value, Mapping):
            raise APIError(422, "invalid_proposal")
        forbidden = {"principal", "actor", "separation_subject", "deployment", "action_contract", "proposer", "approver", "worker"}
        if forbidden & set(value):
            raise APIError(422, "invalid_proposal")
        return dict(value)

    def _invoke_any(self, names: Iterable[str], args: dict[str, Any], request: APIRequest) -> tuple[int, Mapping[str, str], Mapping[str, Any]]:
        for name in names:
            if callable(getattr(self.service, name, None)):
                return self._invoke(name, args, request)
        raise APIError(503, "review_authority_unavailable")

    def _invoke(self, name: str, args: dict[str, Any], request: APIRequest) -> tuple[int, Mapping[str, str], Mapping[str, Any]]:
        method = getattr(self.service, name, None)
        if method is None:
            raise APIError(503, "review_authority_unavailable")
        if name == "submit" and not inspect.signature(method).parameters.get("deployment"):
            value = method(request)
        else:
            try:
                value = method(**args)
            except Exception as exc:
                # Report authorization happens before outcome semantics in
                # the service.  Keep the same opaque response at the
                # transport boundary for compatible service adapters that
                # expose the authorization error directly.
                if request.operation == "report" and args.get("reconciler") is True and getattr(exc, "code", None) == "unauthorized":
                    raise APIError(404, "proposal_not_found") from exc
                raise
        if inspect.isawaitable(value):
            raise APIError(503, "review_authority_unavailable")
        normalized = self._normalize_service_result(value, request=request)
        if (
            self._mode() == "restore_only"
            and request.operation in {"submit", "edit", "decide", "cancel", "claim"}
            and bool(getattr(value, "replay", False))
        ):
            status, headers, projection = normalized
            # A replay is useful for retry convergence but must not reopen a
            # payload or a provider handoff while the restored database is
            # quarantined.  Keep only the non-executable identity/state view.
            redacted = {
                key: projection[key]
                for key in (
                    "proposal_id", "deployment", "execution_id", "decision_id",
                    "review_state", "execution_state", "lifecycle_version",
                    "payload_version", "current_payload_hash", "approved_payload_hash",
                    "outcome", "outcome_reason_code", "outcome_request_hash", "etag",
                )
                if key in projection
            }
            return status, headers, redacted
        return normalized

    @staticmethod
    def _normalize_service_result(value: Any, *, request: APIRequest | None = None) -> tuple[int, Mapping[str, str], Mapping[str, Any]]:
        if isinstance(value, tuple) and len(value) == 3:
            return int(value[0]), dict(value[1]), dict(value[2])
        status = int(getattr(value, "status", 200))
        replay = bool(getattr(value, "replay", False))
        raw = getattr(value, "value", value)
        include_payload = bool(request and request.operation == "read_payload")
        # Durable idempotency records retain the exact canonical response for
        # every mutation, not only full-payload reads.  Prefer those bytes for
        # all operations so an exact historical replay is not reconstructed
        # from the proposal's later state.
        response_body = getattr(value, "response_body", None)
        if isinstance(response_body, bytes) and response_body:
            try:
                parsed = strict_json_loads(response_body)
            except SigningError as exc:
                raise APIError(503, "review_authority_unavailable") from exc
            projection = parsed if isinstance(parsed, Mapping) else None
        elif hasattr(raw, "to_projection"):
            if include_payload:
                payload = getattr(raw, "canonical_payload", None) or getattr(value, "canonical_payload", None)
                projection = raw.to_projection(include_payload=True, payload=payload)
            else:
                projection = raw.to_projection()
        elif isinstance(raw, Mapping):
            projection = dict(raw)
        else:
            projection = getattr(value, "to_dict", lambda: {"value": raw})()
        if not isinstance(projection, Mapping):
            raise APIError(503, "review_authority_unavailable")
        projection = dict(projection)
        headers: dict[str, str] = {}
        if isinstance(projection.get("etag"), str):
            headers["etag"] = projection["etag"]
        if replay:
            headers["idempotent-replay"] = "true"
        if status == 201 and projection.get("proposal_id") and request is not None:
            headers["location"] = f"/v1/deployments/{request.path.split('/')[3]}/external-actions/{projection['proposal_id']}"
        return status, headers, projection


def build_api_app(*args: Any, **kwargs: Any) -> ExternalActionAPI:
    return ExternalActionAPI(*args, **kwargs)


__all__ = ["APIError", "APIRequest", "ExternalActionAPI", "build_api_app", "validate_safe_identifier", "validate_idempotency_key", "parse_if_match", "validate_path_identifier"]
