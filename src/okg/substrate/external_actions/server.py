"""Process entry point for the standalone external-action review API."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Mapping
import inspect
import os
import re
import ssl

from .api import ExternalActionAPI
from .config import (
    ConfigError,
    ExternalActionConfig,
    load_action_contracts,
    load_config,
    load_deployment_product_authority,
)
from .restore_fence import pre_bind_restore_fence
from .signing import SigningError, domain_digest
from .store import (
    ExternalActionStoreError,
    PostgresExternalActionStore,
)


class ServerStartupError(RuntimeError):
    pass


def mtls_uvicorn_protocol_class() -> type[Any]:
    """Return Uvicorn's HTTP protocol with a concrete peer-cert handoff.

    Uvicorn 0.51 does not put its transport or ``ssl_object`` in the ASGI
    scope.  The normal TLS settings therefore establish mTLS but do not, by
    themselves, give the application a certificate to authenticate.  This
    protocol wraps each connection's app and copies the certificate from the
    live transport into the private ``extensions.tls`` handoff consumed by
    :class:`ASGIMTLSPrincipalAdapter`.  No HTTP header or caller-provided
    scope value can select the certificate.

    The import stays lazy so importing the storage/API modules does not make
    Uvicorn a runtime dependency.  A launcher requesting mTLS fails closed if
    the installed Uvicorn does not expose the supported protocol classes.
    """

    try:
        from uvicorn.protocols.http.h11_impl import H11Protocol
    except (ImportError, AttributeError) as exc:  # pragma: no cover - env-specific
        raise ServerStartupError("Uvicorn mTLS peer-certificate adapter is unavailable") from exc

    class PeerCertificateH11Protocol(H11Protocol):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            application = self.app

            async def handoff(scope: Mapping[str, Any], receive: Any, send: Any) -> None:
                if scope.get("type") != "http":
                    await application(scope, receive, send)
                    return
                transport = getattr(self, "transport", None)
                ssl_object = transport.get_extra_info("ssl_object") if transport is not None else None
                getter = getattr(ssl_object, "getpeercert", None)
                certificate = None
                if callable(getter):
                    try:
                        certificate = getter(binary_form=True)
                    except (OSError, TypeError, ValueError):
                        certificate = None
                verified = getattr(ssl_object, "verify_mode", None) == ssl.CERT_REQUIRED
                extensions = scope.get("extensions", {})
                if not isinstance(extensions, Mapping):
                    extensions = {}
                tls = extensions.get("tls", {})
                if not isinstance(tls, Mapping):
                    tls = {}
                # Keep this extension closed.  Values are transport-derived;
                # in particular, a client cannot set ``verified`` or replace
                # a certificate by sending an HTTP field.
                tls = {
                    "verified": verified,
                    "client_cert": certificate,
                }
                forwarded_scope = dict(scope)
                forwarded_scope["extensions"] = {**dict(extensions), "tls": tls}
                await application(forwarded_scope, receive, send)

            self.app = handoff

    PeerCertificateH11Protocol.__name__ = "PeerCertificateH11Protocol"
    return PeerCertificateH11Protocol


def _compiled_action_registry(
    source: Mapping[str, Any] | str | Path,
    *,
    authority: tuple[Any, Any, Any] | None = None,
) -> dict[str, Any]:
    """Load and semantically validate the deployment-pinned action registry."""

    try:
        raw = load_action_contracts(source)
    except Exception as exc:
        # Do not let schema/library details escape the pre-bind boundary.
        raise ServerStartupError("action registry is unavailable; refusing to bind") from exc
    if authority is not None:
        from .action_contracts import resolve_action_contracts

        try:
            return resolve_action_contracts(
                source,
                authored_definition=authority[0],
                resolved_lock=authority[1],
            )
        except Exception as exc:
            raise ServerStartupError(
                "action registry is not bound to the deployment product authority; refusing to bind"
            ) from exc
    actions = raw.get("actions") if isinstance(raw, Mapping) else None
    if not isinstance(actions, list):
        raise ServerStartupError("action registry has no actions; refusing to bind")
    from .action_contracts import ActionContract, ActionContractError

    compiled: dict[str, Any] = {}
    try:
        for action in actions:
            contract = ActionContract.from_mapping(action)
            if contract.action_type in compiled:
                raise ServerStartupError("action registry contains duplicate action types; refusing to bind")
            compiled[contract.action_type] = contract
    except (ActionContractError, TypeError, ValueError) as exc:
        raise ServerStartupError("action registry contains an invalid contract; refusing to bind") from exc
    if not compiled:
        raise ServerStartupError("action registry is empty; refusing to bind")
    return compiled


def _authorizer_available(value: Any) -> bool:
    return callable(value) or callable(getattr(value, "evaluate_runtime_boundary_with_active_policy", None))


def _auth_config_digest(config: ExternalActionConfig) -> str:
    auth = config.authentication
    projection = {
        "issuer": auth.issuer,
        "audience": auth.audience,
        "allowed_algorithms": list(auth.allowed_algorithms),
        "max_token_lifetime_seconds": auth.max_token_lifetime_seconds,
        "clock_skew_seconds": auth.clock_skew_seconds,
        "jwks": dict(auth.jwks),
        "trust_roots": list(auth.trust_roots),
        "trusted_proxy_names": list(auth.trusted_proxy_names),
        "static_bearer": auth.static_bearer,
        "development_loopback_only": auth.development_loopback_only,
    }
    try:
        return domain_digest("okg-external-action-auth-config-v1", projection)
    except SigningError as exc:  # pragma: no cover - config validation catches this
        raise ServerStartupError("authentication configuration cannot be hashed") from exc


def _config_action_registry(loaded: ExternalActionConfig) -> dict[str, Any]:
    source = loaded.action_registry_path
    if not source:
        raise ServerStartupError("--action-contracts is required; refusing to bind without an action registry")
    product_authority = loaded.deployment_product_authority
    if product_authority is None:
        raise ServerStartupError(
            "deployment product authority is required; refusing to bind without an authored definition and product plan"
        )
    try:
        verified = load_deployment_product_authority(product_authority, deployment=loaded.deployment)
    except (ConfigError, OSError, TypeError, ValueError) as exc:
        raise ServerStartupError(
            "deployment product authority is unavailable; refusing to bind"
        ) from exc
    return _compiled_action_registry(source, authority=verified)


def _configured_product_authority(loaded: ExternalActionConfig) -> tuple[Any, Any, Any]:
    authority = loaded.deployment_product_authority
    if authority is None:
        raise ServerStartupError(
            "deployment product authority is required; refusing to bind without an authored definition and product plan"
        )
    try:
        return load_deployment_product_authority(authority, deployment=loaded.deployment)
    except (ConfigError, OSError, TypeError, ValueError) as exc:
        raise ServerStartupError("deployment product authority is unavailable; refusing to bind") from exc


def _require_runtime_store_contract(store: Any) -> None:
    """Prove that a supplied store enforces the live PostgreSQL authority.

    ``is_durable`` is only a capability marker.  It is not verifier authority:
    an embedder can set that attribute on any object.  Every listener boundary
    therefore requires and executes the store's closed runtime-contract check.
    """

    if getattr(store, "is_durable", False) is not True:
        raise ServerStartupError("durable external-action store is required; refusing to bind")
    verifier = getattr(store, "require_runtime_contract", None)
    if not callable(verifier):
        raise ServerStartupError(
            "durable runtime login contract is unavailable; refusing to bind"
        )
    try:
        verifier()
    except Exception as exc:
        raise ServerStartupError(
            "durable runtime login contract is unavailable; refusing to bind"
        ) from exc


def _startup_verification_config(loaded: ExternalActionConfig) -> dict[str, Any]:
    """Build the closed startup trust projection from deployment config."""

    # The startup digest is the complete deployment trust projection, not
    # only the epoch-lease keys.  Restore verification uses the worker,
    # ledger, provider, and operator key history later in the same process;
    # omitting any of those keys would let restart trust context drift while
    # retaining the old digest.
    key_history = [
        {
            "kid": key.kid,
            "purpose": key.purpose,
            "algorithm": key.algorithm,
            "public_key_b64": key.public_key_b64,
            "state": key.state,
            "not_before": key.not_before,
            "not_after": key.not_after,
            "compromise_revoked_at": key.compromise_revoked_at,
        }
        for key in loaded.signing_keys
    ]
    key_history.sort(key=lambda item: item["kid"])
    if not any(item["purpose"] == "execution_epoch_lease" for item in key_history) or len({item["kid"] for item in key_history}) != len(key_history):
        raise ServerStartupError("execution epoch signing-key history is unavailable; refusing to bind")
    algorithms = sorted({item["algorithm"] for item in key_history})
    if not algorithms or any(not isinstance(item, str) or not item for item in algorithms):
        raise ServerStartupError("startup verification algorithms are unavailable; refusing to bind")
    return {
        "schema_version": 1,
        "deployment": loaded.deployment,
        "lease_typ": "okg-execution-epoch+jws",
        "allowed_algorithms": algorithms,
        "key_history": key_history,
        "maximum_chain_envelopes": 64,
        "maximum_chain_bytes": 262_144,
        "maximum_clock_skew_seconds": loaded.authentication.clock_skew_seconds,
    }


_PG_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]{0,62}$")
_UTC_TIMESTAMP_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$")


def _startup_login_binding(store: Any, *, deployment: str) -> Mapping[str, Any]:
    """Read the login binding through the fixed store-owned authority."""

    method = None
    for name in ("startup_login_binding", "get_startup_login_binding", "resolve_startup_login_binding"):
        candidate = getattr(store, name, None)
        if callable(candidate):
            method = candidate
            break
    if method is None:
        raise ServerStartupError("durable startup login-binding authority is unavailable; refusing to bind")
    try:
        parameters = inspect.signature(method).parameters
        deployment_parameter = parameters.get("deployment")
        if deployment_parameter is None:
            value = method()
        elif deployment_parameter.kind is inspect.Parameter.POSITIONAL_ONLY:
            value = method(deployment)
        else:
            value = method(deployment=deployment)
    except Exception as exc:
        raise ServerStartupError("durable startup login-binding authority is unavailable; refusing to bind") from exc
    if not isinstance(value, Mapping):
        raise ServerStartupError("durable startup login-binding authority is unavailable; refusing to bind")
    required = {
        "database_system_identifier",
        "session_user",
        "deployment",
        "binding_version",
        "verifier_capability_generation",
        "bound_at",
    }
    if set(value) != required:
        raise ServerStartupError("startup login binding is not a closed projection; refusing to bind")
    system_id = value["database_system_identifier"]
    if not isinstance(system_id, str) or not re.fullmatch(r"[0-9]+", system_id):
        raise ServerStartupError("startup login binding has an invalid database identifier; refusing to bind")
    if not isinstance(value["session_user"], str) or not _PG_IDENTIFIER_RE.fullmatch(value["session_user"]):
        raise ServerStartupError("startup login binding has an invalid session user; refusing to bind")
    if value["deployment"] != deployment:
        raise ServerStartupError("startup login binding does not match deployment; refusing to bind")
    if isinstance(value["binding_version"], bool) or value["binding_version"] != 1 or not isinstance(value["binding_version"], int):
        raise ServerStartupError("startup login binding has an invalid version; refusing to bind")
    capability_generation = value["verifier_capability_generation"]
    if (
        isinstance(capability_generation, bool)
        or not isinstance(capability_generation, int)
        or capability_generation <= 0
    ):
        raise ServerStartupError(
            "startup login binding has an invalid verifier capability generation; refusing to bind"
        )
    if not isinstance(value["bound_at"], str) or not _UTC_TIMESTAMP_RE.fullmatch(value["bound_at"]):
        raise ServerStartupError("startup login binding has an invalid timestamp; refusing to bind")
    return dict(value)


def _current_signing_key(loaded: ExternalActionConfig, *, purpose: str) -> Any:
    current = [
        key
        for key in loaded.signing_keys
        if key.purpose == purpose and key.state == "current"
    ]
    if len(current) != 1:
        raise ServerStartupError(
            f"exactly one current {purpose} signing key is required; refusing to bind"
        )
    return current[0]


def _owner_attestation_options(loaded: ExternalActionConfig) -> tuple[dict[str, Any], int | None]:
    keys = {
        key.kid: key for key in loaded.signing_keys
        if key.purpose == "owner_unavailability_attestation"
    }
    policy = loaded.policy or {}
    maximum = policy.get("owner_attestation_maximum_lifetime_seconds") if isinstance(policy, Mapping) else None
    if maximum is not None and (
        isinstance(maximum, bool)
        or not isinstance(maximum, int)
        or not 1 <= maximum <= 86_400
    ):
        raise ServerStartupError(
            "owner attestation maximum lifetime must be in 1..86400 seconds; refusing to bind"
        )
    return keys, maximum


def _claim_owner_status_authority(store: Any) -> Any:
    """Return a deployment-owned claim-owner status verifier when exposed."""

    for name in (
        "claim_owner_status_verifier",
        "verify_claim_owner_status",
        "verify_claim_owner_unavailable",
        "claim_owner_status",
    ):
        candidate = getattr(store, name, None)
        if callable(candidate):
            return candidate
    return None


def _handoff_signer(
    loaded: ExternalActionConfig,
    *,
    service: Any,
    store: Any,
    supplied: Callable[[Mapping[str, Any]], Mapping[str, str]] | None,
) -> tuple[Any, Callable[[Mapping[str, Any]], Mapping[str, str]]]:
    key = _current_signing_key(loaded, purpose="claim_and_restore_completion")
    candidate = supplied
    if candidate is None and service is not None:
        candidate = getattr(service, "handoff_signer", None)
    if candidate is None:
        candidate = getattr(store, "claim_handoff_signer", None)
    if not callable(candidate):
        raise ServerStartupError("deployment claim handoff signer is unavailable; refusing to bind")
    candidate_kid = getattr(candidate, "kid", getattr(candidate, "key_id", None))
    if candidate_kid is not None and candidate_kid != key.kid:
        raise ServerStartupError("claim handoff signer is not bound to the current deployment key; refusing to bind")
    candidate_public = getattr(candidate, "public_key_b64", None)
    if candidate_public is not None and candidate_public != key.public_key_b64:
        raise ServerStartupError("claim handoff signer is not bound to the current deployment key; refusing to bind")
    return key, candidate


def _service_kwargs(
    *,
    store: Any,
    authorize: Any,
    registry: Mapping[str, Any],
    handoff_signer: Callable[[Mapping[str, Any]], Mapping[str, str]],
    handoff_verification_key: Any,
    owner_attestation_keys: Mapping[str, Any],
    owner_attestation_maximum_lifetime_seconds: int | None,
) -> dict[str, Any]:
    """Pass deployment-owned hooks while tolerating older test doubles."""

    kwargs: dict[str, Any] = {
        "store": store,
        "authorize": authorize,
        "action_contracts": registry,
        "handoff_signer": handoff_signer,
        "handoff_verification_key": handoff_verification_key,
    }
    try:
        signature = inspect.signature(__import__("okg.substrate.external_actions.service", fromlist=["ExternalActionService"]).ExternalActionService)
        parameters = signature.parameters
        accepts_any = any(item.kind == inspect.Parameter.VAR_KEYWORD for item in parameters.values())
        extras = {
            "owner_attestation_keys": owner_attestation_keys,
            "owner_attestation_maximum_lifetime_seconds": owner_attestation_maximum_lifetime_seconds,
            "claim_owner_status_verifier": _claim_owner_status_authority(store),
        }
        for name, value in extras.items():
            if accepts_any or name in parameters:
                kwargs[name] = value
    except (ImportError, TypeError, ValueError):
        pass
    return kwargs


def create_app(
    *,
    config: ExternalActionConfig | Mapping[str, Any] | str | Path,
    epoch_lease_chain: bytes | str | Path | Mapping[str, Any],
    service: Any = None,
    store: Any = None,
    authenticator: Callable[..., Any] | None = None,
    mode_provider: Callable[[], str] | None = None,
    nonce_store: Any = None,
    login_binding: Mapping[str, Any] | None = None,
    verification_config: Mapping[str, Any] | None = None,
    action_contracts: Mapping[str, Any] | str | Path | None = None,
    authorizer: Callable[..., Any] | None = None,
    nomos_authorizer: Callable[..., Any] | None = None,
    auth_audit_writer: Callable[..., Any] | None = None,
    auth_config_digest: str | None = None,
    handoff_signer: Callable[[Mapping[str, Any]], Mapping[str, str]] | None = None,
    mtls_principal_adapter: Callable[..., Any] | None = None,
    mtls_certificate_mapping: Mapping[str, Any] | Callable[..., Any] | None = None,
) -> ExternalActionAPI:
    """Validate and persist the startup fence before returning an ASGI app.

    ``epoch_lease_chain`` is mandatory.  The returned app is created only
    after the pre-bind transaction commits; callers can safely hand it to an
    ASGI server because no listener is opened by this function.
    """

    try:
        loaded = config if isinstance(config, ExternalActionConfig) else load_config(config)
    except Exception as exc:
        raise ServerStartupError("review configuration is unavailable; refusing to bind") from exc
    if authorizer is not None and nomos_authorizer is not None and authorizer is not nomos_authorizer:
        raise ServerStartupError("multiple authorization authorities are configured; refusing to bind")
    authorizer = authorizer or nomos_authorizer
    raw_registry_mapping = isinstance(action_contracts, Mapping) and isinstance(
        action_contracts.get("actions"), list
    )
    if action_contracts is None:
        registry = _config_action_registry(loaded)
    elif raw_registry_mapping:
        # A raw source registry is still deployment authority, even when an
        # embedder supplies it in memory.  Only an already-compiled mapping
        # may bypass the deployment-product plan for focused tests.
        authority = _configured_product_authority(loaded)
        registry = _compiled_action_registry(action_contracts, authority=authority)
    elif isinstance(action_contracts, (str, Path)):
        # A path is still a source registry, so it must be resolved against the
        # same deployment definition and lock before any service is built.
        authority = _configured_product_authority(loaded)
        registry = _compiled_action_registry(action_contracts, authority=authority)
    else:
        raise ServerStartupError(
            "compiled action contracts cannot replace deployment product authority; refusing to bind"
        )
    if not registry:
        raise ServerStartupError("action registry is empty; refusing to bind")
    if epoch_lease_chain is None:
        raise ServerStartupError("--epoch-lease-chain is required; refusing to bind without an epoch fence")
    if store is None and service is not None:
        store = getattr(service, "store", None)
    if store is None:
        raise ServerStartupError("durable external-action store is required; refusing to bind")
    # A supplied adapter is still a startup trust boundary.  A marker-only
    # fake must never let a broad or unverified login reach the listener.
    _require_runtime_store_contract(store)
    if mode_provider is not None:
        # Service mode is a durable restore-fence decision.  A callable
        # supplied by an embedder could make an otherwise quarantined process
        # advertise normal mode, so production launch has one authoritative
        # source: the persisted fence read below.
        raise ServerStartupError("production service mode cannot be injected; use the persisted restore fence")
    service_store = getattr(service, "store", None) if service is not None else None
    if service is not None and service_store is not store:
        raise ServerStartupError("review service is not bound to the durable store; refusing to bind")
    if nonce_store is not None and nonce_store is not store:
        raise ServerStartupError(
            "production DPoP nonce authority cannot be injected; use the durable store"
        )
    nonce_store = store
    if not callable(getattr(nonce_store, "insert_nonce", None)):
        raise ServerStartupError("durable DPoP nonce authority is required; refusing to bind")
    handoff_verification_key, selected_handoff_signer = _handoff_signer(
        loaded,
        service=service,
        store=store,
        supplied=handoff_signer,
    )
    owner_attestation_keys, owner_attestation_lifetime = _owner_attestation_options(loaded)
    if service is None:
        # ``ExternalActionService`` installs its durable Nomos bridge when no
        # operator callback is supplied.  Constructing it here keeps the
        # launch boundary useful for embedders while the strict-authorizer
        # check below still prevents an insecure service from binding.
        from .service import ExternalActionService

        service = ExternalActionService(
            **_service_kwargs(
                store=store,
                authorize=authorizer,
                registry=registry,
                handoff_signer=selected_handoff_signer,
                handoff_verification_key=handoff_verification_key,
                owner_attestation_keys=owner_attestation_keys,
                owner_attestation_maximum_lifetime_seconds=owner_attestation_lifetime,
            )
        )
    else:
        # A supplied production service remains an embedder boundary, but its
        # registry, signer, and owner-attestation keys are deployment-selected.
        try:
            service.action_contracts = dict(registry)
            service.handoff_signer = selected_handoff_signer
            if handoff_verification_key is not None:
                service.handoff_verification_key = handoff_verification_key
            if hasattr(service, "owner_attestation_keys"):
                service.owner_attestation_keys = owner_attestation_keys
            if hasattr(service, "owner_attestation_maximum_lifetime_seconds"):
                service.owner_attestation_maximum_lifetime_seconds = owner_attestation_lifetime
            status_authority = _claim_owner_status_authority(store)
            if status_authority is not None and hasattr(service, "claim_owner_status_verifier"):
                service.claim_owner_status_verifier = status_authority
        except (AttributeError, TypeError) as exc:
            raise ServerStartupError("review service claim handoff authority cannot be configured") from exc
    # Restore verification uses the same deployment-pinned historical key
    # configuration that passed the pre-bind gate.  It is never request
    # supplied or replaceable by an embedder after startup.
    try:
        service.restore_fence_config = loaded
    except (AttributeError, TypeError) as exc:
        raise ServerStartupError("review service restore verification authority cannot be configured") from exc
    existing_authorizer = getattr(service, "authorize", None)
    if authorizer is not None:
        if existing_authorizer is not None and existing_authorizer is not authorizer:
            raise ServerStartupError("multiple authorization authorities are configured; refusing to bind")
        try:
            service.authorize = authorizer
        except (AttributeError, TypeError) as exc:
            raise ServerStartupError("review service authorization authority cannot be configured") from exc
        existing_authorizer = authorizer
    if not _authorizer_available(existing_authorizer):
        raise ServerStartupError("mandatory Nomos authorizer is unavailable; refusing to bind")
    # Authentication audit persistence is deliberately owned by the service:
    # it appends the redacted evidence only after the same-transaction Nomos
    # decision.  ``auth_audit_writer`` remains an accepted compatibility
    # argument for callers, but is never installed as an API pre-dispatch
    # writer.
    if not callable(getattr(store, "record_auth_audit", None)):
        raise ServerStartupError("durable authentication-audit authority is unavailable; refusing to bind")
    required_fence_methods = ("retained_leases", "restore_fence", "insert_lease", "set_restore_fence", "transaction")
    if any(not callable(getattr(store, name, None)) for name in required_fence_methods):
        raise ServerStartupError("durable restore-fence transaction is unavailable; refusing to bind")
    startup_verification_config = _startup_verification_config(loaded)
    startup_login_binding = _startup_login_binding(
        store, deployment=loaded.deployment
    )
    try:
        startup = pre_bind_restore_fence(
            epoch_lease_chain,
            config=loaded,
            store=store,
            deployment=loaded.deployment,
            verification_config=startup_verification_config,
            login_binding=startup_login_binding,
        )
    except Exception as exc:
        raise ServerStartupError("restore fence is unavailable; refusing to bind") from exc
    if mode_provider is None:
        def persisted_mode() -> str:
            with store.lock:
                fence = getattr(store, "restore_fence", lambda **_: None)(deployment=loaded.deployment)
                if isinstance(fence, Mapping) and fence.get("service_mode") in {"normal", "restore_only"}:
                    return str(fence["service_mode"])
                # Startup's result is proof that the pre-bind transaction
                # committed; it is not a second runtime mode source.  If the
                # persisted projection disappears or becomes malformed,
                # fail closed instead of advertising a stale mode.
                raise RuntimeError("persisted restore-fence mode is unavailable")
        mode_provider = persisted_mode
    principal_mapping: Any = None
    for candidate in (
        startup_login_binding.get("principal_mapping") if isinstance(startup_login_binding, Mapping) else None,
        startup_verification_config.get("principal_mapping") if isinstance(startup_verification_config, Mapping) else None,
        getattr(store, "resolve_principal", None),
    ):
        if candidate is not None:
            principal_mapping = candidate
            break
    app = ExternalActionAPI(
        service,
        config=loaded,
        authenticator=authenticator,
        mode_provider=mode_provider,
        nonce_store=nonce_store,
        principal_mapping=principal_mapping,
        action_contracts=registry,
        auth_audit_writer=None,
        auth_config_digest=auth_config_digest or _auth_config_digest(loaded),
        require_auth_audit=True,
        mtls_principal_adapter=mtls_principal_adapter,
        mtls_certificate_mapping=mtls_certificate_mapping,
        require_identity_barrier=True,
    )
    # Expose proof for tests and for launchers without adding a route or
    # allowing a caller to override persisted service mode.
    app.startup_result = startup  # type: ignore[attr-defined]
    app.listener_bound = False  # type: ignore[attr-defined]
    return app


def run_server(
    config: ExternalActionConfig | Mapping[str, Any] | str | Path | Any = None,
    epoch_lease_chain: bytes | str | Path | Mapping[str, Any] | None = None,
    service: Any = None,
    store: Any = None,
    authenticator: Callable[..., Any] | None = None,
    login_binding: Mapping[str, Any] | None = None,
    verification_config: Mapping[str, Any] | None = None,
    nonce_store: Any = None,
    authorizer: Callable[..., Any] | None = None,
    nomos_authorizer: Callable[..., Any] | None = None,
    auth_audit_writer: Callable[..., Any] | None = None,
    auth_config_digest: str | None = None,
    handoff_signer: Callable[[Mapping[str, Any]], Mapping[str, str]] | None = None,
    mtls_principal_adapter: Callable[..., Any] | None = None,
    mtls_certificate_mapping: Mapping[str, Any] | Callable[..., Any] | None = None,
    host: str | None = None,
    port: int | None = None,
    **uvicorn_kwargs: Any,
) -> int:
    """Run uvicorn after :func:`create_app` has completed the startup gate."""

    if any(name in uvicorn_kwargs for name in ("app", "host", "port")):
        raise ServerStartupError("uvicorn app, host, and port are fixed by the review launcher")

    # The CLI historically calls ``run_server(args)``.  Keep that narrow
    # launcher shape while exposing the typed keyword form for embedders.
    cli_args = config if hasattr(config, "review_config") else None
    if authorizer is not None and nomos_authorizer is not None and authorizer is not nomos_authorizer:
        raise ServerStartupError("multiple authorization authorities are configured; refusing to bind")
    authorizer = authorizer or nomos_authorizer
    action_registry_source: Mapping[str, Any] | str | Path | None = None
    dsn: str | None = None
    if cli_args is not None:
        config = cli_args.review_config
        epoch_lease_chain = cli_args.epoch_lease_chain
        # The action registry is an explicit launch input.  Validate it at
        # the same pre-bind boundary even though construction of the durable
        # service/store is supplied by the deployment integration.
        action_contracts = getattr(cli_args, "action_contracts", None)
        if action_contracts is None:
            raise ServerStartupError("--action-contracts is required; refusing to bind without an action registry")
        action_registry_source = action_contracts
        dsn = getattr(cli_args, "dsn", None)
        if not dsn:
            raise ServerStartupError("--dsn is required; refusing to bind without a durable PostgreSQL store")
        host = getattr(cli_args, "host", host)
        port = getattr(cli_args, "port", port)
        if getattr(cli_args, "tls_cert", None):
            uvicorn_kwargs.setdefault("ssl_certfile", cli_args.tls_cert)
        if getattr(cli_args, "tls_key", None):
            uvicorn_kwargs.setdefault("ssl_keyfile", cli_args.tls_key)
        if getattr(cli_args, "tls_ca", None):
            uvicorn_kwargs.setdefault("ssl_ca_certs", cli_args.tls_ca)
            # A configured CA is meaningful only when the listener requires a
            # verified client certificate.  Identity mapping still happens
            # out-of-band in the ASGI scope; forwarded headers are ignored.
            uvicorn_kwargs.setdefault("ssl_cert_reqs", 2)
        if authorizer is None:
            authorizer = getattr(cli_args, "authorizer", None)
        if auth_audit_writer is None:
            auth_audit_writer = getattr(cli_args, "auth_audit_writer", None)
    if config is None or epoch_lease_chain is None:
        raise ServerStartupError("review config and --epoch-lease-chain are required")
    tls_fields = {
        name: uvicorn_kwargs.get(name)
        for name in ("ssl_certfile", "ssl_keyfile", "ssl_ca_certs")
    }
    tls_requested = any(value is not None for value in tls_fields.values())
    if tls_requested:
        if any(value is None for value in tls_fields.values()):
            raise ServerStartupError(
                "standalone external-action TLS requires certificate, private key, and client CA"
            )
        # A client certificate is required for restore reconciliation.  The
        # custom protocol is part of the launcher contract; accepting a
        # caller-selected protocol would silently drop the peer certificate
        # from the ASGI scope on stock Uvicorn.
        if "http" in uvicorn_kwargs:
            raise ServerStartupError("standalone mTLS owns the Uvicorn HTTP protocol adapter")
        uvicorn_kwargs["ssl_cert_reqs"] = ssl.CERT_REQUIRED
        uvicorn_kwargs["http"] = mtls_uvicorn_protocol_class()
    try:
        loaded = config if isinstance(config, ExternalActionConfig) else load_config(config)
    except Exception as exc:
        raise ServerStartupError("review configuration is unavailable; refusing to bind") from exc
    if action_registry_source is None:
        action_registry_source = loaded.action_registry_path
    if action_registry_source is None:
        raise ServerStartupError("--action-contracts is required; refusing to bind without an action registry")
    # ``run_server`` is the production launcher.  A path or raw mapping is a
    # source registry and therefore always requires the deployment-product
    # definition/plan authority before service construction.
    authority = _configured_product_authority(loaded)
    registry = _compiled_action_registry(action_registry_source, authority=authority)
    if dsn is None:
        dsn = loaded.database_dsn
    if store is None and service is not None:
        # Embedders commonly provide an already-bound service.  Reuse its
        # durable store instead of opening a second store and then failing the
        # identity check in create_app.
        store = getattr(service, "store", None)
    if store is None:
        if not dsn:
            raise ServerStartupError("--dsn is required; refusing to bind without a durable PostgreSQL store")
        capability_env = loaded.verifier_capability_env
        verifier_capability = (
            os.environ.get(capability_env) if capability_env is not None else None
        )
        if verifier_capability is None:
            raise ServerStartupError(
                "review verifier capability environment reference is unavailable"
            )
        try:
            store = PostgresExternalActionStore(
                dsn,
                deployment=loaded.deployment,
                verifier_capability=verifier_capability,
            )
        except (ExternalActionStoreError, OSError, ValueError) as exc:
            raise ServerStartupError(str(exc)) from exc
    _require_runtime_store_contract(store)
    if nonce_store is not None and nonce_store is not store:
        raise ServerStartupError(
            "production DPoP nonce authority cannot be injected; use the durable store"
        )
    nonce_store = store
    if not callable(getattr(nonce_store, "insert_nonce", None)):
        raise ServerStartupError(
            "durable DPoP nonce authority is required; refusing to bind"
        )
    handoff_verification_key, selected_handoff_signer = _handoff_signer(
        loaded,
        service=service,
        store=store,
        supplied=handoff_signer,
    )
    owner_attestation_keys, owner_attestation_lifetime = _owner_attestation_options(loaded)
    if service is None:
        from .service import ExternalActionService

        # A durable store supplies the concrete DurableNomosAuthorizer when
        # no callback is provided.  create_app performs the final strict
        # authorizer check before any listener is opened.
        service = ExternalActionService(
            **_service_kwargs(
                store=store,
                authorize=authorizer,
                registry=registry,
                handoff_signer=selected_handoff_signer,
                handoff_verification_key=handoff_verification_key,
                owner_attestation_keys=owner_attestation_keys,
                owner_attestation_maximum_lifetime_seconds=owner_attestation_lifetime,
            )
        )
    elif authorizer is not None:
        existing = getattr(service, "authorize", None)
        if existing is not None and existing is not authorizer:
            raise ServerStartupError("multiple authorization authorities are configured; refusing to bind")
        try:
            service.authorize = authorizer
        except (AttributeError, TypeError) as exc:
            raise ServerStartupError("review service authorization authority cannot be configured") from exc
    if not _authorizer_available(getattr(service, "authorize", None)):
        raise ServerStartupError("mandatory Nomos authorizer is unavailable; refusing to bind")
    app = create_app(
        config=loaded,
        epoch_lease_chain=epoch_lease_chain,
        service=service,
        store=store,
        authenticator=authenticator,
        nonce_store=nonce_store,
        login_binding=login_binding,
        verification_config=verification_config,
        action_contracts=action_registry_source,
        authorizer=authorizer,
        auth_audit_writer=auth_audit_writer,
        auth_config_digest=auth_config_digest,
        handoff_signer=handoff_signer,
        mtls_principal_adapter=mtls_principal_adapter,
        mtls_certificate_mapping=mtls_certificate_mapping,
    )
    import uvicorn

    actual_host = host if host is not None else loaded.bind_host
    actual_port = port if port is not None else loaded.bind_port
    app.listener_bound = True  # type: ignore[attr-defined]
    uvicorn.run(app, host=actual_host, port=actual_port, **uvicorn_kwargs)
    return 0


__all__ = ["ServerStartupError", "create_app", "mtls_uvicorn_protocol_class", "run_server"]
