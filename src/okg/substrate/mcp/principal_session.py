"""Qualify the SDK's session owner after transport authentication.

The SDK owns session IDs and their lifecycle. This adapter only supplies the
asserted principal to its existing owner comparison, on a request-local token
copy. It does not authenticate credentials or retain authorization decisions.
"""

from __future__ import annotations

import hashlib
import json

from mcp.server.auth.middleware.bearer_auth import AuthenticatedUser
from starlette.datastructures import Headers

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.principal_mapping import identity_claim_for_request


def install_dispatch_gate(mcp, gate):
    """Gate the SDK handler before its process-wide schema cache is read."""
    from mcp import types

    def guarded(original, *, is_call):
        async def dispatch(request):
            name = request.params.name if is_call else "tools/list"
            try:
                # The gate borrows a pool connection and may write an audit
                # row: off the event loop, in this request's context (#3493).
                await mcp._okg_run_blocking(gate, name)
            except SubstrateError:
                if is_call:
                    # The ordinary bounded call adapter owns public error
                    # rendering. Its gate refuses again before tool lookup.
                    return types.ServerResult(
                        await mcp.call_tool(name, request.params.arguments or {})
                    )
                raise
            return await original(request)

        return dispatch

    handlers = mcp._mcp_server.request_handlers
    for request_type, is_call in (
        (types.ListToolsRequest, False),
        (types.CallToolRequest, True),
    ):
        handlers[request_type] = guarded(handlers[request_type], is_call=is_call)


class PrincipalSessionMiddleware:
    def __init__(self, app, *, deployment_name, mapping):
        self.app = app
        self.deployment_name = deployment_name
        self.mapping = mapping

    async def __call__(self, scope, receive, send):
        user = scope.get("user")
        if scope["type"] != "http" or not isinstance(user, AuthenticatedUser):
            await self.app(scope, receive, send)
            return
        token = user.access_token
        try:
            claim = identity_claim_for_request(
                self.mapping,
                headers=Headers(scope=scope),
                transport_identity=token.client_id,
                access_token=token,
            )
            subject = claim["external_subject"]
        except SubstrateError:
            # Initialization still speaks the existing protocol. The fresh
            # dispatch gate returns the typed principal refusal before any
            # list/call; no invalid header can acquire a valid user's session.
            subject = None
        identity = [
            self.deployment_name,
            self.mapping.provider_id,
            self.mapping.issuer,
            self.mapping.audience,
            self.mapping.principal_kind,
            subject,
        ]
        qualified = hashlib.sha256(
            json.dumps(identity, separators=(",", ":")).encode()
        ).hexdigest()
        copied_scope = dict(scope)
        copied_scope["user"] = AuthenticatedUser(
            token.model_copy(
                deep=True, update={"subject": f"okg-principal:{qualified}"}
            )
        )
        await self.app(copied_scope, receive, send)
