"""Fixture connectors whose run health comes from the real process environment.

Issue #1794 needs the ingest-time credential failure *exercised*, not staged:
a connector that reports ``missing_credential`` because the environment
variable it names is genuinely unset, driven through the real
``SourceRunner`` and the real source-admission gate.

These connectors therefore derive health from the production preflight
helpers rather than from a literal ``ConnectorHealth``, and they are written
against the public ``okg.deployment`` SDK alone, so nothing here can drift
ahead of what an external deployment author is able to write.
"""
from __future__ import annotations

import os
import urllib.error
import urllib.request

from okg.deployment import (
    Checkpoint,
    ConnectorAdapter,
    ConnectorHealth,
    ConnectorRun,
    NodeFact,
    PreflightResult,
    credential_preflight,
    http_preflight,
)


def _health_from(result: PreflightResult) -> ConnectorHealth:
    """Project a preflight result onto the run health the runner records.

    The projection copies the reference NAMES the preflight reported and
    never a value: ``credential_preflight`` reports only the variable names
    it looked for, which is what makes the recorded health safe to carry
    into a conformance diagnostic.
    """

    return ConnectorHealth(
        status=result.status,
        mode=result.mode,
        credential_refs=tuple(result.credential_refs),
        alias_refs={
            name: tuple(aliases)
            for name, aliases in (result.alias_refs or {}).items()
        },
        endpoint=result.endpoint,
        reason=result.reason,
        checked_at=result.checked_at,
    )


class CredentialGatedConnector:
    """A connector gated on one environment variable.

    ``run`` calls ``credential_preflight`` against the live process
    environment.  When the variable is unset the run emits no facts and
    reports ``missing_credential`` naming that variable; when it is set the
    same run emits one document fact and reports ``ok``.  The difference
    between the two outcomes is therefore the environment alone.
    """

    profile = "discovery_crawl"

    def __init__(
        self,
        *,
        credential_ref: str,
        node_id: str,
        text: str = "credential gated fixture",
    ) -> None:
        self.name = "credential_gated_fixture"
        self.credential_ref = credential_ref
        self.node_id = node_id
        self.text = text

    def preflight(self) -> PreflightResult:
        return credential_preflight(
            self.name, self.credential_ref, required=True
        )

    def run(self, ctx) -> ConnectorRun:
        health = _health_from(self.preflight())
        if health.status != "ok":
            # An unusable credential produced no records at all, so the run
            # claims no scope and no deletion authority.
            return ConnectorRun(
                facts=(),
                completed_scope=False,
                next_checkpoint=ctx.checkpoint,
                run_mode=ctx.mode,
                health=health,
            )
        fact = NodeFact(
            node_id=self.node_id,
            subtype="document",
            attrs={"text": self.text},
            source_record_id={"id": self.node_id},
            source_revision={"content_hash": self.text},
        )
        # The healthy control declares no completed scope: it exists to show
        # the SAME connector runs `ok` once the variable is set, not to
        # exercise deletion authority.
        return ConnectorRun(
            facts=(fact,),
            completed_scope=False,
            next_checkpoint=Checkpoint(
                next_cursor={"credential_ref": self.credential_ref}
            ),
            run_mode=ctx.mode,
            health=health,
        )


class CredentialGatedFixtureSource(ConnectorAdapter):
    """Registry-constructible adapter over :class:`CredentialGatedConnector`."""

    profile = CredentialGatedConnector.profile

    def __init__(
        self,
        *,
        credential_ref: str,
        node_id: str,
        text: str = "credential gated fixture",
    ) -> None:
        super().__init__(
            CredentialGatedConnector(
                credential_ref=credential_ref, node_id=node_id, text=text
            )
        )


class TokenRejectedConnector:
    """A connector whose endpoint refuses the token the environment supplies.

    ``run`` reads the token from the live environment and presents it to
    ``endpoint``.  An authority that answers with a login page turns the
    production helper ``http_preflight`` into ``auth_failed`` -- the
    supplied-but-rejected case, which is distinct from the unset case above.
    """

    profile = "discovery_crawl"

    def __init__(
        self, *, credential_ref: str, endpoint: str, node_id: str
    ) -> None:
        self.name = "token_rejected_fixture"
        self.credential_ref = credential_ref
        self.endpoint = endpoint
        self.node_id = node_id

    def preflight(self) -> PreflightResult:
        return credential_preflight(
            self.name, self.credential_ref, required=True
        )

    def run(self, ctx) -> ConnectorRun:
        supplied = self.preflight()
        if supplied.status != "ok":
            return ConnectorRun(
                facts=(),
                completed_scope=False,
                next_checkpoint=ctx.checkpoint,
                run_mode=ctx.mode,
                health=_health_from(supplied),
            )
        request = urllib.request.Request(
            self.endpoint,
            headers={
                "Authorization": f"Bearer {os.environ[self.credential_ref]}"
            },
        )
        # Proxy-isolated on purpose. `urlopen` honors http_proxy/HTTP_PROXY,
        # so on a host that sets one without a matching no_proxy this loopback
        # request would leave the machine and fail as a network error rather
        # than as the authority's refusal -- an opaque red with nothing to do
        # with the credential under test.
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        try:
            with opener.open(request, timeout=10) as response:
                body = response.read().decode("utf-8", "replace")
                code = response.status
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", "replace")
            code = exc.code
        login_page_detected = "<form" in body and "password" in body
        health = _health_from(
            http_preflight(
                self.name,
                ok=code == 200,
                endpoint=self.endpoint,
                required=True,
                reason=f"authority answered {code}",
                login_page_detected=login_page_detected,
            )
        )
        if health.status != "ok":
            return ConnectorRun(
                facts=(),
                completed_scope=False,
                next_checkpoint=ctx.checkpoint,
                run_mode=ctx.mode,
                health=health,
            )
        fact = NodeFact(
            node_id=self.node_id,
            subtype="document",
            attrs={"text": body},
            source_record_id={"id": self.node_id},
            source_revision={"content_hash": body},
        )
        return ConnectorRun(
            facts=(fact,),
            completed_scope=False,
            next_checkpoint=Checkpoint(
                next_cursor={"endpoint": self.endpoint}
            ),
            run_mode=ctx.mode,
            health=health,
        )


class TokenRejectedFixtureSource(ConnectorAdapter):
    """Registry-constructible adapter over :class:`TokenRejectedConnector`."""

    profile = TokenRejectedConnector.profile

    def __init__(
        self, *, credential_ref: str, endpoint: str, node_id: str
    ) -> None:
        super().__init__(
            TokenRejectedConnector(
                credential_ref=credential_ref,
                endpoint=endpoint,
                node_id=node_id,
            )
        )
