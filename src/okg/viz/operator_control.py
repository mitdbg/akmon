"""Transport-only seam for the Viz coordination operator controls.

The Viz application owns authentication for these transitions.  This module
owns only the small request and callback contract handed to a deployment's
native coordination writer.  It intentionally does not import a database
client, resolve a DSN, read process identity, or implement a transition.

The native writer can be injected by a deployment later.  Until then the
application reports an explicit service-unavailable response rather than
silently falling through to a generic tool or legacy coordination path.
"""

from __future__ import annotations

import re
from typing import Any, Literal, Mapping, Protocol, TypeAlias, runtime_checkable

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator


ControlOperation: TypeAlias = Literal["bootstrap", "fallback", "reactivation"]
"""The only operations reachable through the operator-control transport."""

OPERATOR_CONTROL_OPERATIONS = frozenset({
    "bootstrap",
    "fallback",
    "reactivation",
})
"""Closed operation set shared by clients and the injected writer."""


# A plan may contain deployment-specific structured data, but it must never be
# a covert second identity or credential channel.  Credential *references* can
# be bound by a native writer later; credential values and all identity claims
# remain transport/server-owned and are not accepted here.
_FORBIDDEN_PLAN_KEYS = frozenset(
    {
        "actor",
        "actor_id",
        "actor_principal",
        "agent",
        "agent_id",
        "agent_principal",
        "auth_token",
        "credential",
        "credential_id",
        "database_credential",
        "database_password",
        "dsn",
        "host",
        "operator",
        "operator_id",
        "operator_principal",
        "owner",
        "owner_principal",
        "password",
        "principal",
        "session",
        "session_id",
        "token",
        "user",
        "username",
    }
)
_FORBIDDEN_PLAN_MARKERS = (
    "private_key",
    "secret",
    "authorization",
    "bearer",
    "token",
    "dsn",
    "password",
)
_SECRET_VALUE_RE = re.compile(
    r"(?i)(?:postgres(?:ql)?|mysql|mongodb|redis)://|"
    r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----"
)


def _safe_plan_value(value: Any, *, path: str = "structured_plan") -> Any:
    """Validate plan metadata without turning it into an identity channel."""
    if isinstance(value, Mapping):
        clean: dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key)
            normalized = key_text.strip().lower().replace("-", "_")
            if (
                normalized in _FORBIDDEN_PLAN_KEYS
                or any(marker in normalized for marker in _FORBIDDEN_PLAN_MARKERS)
            ):
                raise ValueError(
                    f"{path} contains a server-owned or sensitive field"
                )
            clean[key_text] = _safe_plan_value(
                item, path=f"{path}.{key_text}"
            )
        return clean
    if isinstance(value, (list, tuple)):
        return [
            _safe_plan_value(item, path=f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    if isinstance(value, str) and _SECRET_VALUE_RE.search(value):
        raise ValueError(f"{path} contains credential material")
    return value


class OperatorControlRequest(BaseModel):
    """Validated, non-secret input to one operator control transition.

    ``structured_plan`` also accepts the short wire spelling ``plan`` for
    clients that use the transport as a compact JSON API.  Digest aliases are
    compatibility spellings only; the canonical attributes handed to the
    service are explicit about what each digest represents.

    Deliberately absent fields include actor/principal, token, DSN, database
    credentials, host, session, and assignment identity.  Extra fields are
    rejected so a caller cannot smuggle one of those claims into the service.
    """

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
        str_strip_whitespace=True,
    )

    operation: ControlOperation
    structured_plan: dict[str, Any] = Field(
        validation_alias=AliasChoices("structured_plan", "plan"),
        description="Structured transition plan; never credentials or identity claims.",
    )
    claimed_plan_digest: str = Field(
        min_length=1,
        validation_alias=AliasChoices("claimed_plan_digest", "plan_digest"),
    )
    expected_policy_digest: str = Field(
        min_length=1,
        validation_alias=AliasChoices(
            "expected_policy_digest",
            "current_policy_digest",
            "policy_digest_expected",
            "current_policy_digest_expected",
            "policy_digest",
        ),
    )
    expected_project_digest: str = Field(
        min_length=1,
        validation_alias=AliasChoices(
            "expected_project_digest",
            "current_project_digest",
            "project_digest_expected",
            "current_project_digest_expected",
            "project_digest",
        ),
    )
    idempotency_key: str = Field(min_length=1)

    @field_validator("structured_plan", mode="before")
    @classmethod
    def _validate_structured_plan(cls, value: Any) -> dict[str, Any]:
        if not isinstance(value, Mapping):
            raise ValueError("structured_plan must be a JSON object")
        clean = _safe_plan_value(value)
        # _safe_plan_value preserves mapping keys and values but returns a
        # plain dict, making the service boundary deterministic and JSON-safe.
        return dict(clean)

    # Short compatibility views keep the handoff readable to native writers
    # whose function arguments are named ``plan_digest``/``policy_digest``;
    # these are properties, not additional caller-controlled fields.
    @property
    def plan(self) -> dict[str, Any]:
        return self.structured_plan

    @property
    def plan_digest(self) -> str:
        return self.claimed_plan_digest

    @property
    def current_policy_digest(self) -> str:
        return self.expected_policy_digest

    @property
    def current_project_digest(self) -> str:
        return self.expected_project_digest

    @property
    def policy_digest(self) -> str:
        return self.expected_policy_digest

    @property
    def project_digest(self) -> str:
        return self.expected_project_digest


# The service receives exactly two values: the principal stamped by this
# transport and the validated request.  It returns a redacted receipt only;
# database connections and credentials are deployment-owned implementation
# details outside this seam.
RedactedReceipt: TypeAlias = Mapping[str, Any]


@runtime_checkable
class OperatorControlService(Protocol):
    """Callable contract implemented by a native coordination writer."""

    def __call__(
        self,
        principal: str,
        request: OperatorControlRequest,
    ) -> RedactedReceipt:
        """Apply one already-authenticated transition and return its receipt."""
        raise NotImplementedError


OperatorControlCallback: TypeAlias = OperatorControlService


def invoke_operator_control(
    service: OperatorControlService,
    *,
    principal: str,
    request: OperatorControlRequest,
) -> RedactedReceipt:
    """Invoke the injected writer with only server-owned identity and input."""
    # Keyword names make the two-value contract explicit and also allow a
    # deployment callback to declare the values in either readable order;
    # there is still no third context/credential argument.
    receipt = service(principal=principal, request=request)
    if not isinstance(receipt, Mapping):
        raise TypeError("operator-control service must return a redacted mapping receipt")
    return receipt


__all__ = [
    "ControlOperation",
    "OPERATOR_CONTROL_OPERATIONS",
    "OperatorControlCallback",
    "OperatorControlRequest",
    "OperatorControlService",
    "RedactedReceipt",
    "invoke_operator_control",
]
