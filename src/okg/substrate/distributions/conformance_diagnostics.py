"""Harness-side profiles for external conformance diagnostics (issue 1794).

The wire contract in :mod:`okg.substrate.contracts.deployment_product.conformance`
registers the diagnostic codes and closes two of them (declared optional
credential unavailable; declared asset missing).  Two more codes carry the
cases an operator meets at ingest time but had no settled shape:

* ``conformance_secret_reference_invalid`` -- a credential the operator
  supplied, or never supplied for a selected declaration that has no deferred
  binding, could not be used.  It is a ``first_publish`` failure owned by the
  operator, never a blocked arm.
* ``conformance_runtime_environment_unavailable`` -- tooling, a dependency, a
  cache, an endpoint or the runtime itself is missing.  It blocks, and it is
  never owned by the distribution or the consumer, so it cannot be confused
  with a missing declared asset or an absent optional credential.

This module produces those shapes, maps the substrate's own run-time source
health onto them, and checks a receipt against them.  It runs after
:func:`validate_conformance_exchange` and changes nothing in the reviewed
v1/v2 wire schemas or their catalog digests: a receipt this module refuses is
still a contract-valid document, but it does not follow the settled profiles.
"""

from __future__ import annotations

from collections.abc import Mapping
import re
from typing import Any, Literal

from okg.substrate.contracts.deployment_product.conformance import (
    ConformanceOptionalCredentialBinding,
    ExternalDistributionConformanceReceipt,
    ExternalDistributionConformanceRequest,
)
from okg.substrate.errors import SubstrateError


__all__ = [
    "DECLARED_ASSET_CODE",
    "INGEST_CREDENTIAL_EXPECTED",
    "INGEST_CREDENTIAL_FAILURES",
    "OPTIONAL_CREDENTIAL_CODE",
    "RUNTIME_ENVIRONMENT_CODE",
    "RUNTIME_ENVIRONMENT_EXPECTED",
    "RUNTIME_ENVIRONMENT_GAPS",
    "RUNTIME_ENVIRONMENT_OWNERS",
    "SECRET_REFERENCE_CODE",
    "SOURCE_HEALTH_HEALTHY",
    "SOURCE_HEALTH_UNREPRESENTABLE",
    "ConformanceProfileError",
    "IngestCredentialFailure",
    "RuntimeEnvironmentGap",
    "blocked_arm_result",
    "check_conformance_diagnostic_profiles",
    "declared_asset_missing_diagnostic",
    "diagnostic_from_source_health",
    "failed_arm_result",
    "ingest_credential_failure_diagnostic",
    "optional_credential_unavailable_diagnostic",
    "runtime_environment_unavailable_diagnostic",
]


OPTIONAL_CREDENTIAL_CODE = "conformance_declared_optional_credential_unavailable"
SECRET_REFERENCE_CODE = "conformance_secret_reference_invalid"
RUNTIME_ENVIRONMENT_CODE = "conformance_runtime_environment_unavailable"
DECLARED_ASSET_CODE = "conformance_distribution_declared_asset_missing"

# Closed failure tokens for an ingest-time credential failure.  They are
# ``observed`` values, so they must stay clear of the wire filter that refuses
# the substrings ``credential``, ``secret`` and ``token``.  The substrate's
# own source health produces the first and third (``missing_credential`` and
# ``auth_failed``); the other three name outcomes a consumer's bring-up record
# distinguishes but no substrate status reports today.
INGEST_CREDENTIAL_FAILURES: tuple[str, ...] = (
    "reference-unsupplied",
    "reference-invalid",
    "authentication-failed",
    "permission-denied",
    "totp-required",
)
INGEST_CREDENTIAL_EXPECTED = "resolvable-noninteractive-reference"
# Closed gap tokens for a missing runtime environment.
RUNTIME_ENVIRONMENT_GAPS: tuple[str, ...] = (
    "tooling-missing",
    "dependency-missing",
    "cache-unproduced",
    "runtime-unavailable",
)
RUNTIME_ENVIRONMENT_EXPECTED = "runtime-environment-available"
RUNTIME_ENVIRONMENT_OWNERS = frozenset({"harness", "operator", "system"})
# The statuses ``okg.substrate.ingest.service._source_health_is_healthy``
# accepts; every other status rejects the run as ``source_health_not_healthy``.
SOURCE_HEALTH_HEALTHY = frozenset({"ok", "not_applicable", "skipped_optional"})
# Unhealthy statuses no registered conformance code represents: a bounded
# partial run or an indexed-but-stale checkout is neither a credential failure
# nor a missing environment, so the mapping refuses instead of guessing.
SOURCE_HEALTH_UNREPRESENTABLE = frozenset({"partial", "acquire_stale"})
_SOURCE_DECLARATION_KINDS = frozenset({"source", "source-binding", "source-definition"})
_TOOL_DECLARATION_KINDS = frozenset({"tool", "tool-binding", "tool-definition"})
_DECLARATION_KINDS = _SOURCE_DECLARATION_KINDS | _TOOL_DECLARATION_KINDS
# Protocol v2 also accepts a `source-template` declaration, matching the wire
# validator (conformance.py, `declaration_kinds`); v1 does not.
_V2_DECLARATION_KINDS = _DECLARATION_KINDS | {"source-template"}
# The wire ``IdentifierValue`` shape (``_IDENTIFIER_RE`` in the contract
# module).  A product ``owner:ref`` id is wider than this; a producer refuses
# one that does not fit rather than rewriting it.
_WIRE_IDENTIFIER_RE = re.compile(r"^[a-z0-9][a-z0-9_.:-]{0,127}$")

IngestCredentialFailure = Literal[
    "reference-unsupplied",
    "reference-invalid",
    "authentication-failed",
    "permission-denied",
    "totp-required",
]
RuntimeEnvironmentGap = Literal[
    "tooling-missing",
    "dependency-missing",
    "cache-unproduced",
    "runtime-unavailable",
]


class ConformanceProfileError(SubstrateError):
    """A receipt is contract-valid but does not follow a settled profile.

    ``data`` carries ``code`` (the diagnostic code whose profile was broken),
    ``path`` (the diagnostic reference) and ``reason``.
    """

    code = "conformance_profile_refused"

    def to_dict(self) -> dict[str, str | None]:
        """The same shape as ``ConformanceContractError.to_dict``."""

        return {
            "code": self.code,
            "path": str(self.data.get("path")),
            "expected": str(self.data.get("code")),
            "observed": str(self.data.get("reason")),
        }


def _refuse(code: str, path: str, reason: str, fix_hint: str) -> ConformanceProfileError:
    return ConformanceProfileError(
        f"{code} diagnostic at {path} does not follow its settled profile",
        data={"code": code, "path": path, "reason": reason},
        fix_hint=fix_hint,
    )


def _wire_identifier(code: str, value: object, *, field: str) -> str:
    if not isinstance(value, str) or not _WIRE_IDENTIFIER_RE.fullmatch(value):
        raise ConformanceProfileError(
            f"{field} does not fit the wire identifier shape",
            data={"code": code, "path": str(value), "reason": "reference_not_identifier"},
            fix_hint=(
                "name the declaration by an id of at most 128 lowercase "
                "letters, digits, '_', '.', ':' or '-'; a wider product id is "
                "refused, never rewritten"
            ),
        )
    return value


def optional_credential_unavailable_diagnostic(
    binding: ConformanceOptionalCredentialBinding,
) -> dict[str, Any]:
    """The blocked shape for a selected source whose deferred credential is absent."""

    return {
        "code": OPTIONAL_CREDENTIAL_CODE,
        "arm": "first_publish",
        "phase": "apply",
        "reference": binding.binding_id,
        "owner": "operator",
        "retry_classification": "safe",
        "safe_next_action": "provide_optional_credential",
        "optional_credential": {
            "binding_id": binding.binding_id,
            "declaration_ref": binding.declaration_ref,
            "declaration_digest": binding.declaration_digest,
            "provider_connection_ref": binding.provider_connection_ref,
            "provider_connection_digest": binding.provider_connection_digest,
            "credential_reference": binding.credential_reference,
            "operator_reference_supplied": False,
            "missing_tooling": False,
            "missing_packaged_asset": False,
            "missing_dependency": False,
            "permission_denied": False,
            "required_cache_unproduced": False,
            "reference_invalid": False,
            "authentication_failed": False,
            "totp_required": False,
        },
    }


def ingest_credential_failure_diagnostic(
    *,
    declaration_ref: str,
    failure: IngestCredentialFailure,
) -> dict[str, Any]:
    """The failed shape for a credential that could not be used at ingest time.

    ``declaration_ref`` is the owner-qualified id of the selected source or
    tool declaration whose ingest failed.  The secret reference itself is never
    written into a diagnostic.
    """

    declaration_ref = _wire_identifier(
        SECRET_REFERENCE_CODE, declaration_ref, field="declaration_ref"
    )
    if failure not in INGEST_CREDENTIAL_FAILURES:
        raise _refuse(
            SECRET_REFERENCE_CODE,
            declaration_ref,
            "failure_token_unknown",
            f"use one of {', '.join(INGEST_CREDENTIAL_FAILURES)}",
        )
    return {
        "code": SECRET_REFERENCE_CODE,
        "arm": "first_publish",
        "phase": "apply",
        "reference": declaration_ref,
        "expected": INGEST_CREDENTIAL_EXPECTED,
        "observed": failure,
        "owner": "operator",
        "retry_classification": "safe",
        "safe_next_action": "revise_request",
    }


def runtime_environment_unavailable_diagnostic(
    *,
    arm: str,
    phase: str,
    reference: str,
    gap: RuntimeEnvironmentGap,
    owner: str = "harness",
) -> dict[str, Any]:
    """The blocked shape for missing tooling, dependency, cache or runtime."""

    reference = _wire_identifier(RUNTIME_ENVIRONMENT_CODE, reference, field="reference")
    if gap not in RUNTIME_ENVIRONMENT_GAPS:
        raise _refuse(
            RUNTIME_ENVIRONMENT_CODE,
            reference,
            "gap_token_unknown",
            f"use one of {', '.join(RUNTIME_ENVIRONMENT_GAPS)}",
        )
    if owner not in RUNTIME_ENVIRONMENT_OWNERS:
        raise _refuse(
            RUNTIME_ENVIRONMENT_CODE,
            reference,
            "owner_not_environmental",
            "a missing declared asset is a distribution failure, not a gap",
        )
    return {
        "code": RUNTIME_ENVIRONMENT_CODE,
        "arm": arm,
        "phase": phase,
        "reference": reference,
        "expected": RUNTIME_ENVIRONMENT_EXPECTED,
        "observed": gap,
        "owner": owner,
        "retry_classification": "safe",
        "safe_next_action": "provide_runtime_environment",
    }


def declared_asset_missing_diagnostic(
    *,
    asset_ref: str,
    asset_digest: str,
) -> dict[str, Any]:
    """The failed shape for a manifest-declared asset absent from the artifact."""

    asset_ref = _wire_identifier(DECLARED_ASSET_CODE, asset_ref, field="asset_ref")
    return {
        "code": DECLARED_ASSET_CODE,
        "arm": "artifact_materialization",
        "phase": "materialization",
        "reference": asset_ref,
        "owner": "distribution",
        "retry_classification": "never",
        "safe_next_action": "replace_artifact",
        "declared_asset": {
            "asset_ref": asset_ref,
            "asset_digest": asset_digest,
            "declared_in_manifest": True,
            "observed_in_artifact": False,
        },
    }


def diagnostic_from_source_health(
    health: Mapping[str, Any],
    *,
    declaration_ref: str,
    deferred_binding: ConformanceOptionalCredentialBinding | None = None,
) -> dict[str, Any] | None:
    """Map one source's run-time health onto the first_publish/apply profiles.

    ``health`` is the ``source_health`` mapping the ingest service records for
    a source run (``SourceHealth.as_dict()``); its ``status`` is the closed
    ``SOURCE_HEALTH_STATUSES`` vocabulary and the ingest admission gate rejects
    every status outside :data:`SOURCE_HEALTH_HEALTHY` as
    ``source_health_not_healthy``.  A healthy status maps to ``None``.

    * ``missing_credential`` naming at least one credential reference is a
      credential failure: blocked with the optional-credential profile when
      the declaration carries a deferred binding, else failed with
      ``reference-unsupplied``.  The same status naming no reference is not a
      credential failure (sources report it for a missing executable or base
      URL) and maps to ``tooling-missing``.
    * ``auth_failed`` maps to ``authentication-failed``; the substrate cannot
      tell it from a permission refusal, so ``permission-denied`` is never
      produced here.
    * ``cache_missing`` maps to ``cache-unproduced``; ``endpoint_failed``,
      ``tls_failed`` and ``acquire_failed`` map to ``runtime-unavailable``
      owned by the system (``dependency_preflight`` reports a missing module
      as ``endpoint_failed``, so a dependency gap lands here too).
    * ``partial`` and ``acquire_stale`` are rejected runs no registered code
      represents; they refuse ``health_status_unrepresentable``.
    """

    status = health.get("status")
    if status in SOURCE_HEALTH_HEALTHY:
        return None
    if status == "missing_credential":
        if not tuple(health.get("credential_refs") or ()):
            return runtime_environment_unavailable_diagnostic(
                arm="first_publish",
                phase="apply",
                reference=declaration_ref,
                gap="tooling-missing",
            )
        if deferred_binding is not None:
            if deferred_binding.declaration_ref != declaration_ref:
                raise _refuse(
                    OPTIONAL_CREDENTIAL_CODE,
                    declaration_ref,
                    "deferred_binding_mismatch",
                    "pass the deferred binding declared for this declaration",
                )
            return optional_credential_unavailable_diagnostic(deferred_binding)
        return ingest_credential_failure_diagnostic(
            declaration_ref=declaration_ref, failure="reference-unsupplied"
        )
    if status == "auth_failed":
        if deferred_binding is not None:
            raise _refuse(
                SECRET_REFERENCE_CODE,
                declaration_ref,
                "deferred_binding_contradiction",
                "a deferred binding means no credential was supplied, so an "
                "authentication failure cannot be observed for it",
            )
        return ingest_credential_failure_diagnostic(
            declaration_ref=declaration_ref, failure="authentication-failed"
        )
    if status == "cache_missing":
        return runtime_environment_unavailable_diagnostic(
            arm="first_publish",
            phase="apply",
            reference=declaration_ref,
            gap="cache-unproduced",
        )
    if status in {"endpoint_failed", "tls_failed", "acquire_failed"}:
        return runtime_environment_unavailable_diagnostic(
            arm="first_publish",
            phase="apply",
            reference=declaration_ref,
            gap="runtime-unavailable",
            owner="system",
        )
    if status in SOURCE_HEALTH_UNREPRESENTABLE:
        raise _refuse(
            SECRET_REFERENCE_CODE,
            declaration_ref,
            "health_status_unrepresentable",
            f"source health {status!r} rejects the run but names neither a "
            "credential failure nor a missing environment; report the arm "
            "failure with the code that describes the observed defect",
        )
    raise _refuse(
        SECRET_REFERENCE_CODE,
        declaration_ref,
        "health_status_unknown",
        "source health must carry one SOURCE_HEALTH_STATUSES value",
    )


def blocked_arm_result(arm: str, codes: tuple[str, ...]) -> dict[str, Any]:
    return {
        "arm": arm,
        "status": "blocked",
        "mutation": "none",
        "retry_classification": "safe",
        "diagnostic_codes": sorted(set(codes)),
    }


def failed_arm_result(
    arm: str,
    codes: tuple[str, ...],
    *,
    mutation: Literal["none", "read_only", "mutating"],
    retry_classification: Literal["never", "safe", "unsafe"],
) -> dict[str, Any]:
    """A failed arm; a credential failure is always safe to retry.

    ``mutation`` is ``none`` when the failure was detected before apply
    touched the deployment (a plan-time secret refusal) and ``mutating`` when
    an ingest run had begun and was retracted.
    """

    if SECRET_REFERENCE_CODE in codes and (
        retry_classification != "safe" or mutation == "read_only"
    ):
        raise _refuse(
            SECRET_REFERENCE_CODE,
            arm,
            "arm_profile_mismatch",
            "a credential failure is safe to retry and records mutation none "
            "(detected before apply) or mutating (an ingest run was retracted)",
        )
    return {
        "arm": arm,
        "status": "fail",
        "mutation": mutation,
        "retry_classification": retry_classification,
        "diagnostic_codes": sorted(set(codes)),
    }


def _selected_declarations(
    request: ExternalDistributionConformanceRequest,
) -> Mapping[str, str]:
    """Owner-qualified id -> kind for every selected source/tool declaration."""

    kinds = (
        _V2_DECLARATION_KINDS if request.PROTOCOL_VERSION == "v2" else _DECLARATION_KINDS
    )
    selected: dict[str, str] = {}
    for asset in (
        *request.selection.distribution_release.asset_refs,
        *(asset for bundle in request.selection.bundles for asset in bundle.asset_refs),
    ):
        if asset.kind in kinds:
            selected[asset.qualified_id] = asset.kind
    return selected


def check_conformance_diagnostic_profiles(
    request: ExternalDistributionConformanceRequest,
    receipt: ExternalDistributionConformanceReceipt,
) -> None:
    """Refuse a receipt whose credential or environment diagnostics break profile.

    Call after :func:`validate_conformance_exchange`; this function trusts the
    wire validator for digests, arm coverage and the two profiles it already
    closes, and checks only what the wire contract leaves open.  A runtime
    environment diagnostic may omit ``observed`` (the reviewed blocked fixture
    does); when present it must be a closed gap token.
    """

    selected = _selected_declarations(request)
    deferred = {
        binding.declaration_ref
        for binding in request.inputs.optional_credential_bindings
    }
    for diagnostic in receipt.diagnostics:
        path = diagnostic.reference
        if diagnostic.code == SECRET_REFERENCE_CODE:
            if (
                diagnostic.arm != "first_publish"
                or diagnostic.phase != "apply"
                or diagnostic.owner != "operator"
                or diagnostic.retry_classification != "safe"
                or diagnostic.safe_next_action != "revise_request"
                or diagnostic.expected != INGEST_CREDENTIAL_EXPECTED
            ):
                raise _refuse(
                    SECRET_REFERENCE_CODE,
                    path,
                    "profile_fields_mismatch",
                    "an ingest-time credential failure is first_publish/apply, "
                    "owned by the operator, safe to retry, next action "
                    f"revise_request, expected {INGEST_CREDENTIAL_EXPECTED}",
                )
            if diagnostic.observed not in INGEST_CREDENTIAL_FAILURES:
                raise _refuse(
                    SECRET_REFERENCE_CODE,
                    path,
                    "failure_token_unknown",
                    f"observed must be one of {', '.join(INGEST_CREDENTIAL_FAILURES)}",
                )
            if diagnostic.reference not in selected:
                raise _refuse(
                    SECRET_REFERENCE_CODE,
                    path,
                    "declaration_not_selected",
                    "reference the owner-qualified id of a source or tool "
                    "declaration the request selected",
                )
            if diagnostic.reference in deferred:
                raise _refuse(
                    SECRET_REFERENCE_CODE,
                    path,
                    "declaration_has_deferred_binding",
                    "a declaration with a deferred optional binding blocks with "
                    f"{OPTIONAL_CREDENTIAL_CODE}; it does not fail",
                )
        elif diagnostic.code == RUNTIME_ENVIRONMENT_CODE:
            if (
                diagnostic.owner not in RUNTIME_ENVIRONMENT_OWNERS
                or diagnostic.safe_next_action != "provide_runtime_environment"
                or diagnostic.retry_classification != "safe"
                or diagnostic.expected not in {None, RUNTIME_ENVIRONMENT_EXPECTED}
            ):
                raise _refuse(
                    RUNTIME_ENVIRONMENT_CODE,
                    path,
                    "profile_fields_mismatch",
                    "a missing runtime environment is owned by the harness, "
                    "operator or system, safe to retry, next action "
                    "provide_runtime_environment",
                )
            if (
                diagnostic.observed is not None
                and diagnostic.observed not in RUNTIME_ENVIRONMENT_GAPS
            ):
                raise _refuse(
                    RUNTIME_ENVIRONMENT_CODE,
                    path,
                    "gap_token_unknown",
                    f"observed must be one of {', '.join(RUNTIME_ENVIRONMENT_GAPS)}",
                )
            if diagnostic.reference in deferred:
                # The wire's optional_credential block carries its own
                # missing_tooling / missing_dependency / required_cache flags;
                # an environment gap must not stand in for that record.
                raise _refuse(
                    RUNTIME_ENVIRONMENT_CODE,
                    path,
                    "declaration_has_deferred_binding",
                    "a declaration with a deferred optional binding reports its "
                    f"outcome through {OPTIONAL_CREDENTIAL_CODE}, never as an "
                    "environment gap",
                )
    for arm in receipt.arms:
        if SECRET_REFERENCE_CODE in arm.diagnostic_codes and (
            arm.arm != "first_publish"
            or arm.status != "fail"
            or arm.retry_classification != "safe"
            or arm.mutation == "read_only"
        ):
            raise _refuse(
                SECRET_REFERENCE_CODE,
                arm.arm,
                "arm_profile_mismatch",
                "a credential failure fails the first_publish arm, safe to "
                "retry, with mutation none or mutating",
            )
