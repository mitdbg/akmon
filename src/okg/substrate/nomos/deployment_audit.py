"""Nomos deployment and source privacy policy audit."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from okg.substrate.nomos.vocabulary import (
    DATA_CLASSIFICATIONS,
    DEPLOYMENT_CLASSES,
    EFFECTS,
    OBLIGATION_TYPES,
    PII_CLASSES,
    PRINCIPAL_KINDS,
    SOURCE_SENSITIVITIES,
)


DEPLOYMENT_POSTURE_FIELDS: tuple[str, ...] = (
    "deployment_class",
    "read_principals",
    "write_principals",
    "mcp_write_default",
    "export_policy",
    "secret_handling",
    "live_call_default",
    "allowed_source_sensitivities",
    "allowed_data_classifications",
    "allowed_exportability",
    "privacy_obligations",
    "readiness_profiles",
)
SOURCE_POLICY_FIELDS: tuple[str, ...] = (
    "sensitivity",
    "data_classification",
    "credential_ref_policy",
    "live_call_allowed",
    "exportability",
    "retention",
    "provenance",
    "privacy_obligations",
)
EXPORT_POLICIES = frozenset({
    "internal_only",
    "no_export",
    "public_allowed",
})
SECRET_HANDLING_POLICIES = frozenset({
    "no_secrets",
    "operator_managed",
    "secret_ref_required",
})
CREDENTIAL_REF_POLICIES = frozenset({
    "none",
    "optional",
    "required",
})
EXPORTABILITY_POLICIES = frozenset({
    "internal",
    "no_export",
    "public",
})
RETENTION_POLICIES = frozenset({
    "normal",
    "no_persistence",
    "short",
    "source_defined",
})
PROVENANCE_POLICIES = frozenset({
    "declared",
    "local_filesystem",
    "operator_declared",
    "registry_metadata",
    "remote_api",
})
DEPLOYMENT_CLASS_REQUIREMENTS: Mapping[str, Mapping[str, Any]] = {
    "ci_authority": {
        "allowed_export_policies": {"internal_only", "no_export"},
        "allowed_mcp_write_default": {"deny"},
        "allowed_live_call_default": {"deny", "review"},
        "allowed_secret_handling": {"secret_ref_required"},
        "required_read_principals": {"ci_runner"},
        "required_write_principals": {"ci_runner"},
    },
    "dogfood_workspace": {
        "allowed_export_policies": {"internal_only", "no_export"},
        "allowed_mcp_write_default": {"deny", "review"},
        "allowed_live_call_default": {"deny", "review"},
        "allowed_secret_handling": {"secret_ref_required"},
        "required_allowed_source_sensitivities": {"local_private"},
        "forbidden_export_policies": {"public_allowed"},
    },
    "personal_okg": {
        "allowed_export_policies": {"no_export"},
        "allowed_mcp_write_default": {"deny", "review"},
        "allowed_live_call_default": {"deny", "review"},
        "allowed_secret_handling": {"secret_ref_required"},
        "allowed_exportability_subset": {"no_export"},
        "required_allowed_source_sensitivities": {"local_private"},
        "required_allowed_data_classifications": {"private"},
        "required_privacy_obligations": {"no_export"},
    },
    "public_reference_demo": {
        "allowed_export_policies": {"public_allowed"},
        "allowed_mcp_write_default": {"deny", "review"},
        "allowed_live_call_default": {"deny"},
        "allowed_secret_handling": {"no_secrets"},
        "allowed_source_sensitivities_subset": {"public"},
        "allowed_data_classifications_subset": {"public"},
        "allowed_exportability_subset": {"public"},
    },
    "synthetic_healthcare_demo": {
        "allowed_export_policies": {"internal_only", "no_export"},
        "allowed_mcp_write_default": {"deny", "review"},
        "allowed_live_call_default": {"deny", "review"},
        "allowed_secret_handling": {"no_secrets", "secret_ref_required"},
        "allowed_source_sensitivities_subset": {
            "public",
            "internal",
            "phi_synthetic",
        },
        "allowed_data_classifications_subset": {
            "public",
            "internal",
            "synthetic_phi",
        },
        "required_allowed_source_sensitivities": {"phi_synthetic"},
        "required_allowed_data_classifications": {"synthetic_phi"},
        "required_privacy_obligations": {"redaction"},
    },
    "governed_research_metadata": {
        "allowed_export_policies": {"internal_only", "no_export"},
        "allowed_mcp_write_default": {"deny", "review"},
        "allowed_live_call_default": {"deny", "review"},
        "allowed_secret_handling": {"secret_ref_required"},
        "required_allowed_source_sensitivities": {"regulated_metadata"},
        "required_allowed_data_classifications": {"regulated_metadata"},
        "required_privacy_obligations": {"source_retention"},
    },
    "org_operational_private": {
        "allowed_export_policies": {"internal_only", "no_export"},
        "allowed_mcp_write_default": {"deny", "review"},
        "allowed_live_call_default": {"deny", "review"},
        "allowed_secret_handling": {"secret_ref_required"},
        "required_allowed_source_sensitivities": {"private"},
        "required_allowed_data_classifications": {"private"},
        "forbidden_export_policies": {"public_allowed"},
    },
    "private_product_engineering": {
        "allowed_export_policies": {"internal_only", "no_export"},
        "allowed_mcp_write_default": {"deny", "review"},
        "allowed_live_call_default": {"deny", "review"},
        "allowed_secret_handling": {"secret_ref_required"},
        "required_allowed_source_sensitivities": {"credentialed_external"},
        "required_allowed_data_classifications": {"restricted"},
        "required_privacy_obligations": {"secret_ref_required"},
        "forbidden_export_policies": {"public_allowed"},
    },
}


@dataclass(frozen=True)
class NomosAuditFinding:
    """Structured deployment-policy audit finding."""

    code: str
    message: str
    path: str
    severity: str = "error"
    source_name: str | None = None
    details: Mapping[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "code": self.code,
            "reason_code": self.code,
            "severity": self.severity,
            "message": self.message,
            "path": self.path,
        }
        if self.source_name:
            out["source_name"] = self.source_name
        if self.details:
            out["details"] = dict(self.details)
        return out


def audit_deployment_policy(
    manifest: Mapping[str, Any],
    *,
    source_registry: Mapping[str, Any] | None = None,
    deployment: str | None = None,
) -> dict[str, Any]:
    """Audit a deployment manifest plus optional source registry.

    The manifest must carry ``nomos.deployment_posture``. Source privacy
    properties may be declared per source as ``source_policy`` or as explicit
    deployment-owned class/name defaults under
    ``nomos.source_policy_defaults``.
    """
    findings: list[NomosAuditFinding] = []
    warnings: list[NomosAuditFinding] = []
    normalized_posture = _normalize_deployment_posture(manifest, findings)
    source_policies: dict[str, dict[str, Any]] = {}

    if source_registry is not None:
        source_policies = _audit_source_registry(
            manifest=manifest,
            source_registry=source_registry,
            posture=normalized_posture,
            findings=findings,
            warnings=warnings,
        )

    error_findings = [f for f in findings if f.severity == "error"]
    return {
        "schema": "okg.nomos.deployment_policy_audit.v1",
        "ok": not error_findings,
        "deployment": deployment or str(manifest.get("name") or ""),
        "deployment_posture": normalized_posture,
        "source_policies": source_policies,
        "source_policy_count": len(source_policies),
        "findings": [f.to_dict() for f in findings],
        "warnings": [f.to_dict() for f in warnings],
    }


def audit_source_privacy_policy(
    source_name: str,
    source_entry: Mapping[str, Any],
    *,
    manifest: Mapping[str, Any],
    posture: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Audit one source entry against a deployment posture."""
    findings: list[NomosAuditFinding] = []
    warnings: list[NomosAuditFinding] = []
    normalized_posture = (
        dict(posture)
        if posture is not None
        else _normalize_deployment_posture(manifest, findings)
    )
    policy = _source_policy_for_entry(
        manifest=manifest,
        source_name=source_name,
        entry=source_entry,
        findings=findings,
    )
    normalized = (
        _normalize_source_policy(
            source_name=source_name,
            source_entry=source_entry,
            policy=policy,
            posture=normalized_posture,
            findings=findings,
            warnings=warnings,
        )
        if policy is not None
        else {}
    )
    return {
        "schema": "okg.nomos.source_privacy_policy_audit.v1",
        "ok": not [f for f in findings if f.severity == "error"],
        "source_name": source_name,
        "source_policy": normalized,
        "findings": [f.to_dict() for f in findings],
        "warnings": [f.to_dict() for f in warnings],
    }


def _normalize_deployment_posture(
    manifest: Mapping[str, Any],
    findings: list[NomosAuditFinding],
) -> dict[str, Any]:
    nomos = _mapping(manifest.get("nomos"))
    posture = _mapping(
        nomos.get("deployment_posture")
        if nomos is not None
        else None
    )
    if posture is None:
        posture = _mapping(nomos.get("posture")) if nomos is not None else None
    if posture is None:
        findings.append(NomosAuditFinding(
            code="nomos.audit.deployment_posture_missing",
            message="deployment manifest is missing nomos.deployment_posture",
            path="nomos.deployment_posture",
        ))
        return {}

    normalized: dict[str, Any] = {}
    for field in DEPLOYMENT_POSTURE_FIELDS:
        if field not in posture:
            findings.append(NomosAuditFinding(
                code="nomos.audit.deployment_posture_field_missing",
                message=f"deployment posture is missing {field}",
                path=f"nomos.deployment_posture.{field}",
            ))

    deployment_class = _str_value(posture.get("deployment_class"))
    if deployment_class:
        normalized["deployment_class"] = deployment_class
        _check_member(
            findings,
            value=deployment_class,
            allowed=DEPLOYMENT_CLASSES,
            code="nomos.audit.unknown_deployment_class",
            path="nomos.deployment_posture.deployment_class",
            label="deployment class",
        )

    for field in ("read_principals", "write_principals"):
        values = _declared_string_list(
            posture.get(field),
            findings,
            path=f"nomos.deployment_posture.{field}",
        )
        normalized[field] = values
        if not values and field in posture:
            _invalid_list(
                findings,
                path=f"nomos.deployment_posture.{field}",
                message=f"{field} must be a non-empty list of principal kinds",
            )
        for value in values:
            _check_member(
                findings,
                value=value,
                allowed=PRINCIPAL_KINDS,
                code="nomos.audit.unknown_principal_kind",
                path=f"nomos.deployment_posture.{field}",
                label="principal kind",
            )

    for field in ("mcp_write_default", "live_call_default"):
        value = _str_value(posture.get(field))
        if value:
            normalized[field] = value
            _check_member(
                findings,
                value=value,
                allowed=EFFECTS,
                code="nomos.audit.unknown_effect",
                path=f"nomos.deployment_posture.{field}",
                label="effect",
            )

    export_policy = _str_value(posture.get("export_policy"))
    if export_policy:
        normalized["export_policy"] = export_policy
        _check_member(
            findings,
            value=export_policy,
            allowed=EXPORT_POLICIES,
            code="nomos.audit.unknown_export_policy",
            path="nomos.deployment_posture.export_policy",
            label="export policy",
        )

    secret_handling = _str_value(posture.get("secret_handling"))
    if secret_handling:
        normalized["secret_handling"] = secret_handling
        _check_member(
            findings,
            value=secret_handling,
            allowed=SECRET_HANDLING_POLICIES,
            code="nomos.audit.unknown_secret_handling",
            path="nomos.deployment_posture.secret_handling",
            label="secret handling policy",
        )

    normalized["allowed_source_sensitivities"] = _validate_list_members(
        findings,
        posture=posture,
        field="allowed_source_sensitivities",
        allowed=SOURCE_SENSITIVITIES,
        code="nomos.audit.unknown_source_sensitivity",
        label="source sensitivity",
    )
    normalized["allowed_data_classifications"] = _validate_list_members(
        findings,
        posture=posture,
        field="allowed_data_classifications",
        allowed=DATA_CLASSIFICATIONS,
        code="nomos.audit.unknown_data_classification",
        label="data classification",
    )
    normalized["allowed_exportability"] = _validate_list_members(
        findings,
        posture=posture,
        field="allowed_exportability",
        allowed=EXPORTABILITY_POLICIES,
        code="nomos.audit.unknown_exportability",
        label="exportability",
    )
    normalized["privacy_obligations"] = _validate_list_members(
        findings,
        posture=posture,
        field="privacy_obligations",
        allowed=OBLIGATION_TYPES,
        code="nomos.audit.unknown_privacy_obligation",
        label="privacy obligation",
    )
    normalized["readiness_profiles"] = _string_list(
        _declared_string_list(
            posture.get("readiness_profiles"),
            findings,
            path="nomos.deployment_posture.readiness_profiles",
        )
    )
    if "readiness_profiles" in posture and not normalized["readiness_profiles"]:
        _invalid_list(
            findings,
            path="nomos.deployment_posture.readiness_profiles",
            message="readiness_profiles must be a non-empty list",
        )
    _check_class_requirements(findings, normalized)
    return normalized


def _check_class_requirements(
    findings: list[NomosAuditFinding],
    normalized: Mapping[str, Any],
) -> None:
    deployment_class = _str_value(normalized.get("deployment_class"))
    requirements = DEPLOYMENT_CLASS_REQUIREMENTS.get(deployment_class)
    if requirements is None:
        return
    for field, requirement_key in (
        ("export_policy", "allowed_export_policies"),
        ("mcp_write_default", "allowed_mcp_write_default"),
        ("live_call_default", "allowed_live_call_default"),
        ("secret_handling", "allowed_secret_handling"),
    ):
        allowed = set(_string_list(requirements.get(requirement_key)))
        value = _str_value(normalized.get(field))
        if allowed and value and value not in allowed:
            _class_finding(
                findings,
                deployment_class=deployment_class,
                path=f"nomos.deployment_posture.{field}",
                message=(
                    f"{deployment_class} posture requires {field} in "
                    f"{sorted(allowed)}, got {value}"
                ),
                details={"allowed": sorted(allowed), "value": value},
            )

    for field, requirement_key in (
        ("read_principals", "required_read_principals"),
        ("write_principals", "required_write_principals"),
        (
            "allowed_source_sensitivities",
            "required_allowed_source_sensitivities",
        ),
        (
            "allowed_data_classifications",
            "required_allowed_data_classifications",
        ),
        ("privacy_obligations", "required_privacy_obligations"),
    ):
        required = set(_string_list(requirements.get(requirement_key)))
        if not required:
            continue
        values = set(_string_list(normalized.get(field)))
        missing = sorted(required - values)
        if missing:
            _class_finding(
                findings,
                deployment_class=deployment_class,
                path=f"nomos.deployment_posture.{field}",
                message=(
                    f"{deployment_class} posture is missing required "
                    f"{field}: {missing}"
                ),
                details={"missing": missing},
            )

    for field, requirement_key in (
        (
            "allowed_source_sensitivities",
            "allowed_source_sensitivities_subset",
        ),
        (
            "allowed_data_classifications",
            "allowed_data_classifications_subset",
        ),
        ("allowed_exportability", "allowed_exportability_subset"),
    ):
        allowed_subset = set(_string_list(requirements.get(requirement_key)))
        if not allowed_subset:
            continue
        values = set(_string_list(normalized.get(field)))
        disallowed = sorted(values - allowed_subset)
        if disallowed:
            _class_finding(
                findings,
                deployment_class=deployment_class,
                path=f"nomos.deployment_posture.{field}",
                message=(
                    f"{deployment_class} posture allows disallowed "
                    f"{field}: {disallowed}"
                ),
                details={
                    "allowed": sorted(allowed_subset),
                    "disallowed": disallowed,
                },
            )

    forbidden_exports = set(_string_list(
        requirements.get("forbidden_export_policies")
    ))
    export_policy = _str_value(normalized.get("export_policy"))
    if export_policy and export_policy in forbidden_exports:
        _class_finding(
            findings,
            deployment_class=deployment_class,
            path="nomos.deployment_posture.export_policy",
            message=(
                f"{deployment_class} posture forbids export_policy "
                f"{export_policy}"
            ),
            details={"forbidden": sorted(forbidden_exports)},
        )


def _class_finding(
    findings: list[NomosAuditFinding],
    *,
    deployment_class: str,
    path: str,
    message: str,
    details: Mapping[str, Any],
) -> None:
    findings.append(NomosAuditFinding(
        code="nomos.audit.deployment_posture_weaker_than_class",
        message=message,
        path=path,
        details={"deployment_class": deployment_class, **dict(details)},
    ))


def _audit_source_registry(
    *,
    manifest: Mapping[str, Any],
    source_registry: Mapping[str, Any],
    posture: Mapping[str, Any],
    findings: list[NomosAuditFinding],
    warnings: list[NomosAuditFinding],
) -> dict[str, dict[str, Any]]:
    policies: dict[str, dict[str, Any]] = {}
    sources = _sources_mapping(source_registry)
    for source_name, raw_entry in sources.items():
        entry = _mapping(raw_entry)
        if entry is None:
            findings.append(NomosAuditFinding(
                code="nomos.audit.source_entry_invalid",
                message="source registry entry must be a mapping",
                path=f"sources.{source_name}",
                source_name=source_name,
            ))
            continue
        policy = _source_policy_for_entry(
            manifest=manifest,
            source_name=source_name,
            entry=entry,
            findings=findings,
        )
        if policy is None:
            continue
        policies[source_name] = _normalize_source_policy(
            source_name=source_name,
            source_entry=entry,
            policy=policy,
            posture=posture,
            findings=findings,
            warnings=warnings,
        )
    return policies


def _source_policy_for_entry(
    *,
    manifest: Mapping[str, Any],
    source_name: str,
    entry: Mapping[str, Any],
    findings: list[NomosAuditFinding],
) -> Mapping[str, Any] | None:
    explicit = _mapping(entry.get("source_policy"))
    if explicit is not None:
        return {**explicit, "policy_origin": "source_registry.source_policy"}
    if "source_policy" in entry:
        findings.append(NomosAuditFinding(
            code="nomos.audit.source_policy_invalid",
            message="source_policy must be a mapping",
            path=f"sources.{source_name}.source_policy",
            source_name=source_name,
        ))
        return None

    defaults = _mapping((_mapping(manifest.get("nomos")) or {}).get(
        "source_policy_defaults"
    )) or {}
    by_source_name = _mapping(defaults.get("by_source_name")) or {}
    by_source_class = _mapping(defaults.get("by_source_class")) or {}
    named = _mapping(by_source_name.get(source_name))
    if named is not None:
        return {
            **named,
            "policy_origin": "nomos.source_policy_defaults.by_source_name",
        }
    source_class = _str_value(entry.get("source_class"))
    class_policy = (
        _mapping(by_source_class.get(source_class))
        if source_class
        else None
    )
    if class_policy is not None:
        return {
            **class_policy,
            "policy_origin": "nomos.source_policy_defaults.by_source_class",
        }

    findings.append(NomosAuditFinding(
        code="nomos.audit.source_policy_missing",
        message=(
            "source has no source_policy and no explicit deployment "
            "source_policy_default"
        ),
        path=f"sources.{source_name}.source_policy",
        source_name=source_name,
        details={"source_class": source_class},
    ))
    return None


def _normalize_source_policy(
    *,
    source_name: str,
    source_entry: Mapping[str, Any],
    policy: Mapping[str, Any],
    posture: Mapping[str, Any],
    findings: list[NomosAuditFinding],
    warnings: list[NomosAuditFinding],
) -> dict[str, Any]:
    normalized: dict[str, Any] = {
        "policy_origin": _str_value(policy.get("policy_origin")) or "unknown",
    }
    for field in SOURCE_POLICY_FIELDS:
        if field not in policy:
            findings.append(NomosAuditFinding(
                code="nomos.audit.source_policy_field_missing",
                message=f"source policy is missing {field}",
                path=f"sources.{source_name}.source_policy.{field}",
                source_name=source_name,
            ))

    sensitivity = _str_value(policy.get("sensitivity"))
    if sensitivity:
        normalized["sensitivity"] = sensitivity
        _check_member(
            findings,
            value=sensitivity,
            allowed=SOURCE_SENSITIVITIES,
            code="nomos.audit.unknown_source_sensitivity",
            path=f"sources.{source_name}.source_policy.sensitivity",
            label="source sensitivity",
            source_name=source_name,
        )
        _check_allowed_by_posture(
            findings,
            value=sensitivity,
            allowed=posture.get("allowed_source_sensitivities"),
            code="nomos.audit.source_sensitivity_not_allowed",
            path=f"sources.{source_name}.source_policy.sensitivity",
            source_name=source_name,
            label="source sensitivity",
        )

    classification = _str_value(policy.get("data_classification"))
    if classification:
        normalized["data_classification"] = classification
        _check_member(
            findings,
            value=classification,
            allowed=DATA_CLASSIFICATIONS,
            code="nomos.audit.unknown_data_classification",
            path=f"sources.{source_name}.source_policy.data_classification",
            label="data classification",
            source_name=source_name,
        )
        _check_allowed_by_posture(
            findings,
            value=classification,
            allowed=posture.get("allowed_data_classifications"),
            code="nomos.audit.data_classification_not_allowed",
            path=f"sources.{source_name}.source_policy.data_classification",
            source_name=source_name,
            label="data classification",
        )

    credential_policy = _str_value(policy.get("credential_ref_policy"))
    if credential_policy:
        normalized["credential_ref_policy"] = credential_policy
        _check_member(
            findings,
            value=credential_policy,
            allowed=CREDENTIAL_REF_POLICIES,
            code="nomos.audit.unknown_credential_ref_policy",
            path=f"sources.{source_name}.source_policy.credential_ref_policy",
            label="credential ref policy",
            source_name=source_name,
        )
        credential_refs = _declared_string_list(
            source_entry.get("credential_refs"),
            findings,
            path=f"sources.{source_name}.credential_refs",
            source_name=source_name,
            required=False,
        )
        if credential_policy == "required" and not credential_refs:
            findings.append(NomosAuditFinding(
                code="nomos.audit.credential_ref_required",
                message=(
                    "source policy requires credential_refs but the source "
                    "entry declares none"
                ),
                path=f"sources.{source_name}.credential_refs",
                source_name=source_name,
            ))
        if credential_refs:
            normalized["credential_refs_declared"] = True

    # `bind-at-rest-anonymization`: optional at-rest posture. `store_raw`
    # (bool, default true when omitted) says whether the source may store raw
    # identifiers; `pii_classes` (attr -> pii_class) is the fail-safe classifier
    # a not-store-raw source uses to reveal / pseudonymize / mask its attrs.
    # Neither is required (omission preserves today's store-raw behavior); when
    # present each is validated so a malformed posture is caught before runtime,
    # not silently ignored.
    if "store_raw" in policy:
        store_raw_value = policy.get("store_raw")
        if isinstance(store_raw_value, bool):
            normalized["store_raw"] = store_raw_value
        else:
            findings.append(NomosAuditFinding(
                code="nomos.audit.invalid_store_raw",
                message="source_policy.store_raw must be a boolean (true/false)",
                path=f"sources.{source_name}.source_policy.store_raw",
                source_name=source_name,
                details={"value": repr(store_raw_value)},
            ))

    if "pii_classes" in policy:
        pii_classes_value = policy.get("pii_classes")
        if isinstance(pii_classes_value, Mapping):
            unknown = sorted(
                {
                    str(value)
                    for value in pii_classes_value.values()
                    if str(value) not in PII_CLASSES
                }
            )
            if unknown:
                findings.append(NomosAuditFinding(
                    code="nomos.audit.unknown_pii_class",
                    message=(
                        "source_policy.pii_classes declares unknown pii_class(es): "
                        + ", ".join(unknown)
                    ),
                    path=f"sources.{source_name}.source_policy.pii_classes",
                    source_name=source_name,
                    details={"allowed": sorted(PII_CLASSES)},
                ))
            else:
                normalized["pii_classes"] = {
                    str(key): str(value)
                    for key, value in pii_classes_value.items()
                }
        else:
            findings.append(NomosAuditFinding(
                code="nomos.audit.invalid_pii_classes",
                message=(
                    "source_policy.pii_classes must be a mapping of "
                    "attribute -> pii_class"
                ),
                path=f"sources.{source_name}.source_policy.pii_classes",
                source_name=source_name,
            ))

    obligations = _declared_string_list(
        policy.get("privacy_obligations"),
        findings,
        path=f"sources.{source_name}.source_policy.privacy_obligations",
        source_name=source_name,
    )
    normalized["privacy_obligations"] = obligations
    if "privacy_obligations" in policy and not obligations:
        _invalid_list(
            findings,
            path=f"sources.{source_name}.source_policy.privacy_obligations",
            message="privacy_obligations must be a non-empty list",
            source_name=source_name,
        )
    for obligation in obligations:
        _check_member(
            findings,
            value=obligation,
            allowed=OBLIGATION_TYPES,
            code="nomos.audit.unknown_privacy_obligation",
            path=f"sources.{source_name}.source_policy.privacy_obligations",
            label="privacy obligation",
            source_name=source_name,
        )

    live_call_allowed = policy.get("live_call_allowed")
    if isinstance(live_call_allowed, bool):
        normalized["live_call_allowed"] = live_call_allowed
        if (
            live_call_allowed
            and posture.get("live_call_default") == "deny"
        ):
            findings.append(NomosAuditFinding(
                code="nomos.audit.live_call_not_allowed",
                message=(
                    "source allows live calls while deployment posture "
                    "sets live_call_default to deny"
                ),
                path=f"sources.{source_name}.source_policy.live_call_allowed",
                source_name=source_name,
            ))
        elif (
            live_call_allowed
            and posture.get("live_call_default") == "review"
            and "review" not in set(normalized.get(
                "privacy_obligations", []
            ))
        ):
            warnings.append(NomosAuditFinding(
                code="nomos.audit.live_call_review_obligation_missing",
                severity="warning",
                message=(
                    "source allows live calls under a review-default "
                    "deployment posture but does not declare review"
                ),
                path=f"sources.{source_name}.source_policy.privacy_obligations",
                source_name=source_name,
            ))
    elif "live_call_allowed" in policy:
        findings.append(NomosAuditFinding(
            code="nomos.audit.live_call_allowed_invalid",
            message="live_call_allowed must be a boolean",
            path=f"sources.{source_name}.source_policy.live_call_allowed",
            source_name=source_name,
        ))

    exportability = _str_value(policy.get("exportability"))
    if exportability:
        normalized["exportability"] = exportability
        _check_member(
            findings,
            value=exportability,
            allowed=EXPORTABILITY_POLICIES,
            code="nomos.audit.unknown_exportability",
            path=f"sources.{source_name}.source_policy.exportability",
            label="exportability",
            source_name=source_name,
        )
        _check_allowed_by_posture(
            findings,
            value=exportability,
            allowed=posture.get("allowed_exportability"),
            code="nomos.audit.exportability_not_allowed",
            path=f"sources.{source_name}.source_policy.exportability",
            source_name=source_name,
            label="exportability",
        )
        if posture.get("export_policy") == "no_export" and exportability != "no_export":
            findings.append(NomosAuditFinding(
                code="nomos.audit.exportability_not_allowed",
                message=(
                    "source exportability must be no_export when deployment "
                    "export_policy is no_export"
                ),
                path=f"sources.{source_name}.source_policy.exportability",
                source_name=source_name,
                details={"export_policy": "no_export", "value": exportability},
            ))

    retention = _str_value(policy.get("retention"))
    if retention:
        normalized["retention"] = retention
        _check_member(
            findings,
            value=retention,
            allowed=RETENTION_POLICIES,
            code="nomos.audit.unknown_retention_policy",
            path=f"sources.{source_name}.source_policy.retention",
            label="retention policy",
            source_name=source_name,
        )

    provenance = _str_value(policy.get("provenance"))
    if provenance:
        normalized["provenance"] = provenance
        _check_member(
            findings,
            value=provenance,
            allowed=PROVENANCE_POLICIES,
            code="nomos.audit.unknown_provenance_policy",
            path=f"sources.{source_name}.source_policy.provenance",
            label="provenance policy",
            source_name=source_name,
        )

    _check_secret_source_requirements(
        source_name=source_name,
        source_entry=source_entry,
        posture=posture,
        normalized=normalized,
        findings=findings,
    )
    return normalized


def _check_secret_source_requirements(
    *,
    source_name: str,
    source_entry: Mapping[str, Any],
    posture: Mapping[str, Any],
    normalized: Mapping[str, Any],
    findings: list[NomosAuditFinding],
) -> None:
    sensitivity = _str_value(normalized.get("sensitivity"))
    classification = _str_value(normalized.get("data_classification"))
    secret_source = "secret" in {sensitivity, classification}
    if not secret_source:
        return
    credential_policy = _str_value(normalized.get("credential_ref_policy"))
    credential_refs = _declared_string_list(
        source_entry.get("credential_refs"),
        findings,
        path=f"sources.{source_name}.credential_refs",
        source_name=source_name,
        required=False,
    )
    obligations = set(_string_list(normalized.get("privacy_obligations")))
    if posture.get("secret_handling") == "no_secrets":
        findings.append(NomosAuditFinding(
            code="nomos.audit.secret_source_not_allowed",
            message="deployment posture forbids secret-bearing sources",
            path=f"sources.{source_name}.source_policy.sensitivity",
            source_name=source_name,
        ))
    if credential_policy != "required":
        findings.append(NomosAuditFinding(
            code="nomos.audit.secret_ref_required",
            message=(
                "secret-bearing sources must set credential_ref_policy "
                "to required"
            ),
            path=f"sources.{source_name}.source_policy.credential_ref_policy",
            source_name=source_name,
            details={"value": credential_policy or None},
        ))
    if not credential_refs:
        findings.append(NomosAuditFinding(
            code="nomos.audit.credential_ref_required",
            message="secret-bearing sources must declare credential_refs",
            path=f"sources.{source_name}.credential_refs",
            source_name=source_name,
        ))
    if "secret_ref_required" not in obligations:
        findings.append(NomosAuditFinding(
            code="nomos.audit.secret_ref_obligation_missing",
            message=(
                "secret-bearing sources must declare the "
                "secret_ref_required privacy obligation"
            ),
            path=f"sources.{source_name}.source_policy.privacy_obligations",
            source_name=source_name,
        ))


def _validate_list_members(
    findings: list[NomosAuditFinding],
    *,
    posture: Mapping[str, Any],
    field: str,
    allowed: frozenset[str],
    code: str,
    label: str,
) -> list[str]:
    values = _declared_string_list(
        posture.get(field),
        findings,
        path=f"nomos.deployment_posture.{field}",
    )
    if field in posture and not values:
        _invalid_list(
            findings,
            path=f"nomos.deployment_posture.{field}",
            message=f"{field} must be a non-empty list",
        )
    for value in values:
        _check_member(
            findings,
            value=value,
            allowed=allowed,
            code=code,
            path=f"nomos.deployment_posture.{field}",
            label=label,
        )
    return values


def _check_member(
    findings: list[NomosAuditFinding],
    *,
    value: str,
    allowed: frozenset[str],
    code: str,
    path: str,
    label: str,
    source_name: str | None = None,
) -> None:
    if value not in allowed:
        findings.append(NomosAuditFinding(
            code=code,
            message=f"unknown {label}: {value}",
            path=path,
            source_name=source_name,
            details={"allowed": sorted(allowed), "value": value},
        ))


def _check_allowed_by_posture(
    findings: list[NomosAuditFinding],
    *,
    value: str,
    allowed: Any,
    code: str,
    path: str,
    source_name: str,
    label: str,
) -> None:
    allowed_values = set(_string_list(allowed))
    if allowed_values and value not in allowed_values:
        findings.append(NomosAuditFinding(
            code=code,
            message=f"{label} {value} is not allowed by deployment posture",
            path=path,
            source_name=source_name,
            details={"allowed": sorted(allowed_values), "value": value},
        ))


def _invalid_list(
    findings: list[NomosAuditFinding],
    *,
    path: str,
    message: str,
    source_name: str | None = None,
) -> None:
    findings.append(NomosAuditFinding(
        code="nomos.audit.invalid_list",
        message=message,
        path=path,
        source_name=source_name,
    ))


def _sources_mapping(
    source_registry: Mapping[str, Any],
) -> Mapping[str, Any]:
    sources = source_registry.get("sources")
    if isinstance(sources, Mapping):
        return sources
    return source_registry


def _mapping(value: Any) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _str_value(value: Any) -> str:
    return value.strip() if isinstance(value, str) and value.strip() else ""


def _string_list(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if not isinstance(value, Sequence) and not isinstance(value, (set, frozenset)):
        return []
    out: list[str] = []
    for item in value:
        if isinstance(item, str) and item.strip():
            out.append(item.strip())
    return out


def _declared_string_list(
    value: Any,
    findings: list[NomosAuditFinding],
    *,
    path: str,
    source_name: str | None = None,
    required: bool = True,
) -> list[str]:
    if value is None:
        return []
    if (
        isinstance(value, (str, bytes, Mapping, set, frozenset))
        or not isinstance(value, Sequence)
    ):
        _invalid_list(
            findings,
            path=path,
            message=f"{path} must be a list of non-empty strings",
            source_name=source_name,
        )
        return []
    out: list[str] = []
    invalid = False
    for item in value:
        if isinstance(item, str) and item.strip():
            out.append(item.strip())
        else:
            invalid = True
    if invalid or (required and not out):
        _invalid_list(
            findings,
            path=path,
            message=f"{path} must be a list of non-empty strings",
            source_name=source_name,
        )
    return out
