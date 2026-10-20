"""Postgres-backed active Nomos policy and configuration state."""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Protocol

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from okg.substrate.commit_scope import db_scope_key, memoized
from okg.substrate.nomos.core import (
    CompiledConfigSet,
    CompiledPolicyBundle,
    ConfigEntry,
    DecisionInput,
    NomosPolicyError,
    PolicyDecision,
    config_set_from_mapping,
    compile_config_graph,
    compile_policy_graph,
    policy_bundle_from_mapping,
)
from okg.substrate.nomos.identity_store import persist_nomos_decision
from okg.substrate.nomos.read_masking import (
    READ_DROP_CLASSES,
    REDACTION_MARKER,
    legacy_attr_classifier,
    mask_attrs,
    safe_decision_payload,
)
from okg.substrate.searchable_text import without_producer_text
from okg.substrate.nomos.runtime_enforcement import (
    NomosPrimitiveCheck,
    NomosRuntimeOutcome,
    NomosRuntimeRequest,
    deployment_nomos_enforcement_mode,
    deployment_requires_nomos_runtime,
    evaluate_runtime_boundary,
    runtime_surface_mapping_for,
    surface_for_action,
)
from okg.substrate.nomos.source_lineage import (
    attrs_with_lineage,
    lineage_for_row,
    source_ids_from_lineage,
)
from okg.substrate.nomos.vocabulary import NomosVocabularyError
from okg.substrate.errors import SubstrateError


#: Facts the runtime boundary derives itself rather than taking from the
#: caller. Policy rules match on a flat fact map built from subject, action and
#: classification, then the request resource, then the request context
#: (``core._rule_matches``), so a caller-supplied resource or context key with
#: one of these names would stand in for the trusted fact.
#:
#: - ``deployment`` and ``scope``: the arguments this boundary was called with.
#: - ``subject``: ``request.subject`` (stripped), the identity being decided.
#: - ``action``, ``principal_kind``: fixed by the surface's canonical mapping.
#: - ``nomos_surface``, ``nomos_boundary``: the surface itself.
#: - ``classification``: ``request.classification`` or the surface default.
#:
#: ``resource_kind`` is deliberately NOT reserved: the surface mapping supplies
#: only a default (``setdefault``) and callers legitimately refine it
#: (``cli_search_boundary``, ``mcp_tool``, ``graph_export_boundary``, ...).
RESERVED_BOUNDARY_FACTS: tuple[str, ...] = (
    "deployment",
    "scope",
    "subject",
    "action",
    "classification",
    "principal_kind",
    "nomos_surface",
    "nomos_boundary",
)


class NomosReservedFactConflict(SubstrateError, NomosPolicyError):
    """A caller tried to set a boundary-owned fact to a different value.

    Raised before evaluation, so no policy decision is recorded: the request
    never reached a verdict. An equal value is accepted, because the decision
    input is identical with or without it.
    """

    code = "nomos_reserved_fact_conflict"


class NomosBoundaryRequestMalformed(SubstrateError, NomosPolicyError):
    """A request resource or context is not a mapping; refused before evaluation."""

    code = "nomos_boundary_request_malformed"


def _with_trusted_facts(
    values: Mapping[str, Any],
    trusted: Mapping[str, Any],
) -> dict[str, Any]:
    """Return ``values`` with every trusted fact written last.

    ``deployment`` and ``scope`` are always set. The other reserved facts
    replace a caller copy only when the caller supplied the key, so an honest
    request's evaluated input (and therefore its decision identity) is
    unchanged. The conflict check refuses a differing copy first; this order
    keeps the boundary's value authoritative if that check is ever weakened.
    """
    result = dict(values)
    for key in RESERVED_BOUNDARY_FACTS:
        if key in result and trusted.get(key) is not None:
            result[key] = trusted[key]
    result["deployment"] = trusted["deployment"]
    result["scope"] = trusted["scope"]
    return result


def _trusted_boundary_facts(
    *,
    deployment_name: str,
    scope: str,
    request: NomosRuntimeRequest,
) -> dict[str, Any]:
    """Return the value this boundary owns for every reserved fact.

    An unknown surface has no trusted action, principal kind or default
    classification; those facts are ``None`` here, so any caller value for
    them is refused rather than guessed at (the evaluation itself would fail
    closed as ``nomos.runtime.unknown_boundary``).
    """
    try:
        mapping = runtime_surface_mapping_for(request.surface)
    except NomosVocabularyError:
        mapping = None
    subject = request.subject
    return {
        "deployment": deployment_name,
        "scope": scope,
        "subject": subject.strip() if isinstance(subject, str) else subject,
        "action": mapping.action if mapping is not None else None,
        "classification": request.classification
        or (mapping.classification if mapping is not None else None),
        "principal_kind": mapping.principal_kind if mapping is not None else None,
        "nomos_surface": mapping.surface if mapping is not None else None,
        "nomos_boundary": mapping.surface if mapping is not None else None,
    }


def _refuse_reserved_fact_conflicts(
    *,
    trusted: Mapping[str, Any],
    surface: str,
    **supplied: Mapping[str, Any],
) -> None:
    """Refuse a caller mapping that sets a reserved fact to another value.

    A resource or context that is not a mapping is refused too: the boundary
    later turns it into a dict, so a list of key/value pairs would otherwise
    carry reserved facts past this check.
    """
    for location, values in supplied.items():
        if not isinstance(values, Mapping):
            raise NomosBoundaryRequestMalformed(
                f"Nomos boundary {surface!r} refused: the request {location} "
                f"is a {type(values).__name__}, not a mapping",
                data={
                    "location": location,
                    "value_type": type(values).__name__,
                    "surface": surface,
                    "deployment": trusted["deployment"],
                    "scope": trusted["scope"],
                },
                fix_hint=f"pass the request {location} as a mapping (dict)",
            )
        # A str subclass can override __eq__/__hash__ so that dict lookup
        # misses it while trace sanitizing later rebuilds it as a plain
        # reserved key. Only exact str keys are accepted.
        odd_key = next((k for k in values if type(k) is not str), None)
        if odd_key is not None:
            raise NomosBoundaryRequestMalformed(
                f"Nomos boundary {surface!r} refused: the request {location} "
                f"has a key of type {type(odd_key).__name__}, not str",
                data={
                    "location": location,
                    "value_type": type(values).__name__,
                    "key_type": type(odd_key).__name__,
                    "surface": surface,
                    "deployment": trusted["deployment"],
                    "scope": trusted["scope"],
                },
                fix_hint=f"use plain str keys in the request {location}",
            )
        for key in RESERVED_BOUNDARY_FACTS:
            if key in values and values[key] != trusted[key]:
                raise NomosReservedFactConflict(
                    f"Nomos boundary {surface!r} refused: the caller's "
                    f"{location} sets the reserved fact {key!r} to a value "
                    "that differs from the one this boundary derives",
                    data={
                        "key": key,
                        "location": location,
                        "surface": surface,
                        "deployment": trusted["deployment"],
                        "scope": trusted["scope"],
                    },
                    fix_hint=(
                        f"remove {key!r} from the request {location}; the "
                        "boundary sets it from the call itself"
                    ),
                )


class RuntimeNomosStorageAdapter(Protocol):
    """Optional same-transaction storage boundary for runtime evaluation.

    External-action review uses this narrow seam to route policy reads and
    decision persistence through its role-safe PostgreSQL adapter.  The
    default path below deliberately remains the existing direct store; an
    adapter is never discovered implicitly and a partial adapter is rejected
    rather than silently falling back to a privileged direct query.
    """

    def load_active_policy_bundle(
        self,
        conn: psycopg.Connection,
        *,
        deployment_name: str,
        scope: str,
    ) -> CompiledPolicyBundle | None: ...

    def persist_nomos_decision(
        self,
        conn: psycopg.Connection,
        *,
        deployment_name: str,
        decision: PolicyDecision,
        target: Mapping[str, Any],
    ) -> None: ...


def upsert_policy_bundle(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    bundle: CompiledPolicyBundle,
    status: str = "compiled",
    metadata: Mapping[str, Any] | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.nomos_policy_bundles (
              deployment_name, policy_bundle_id, policy_version,
              policy_fingerprint, compiler_version, source_generation_id,
              scope, default_effect, reason_code_catalog, bundle,
              status, effective_from, effective_until, compiled_at, metadata
            )
            VALUES (
              %(deployment_name)s, %(policy_bundle_id)s, %(policy_version)s,
              %(policy_fingerprint)s, %(compiler_version)s,
              %(source_generation_id)s, %(scope)s, %(default_effect)s,
              %(reason_code_catalog)s, %(bundle)s, %(status)s,
              %(effective_from)s, %(effective_until)s, %(compiled_at)s,
              %(metadata)s
            )
            ON CONFLICT (deployment_name, policy_fingerprint)
            DO UPDATE SET
              policy_bundle_id = EXCLUDED.policy_bundle_id,
              policy_version = EXCLUDED.policy_version,
              compiler_version = EXCLUDED.compiler_version,
              source_generation_id = EXCLUDED.source_generation_id,
              scope = EXCLUDED.scope,
              default_effect = EXCLUDED.default_effect,
              reason_code_catalog = EXCLUDED.reason_code_catalog,
              bundle = EXCLUDED.bundle,
              status = EXCLUDED.status,
              effective_from = EXCLUDED.effective_from,
              effective_until = EXCLUDED.effective_until,
              metadata = EXCLUDED.metadata,
              updated_at = now()
            """,
            {
                "deployment_name": deployment_name,
                "policy_bundle_id": bundle.policy_bundle_id,
                "policy_version": bundle.policy_version,
                "policy_fingerprint": bundle.policy_fingerprint,
                "compiler_version": bundle.compiler_version,
                "source_generation_id": bundle.source_generation_id,
                "scope": Jsonb(dict(bundle.scope)),
                "default_effect": bundle.default_effect,
                "reason_code_catalog": Jsonb(dict(bundle.reason_code_catalog)),
                "bundle": Jsonb(bundle.to_dict()),
                "status": status,
                "effective_from": bundle.effective_from,
                "effective_until": bundle.effective_until,
                "compiled_at": bundle.compiled_at,
                "metadata": Jsonb(dict(metadata or {})),
            },
        )


def seed_compatibility_policy_bundle(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    scope: str,
    activated_by: str,
) -> CompiledPolicyBundle:
    """Seed a conservative Nomos bundle from existing producer profiles."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT producer_id, admission_mode
              FROM okg.producer_profiles
             WHERE deployment_name = %s
               AND status IN ('active', 'compat')
             ORDER BY producer_id
            """,
            (deployment_name,),
        )
        profiles = list(cur.fetchall())
    rules = []
    for row in profiles:
        producer_id = str(row["producer_id"])
        admission_mode = str(row["admission_mode"] or "")
        if admission_mode not in {"direct", "fast_track"}:
            continue
        rules.append({
            "rule_id": f"compat_allow_source_emit_{_slug(producer_id)}",
            "effect": "allow",
            "priority": 10,
            "subjects": list(dict.fromkeys([
                producer_id,
                _source_subject(producer_id),
            ])),
            "actions": ["source.emit"],
            "match": {
                "deployment": deployment_name,
                "scope": scope,
            },
            "reason_code": "nomos.compat.producer_profile_allowed",
        })
    if not rules:
        rules.append({
            "rule_id": "compat_default_no_producer_profile",
            "effect": "deny",
            "priority": 0,
            "subjects": ["*"],
            "actions": ["source.emit"],
            "reason_code": "nomos.compat.no_matching_producer_profile",
        })
    bundle = compile_policy_graph({
        "policy_bundle_id": f"nomos:compat:policy:{deployment_name}",
        "policy_version": "compat-v0",
        "default_effect": "deny",
        "scope": {"deployment": deployment_name, "scope": scope},
        "rules": rules,
    })
    upsert_policy_bundle(
        conn,
        deployment_name=deployment_name,
        bundle=bundle,
        status="active",
        metadata={"compatibility_seed": True},
    )
    activate_policy_bundle(
        conn,
        deployment_name=deployment_name,
        bundle=bundle,
        scope=scope,
        activated_by=activated_by,
        metadata={"compatibility_seed": True},
    )
    return bundle


def deployment_baseline_policy_graph(
    manifest: Mapping[str, Any],
    *,
    deployment_name: str,
    scope: str,
    activated_by: str,
    source_names: Iterable[str] = (),
    source_policies: Mapping[str, Mapping[str, Any]] | None = None,
    runtime_source_names: Iterable[str] | None = None,
) -> dict[str, Any]:
    """Build the conservative rollout baseline for an enforce-mode deployment.

    This is not a fallback allow-all policy. It is the explicit first policy used
    by rollout so normal graph reads, source runs, and publish cycles can proceed
    after ``nomos.runtime_enforcement: enforce`` is enabled. The active policy is
    still scoped by deployment/scope in Postgres and defaults to deny.
    """
    posture = _manifest_deployment_posture(manifest)
    rule_match = {"deployment": deployment_name, "scope": scope}
    read_principals = _posture_principals(posture, "read_principals")
    write_principals = _posture_principals(posture, "write_principals")
    read_subjects = _subjects_for_principals(read_principals)
    write_subjects = _subjects_for_principals(write_principals)
    normalized_source_policies = dict(source_policies or {})
    source_names_set = {
        str(name).strip()
        for name in source_names
        if str(name).strip()
    }
    source_names_set.update(
        str(name).strip()
        for name in normalized_source_policies
        if str(name).strip()
    )
    source_names_set.update(
        deployment_derived_source_families(manifest)
    )
    runtime_source_names_set = {
        str(name).strip()
        for name in (
            runtime_source_names
            if runtime_source_names is not None
            else source_names
        )
        if str(name).strip()
    }
    source_id_match = sorted(source_names_set)
    source_read_match = dict(rule_match)
    if source_id_match:
        source_read_match["source_ids"] = source_id_match
    source_subjects = tuple(dict.fromkeys(
        subject
        for name in sorted(runtime_source_names_set)
        for subject in _source_subject_variants(deployment_name, name)
    ))
    rules: list[dict[str, Any]] = []
    if read_subjects:
        rules.append({
            "rule_id": "rollout_allow_manifest_read_principals",
            "effect": "allow",
            "priority": 100,
            "subjects": list(read_subjects),
            "actions": [
                "graph.read",
                "graph.read.audit",
                "graph.read.cli_search",
                "graph.read.local_operator",
            ],
            "match": source_read_match,
            "reason_code": "nomos.rollout.baseline_read_principal_allow",
            "obligations": [{"type": "audit"}],
        })
        if source_id_match:
            rules.append({
                "rule_id": "rollout_allow_manifest_cli_search_boundary",
                "effect": "allow",
                "priority": 100,
                "subjects": list(read_subjects),
                "actions": ["graph.read.cli_search"],
                "match": {
                    **rule_match,
                    "resource_kind": "cli_search_boundary",
                },
                "reason_code": "nomos.rollout.baseline_read_boundary_allow",
                "obligations": [{"type": "audit"}],
            })
            # PACT operator-ui-search-view: the operator UI authorizes each read
            # at its own boundary, so a deployment can grant or refuse the UI
            # separately from the CLI. The baseline grants it to the same read
            # principals the CLI boundary above serves.
            rules.append({
                "rule_id": "rollout_allow_manifest_local_operator_boundary",
                "effect": "allow",
                "priority": 100,
                "subjects": list(read_subjects),
                "actions": ["graph.read.local_operator"],
                "match": {
                    **rule_match,
                    "resource_kind": "local_operator_boundary",
                },
                "reason_code": "nomos.rollout.baseline_read_boundary_allow",
                "obligations": [{"type": "audit"}],
            })
        if source_names_set:
            rules.append({
                "rule_id": "rollout_allow_source_lineage_filtered_reads",
                "effect": "allow",
                "priority": 100,
                "subjects": list(read_subjects),
                "actions": [
                    "graph.read",
                    "graph.read.audit",
                    "graph.read.cli_search",
                    "graph.read.local_operator",
                    "graph.read.mcp",
                ],
                "match": {
                    **rule_match,
                    "source_ids": sorted(source_names_set),
                },
                "reason_code": "nomos.rollout.baseline_source_lineage_read_allow",
                "obligations": [{"type": "audit"}],
            })
    if "mcp_client" in read_principals:
        rules.append({
            "rule_id": "rollout_allow_manifest_mcp_graph_read",
            "effect": "allow",
            "priority": 100,
            "subjects": ["subject:mcp_client"],
            "actions": ["graph.read.mcp"],
            "match": source_read_match,
            "reason_code": "nomos.rollout.baseline_mcp_read_allow",
            "obligations": [{"type": "audit"}],
        })
        if source_id_match:
            rules.append({
                "rule_id": "rollout_allow_manifest_mcp_graph_read_boundary",
                "effect": "allow",
                "priority": 100,
                "subjects": ["subject:mcp_client"],
                "actions": ["graph.read.mcp"],
                "match": {
                    **rule_match,
                    "resource_kind": "mcp_graph_read_boundary",
                },
                "reason_code": "nomos.rollout.baseline_read_boundary_allow",
                "obligations": [{"type": "audit"}],
            })
        rules.append({
            "rule_id": "rollout_allow_manifest_mcp_read_tool_call",
            "effect": "allow",
            "priority": 100,
            "subjects": ["subject:mcp_client"],
            "actions": ["mcp.tool.call"],
            "match": {**rule_match, "resource_kind": "mcp_tool"},
            "reason_code": "nomos.rollout.baseline_mcp_read_tool_allow",
            "obligations": [{"type": "audit"}],
        })
    if source_subjects:
        rules.append({
            "rule_id": "rollout_allow_registered_source_runs",
            "effect": "allow",
            "priority": 100,
            "subjects": list(source_subjects),
            "actions": ["source.ingest", "source.emit"],
            "match": rule_match,
            "reason_code": "nomos.rollout.baseline_registered_source_allow",
            "obligations": [{"type": "audit"}],
        })
    if str(posture.get("mcp_write_default") or "deny") == "review" and write_subjects:
        rules.append({
            "rule_id": "rollout_allow_manifest_mcp_record_review_admission",
            "effect": "allow",
            "priority": 100,
            "subjects": list(write_subjects),
            "actions": ["mcp.record"],
            "match": rule_match,
            "reason_code": (
                "nomos.rollout.baseline_mcp_record_review_admission_allow"
            ),
            "obligations": [{"type": "audit"}],
        })
    live_call_default = str(posture.get("live_call_default") or "deny")
    live_call_subjects = tuple(dict.fromkeys(
        subject
        for name, policy in sorted(normalized_source_policies.items())
        if name in runtime_source_names_set
        if isinstance(policy, Mapping) and policy.get("live_call_allowed") is True
        for subject in _source_subject_variants(deployment_name, name)
    ))
    if live_call_subjects and live_call_default != "deny":
        obligations = [{"type": "audit"}]
        if live_call_default == "review":
            obligations.append({"type": "review"})
        rules.append({
            "rule_id": "rollout_live_call_for_explicit_sources",
            "effect": "allow",
            "priority": 100,
            "subjects": list(live_call_subjects),
            "actions": ["source.live_call"],
            "match": rule_match,
            "reason_code": (
                "nomos.rollout.baseline_source_live_call_review_obligation_allow"
                if live_call_default == "review"
                else "nomos.rollout.baseline_source_live_call_allow"
            ),
            "obligations": obligations,
        })
    if "okg_service" in write_principals:
        rules.append({
            "rule_id": "rollout_allow_manifest_publisher",
            "effect": "allow",
            "priority": 100,
            "subjects": ["subject:okg_service", "okg_service:publisher"],
            "actions": ["generation.publish"],
            "match": rule_match,
            "reason_code": "nomos.rollout.baseline_publish_allow",
            "obligations": [{"type": "audit"}],
        })
    if (
        str(posture.get("export_policy") or "no_export") != "no_export"
        and read_subjects
    ):
        rules.append({
            "rule_id": "rollout_allow_manifest_export_like_read",
            "effect": "allow",
            "priority": 90,
            "subjects": list(read_subjects),
            "actions": ["graph.read.export_like"],
            "match": source_read_match,
            "reason_code": "nomos.rollout.baseline_export_read_allow",
            "obligations": [{"type": "audit"}],
        })
        rules.append({
            "rule_id": "rollout_allow_manifest_export_boundary",
            "effect": "allow",
            "priority": 90,
            "subjects": list(read_subjects),
            "actions": ["graph.export"],
            "match": {
                **rule_match,
                "resource_kind": "graph_export_boundary",
            },
            "reason_code": "nomos.rollout.baseline_export_boundary_allow",
            "obligations": [{"type": "audit"}],
        })
    rules.append({
        "rule_id": "rollout_allow_explicit_activator_config",
        "effect": "allow",
        "priority": 200,
        "subjects": [activated_by],
        "actions": ["config.activate", "policy.activate"],
        "match": rule_match,
        "reason_code": "nomos.rollout.baseline_activator_allow",
        "obligations": [{"type": "audit"}],
    })
    return {
        "policy_bundle_id": f"nomos:rollout:baseline:{deployment_name}",
        "policy_version": "rollout-v1",
        "default_effect": "deny",
        "scope": {"deployment": deployment_name, "scope": scope},
        "rules": rules,
    }


def deployment_derived_source_families(
    manifest: Mapping[str, Any],
) -> set[str]:
    """Return graph source ids declared by deployment-owned derived producers."""
    families: set[str] = set()

    def visit(value: Any) -> None:
        if isinstance(value, Mapping):
            authority_scope = value.get("authority_scope")
            if isinstance(authority_scope, Mapping):
                source_family = authority_scope.get("source_family")
                if isinstance(source_family, str) and source_family.strip():
                    families.add(source_family.strip())
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(manifest)
    return families


def deployment_baseline_config_graph(
    manifest: Mapping[str, Any],
    *,
    deployment_name: str,
    scope: str,
) -> dict[str, Any]:
    posture = _manifest_deployment_posture(manifest)
    entries = [
        {
            "config_key": "runtime.enforcement_mode",
            "value": "enforce",
        },
        {
            "config_key": "deployment.class",
            "value": str(posture.get("deployment_class") or "unknown"),
        },
        {
            "config_key": "deployment.export_policy",
            "value": str(posture.get("export_policy") or "unknown"),
        },
        {
            "config_key": "deployment.mcp_write_default",
            "value": str(posture.get("mcp_write_default") or "unknown"),
        },
        {
            "config_key": "deployment.protected_material_policy",
            "value": str(posture.get("secret_handling") or "unknown"),
        },
        {
            "config_key": "deployment.live_call_default",
            "value": str(posture.get("live_call_default") or "unknown"),
        },
    ]
    return {
        "config_set_id": f"nomos:rollout:baseline-config:{deployment_name}",
        "config_version": "rollout-v1",
        "scope": {"deployment": deployment_name, "scope": scope},
        "entries": entries,
    }


def seed_deployment_baseline_nomos_state(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    manifest: Mapping[str, Any],
    scope: str | None = None,
    activated_by: str = "subject:nomos-rollout",
    source_names: Iterable[str] = (),
    source_policies: Mapping[str, Mapping[str, Any]] | None = None,
    runtime_source_names: Iterable[str] | None = None,
    policy_graph: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Ensure an enforce-mode deployment has an explicit active baseline.

    The first policy activation uses the bootstrap boundary and is refused when
    an active policy already exists. Config activation then goes through the
    newly active policy, so this still proves the normal activation path.
    """
    resolved_scope = scope or f"deployment:{deployment_name}"
    expected_policy = compile_policy_graph(
        policy_graph
        if policy_graph is not None
        else deployment_baseline_policy_graph(
            manifest,
            deployment_name=deployment_name,
            scope=resolved_scope,
            activated_by=activated_by,
            source_names=source_names,
            source_policies=source_policies,
            runtime_source_names=runtime_source_names,
        )
    )
    if (
        expected_policy.policy_bundle_id
        != f"nomos:rollout:baseline:{deployment_name}"
        or dict(expected_policy.scope)
        != {"deployment": deployment_name, "scope": resolved_scope}
    ):
        raise NomosPolicyError(
            "deployment baseline policy graph must retain the managed bundle "
            "identity and exact deployment scope"
        )
    expected_config = compile_config_graph(
        deployment_baseline_config_graph(
            manifest,
            deployment_name=deployment_name,
            scope=resolved_scope,
        ),
    )
    existing_policy = load_active_policy_bundle(
        conn,
        deployment_name=deployment_name,
        scope=resolved_scope,
    )
    existing_config = load_active_config_set(
        conn,
        deployment_name=deployment_name,
        scope=resolved_scope,
    )
    policy_activation = None
    config_activation = None
    baseline_policy_managed = existing_policy is None or (
        existing_policy.policy_bundle_id
        == f"nomos:rollout:baseline:{deployment_name}"
    )
    if existing_policy is None:
        policy_activation = activate_policy_bundle(
            conn,
            deployment_name=deployment_name,
            bundle=expected_policy,
            scope=resolved_scope,
            activated_by=activated_by,
            metadata={
                "rollout_baseline": True,
                "source_lineage_rollout": True,
            },
            bootstrap=True,
        )
    elif (
        existing_policy.policy_bundle_id
        == f"nomos:rollout:baseline:{deployment_name}"
        and existing_policy.policy_fingerprint != expected_policy.policy_fingerprint
    ):
        policy_activation = activate_policy_bundle(
            conn,
            deployment_name=deployment_name,
            bundle=expected_policy,
            scope=resolved_scope,
            activated_by=activated_by,
            metadata={
                "rollout_baseline": True,
                "source_lineage_rollout": True,
                "supersedes_policy_fingerprint": existing_policy.policy_fingerprint,
            },
        )
    if baseline_policy_managed and existing_config is None:
        config_activation = activate_config_set(
            conn,
            deployment_name=deployment_name,
            config_set=expected_config,
            scope=resolved_scope,
            activated_by=activated_by,
            metadata={"rollout_baseline": True},
        )
    elif (
        baseline_policy_managed
        and existing_config.config_set_id
        == f"nomos:rollout:baseline-config:{deployment_name}"
        and existing_config.config_fingerprint != expected_config.config_fingerprint
    ):
        config_activation = activate_config_set(
            conn,
            deployment_name=deployment_name,
            config_set=expected_config,
            scope=resolved_scope,
            activated_by=activated_by,
            metadata={
                "rollout_baseline": True,
                "supersedes_config_fingerprint": existing_config.config_fingerprint,
            },
        )
    summary = active_nomos_summary(
        conn,
        deployment_name=deployment_name,
        scope=resolved_scope,
    )
    return {
        "ok": bool(summary.get("active_policy") and summary.get("active_config")),
        "deployment_name": deployment_name,
        "scope": resolved_scope,
        "policy_activation": policy_activation,
        "config_activation": config_activation,
        "active_policy": summary.get("active_policy"),
        "active_config": summary.get("active_config"),
    }


def activate_policy_bundle(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    bundle: CompiledPolicyBundle,
    scope: str,
    activated_by: str,
    decision_id: str | None = None,
    metadata: Mapping[str, Any] | None = None,
    bootstrap: bool = False,
) -> str:
    decision_id = _authorize_activation_boundary(
        conn,
        deployment_name=deployment_name,
        scope=scope,
        surface="policy.activate",
        subject=activated_by,
        resource={
            "policy_bundle_id": bundle.policy_bundle_id,
            "policy_fingerprint": bundle.policy_fingerprint,
            "policy_version": bundle.policy_version,
            "metadata": dict(metadata or {}),
        },
        target={
            "kind": "policy_activation",
            "scope": scope,
            "policy_bundle_id": bundle.policy_bundle_id,
            "policy_fingerprint": bundle.policy_fingerprint,
        },
        bootstrap=bootstrap,
    )
    upsert_policy_bundle(
        conn,
        deployment_name=deployment_name,
        bundle=bundle,
        status="active",
        metadata=metadata,
    )
    activation_id = (
        f"nomos:policy_activation:{deployment_name}:"
        f"{scope}:{bundle.policy_fingerprint}"
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE okg.nomos_policy_activations
               SET status = 'superseded', superseded_at = now()
             WHERE deployment_name = %(deployment_name)s
               AND scope = %(scope)s
               AND status = 'active'
               AND policy_fingerprint <> %(policy_fingerprint)s
            """,
            {
                "deployment_name": deployment_name,
                "scope": scope,
                "policy_fingerprint": bundle.policy_fingerprint,
            },
        )
        cur.execute(
            """
            INSERT INTO okg.nomos_policy_activations (
              deployment_name, activation_id, scope, policy_fingerprint,
              status, activated_by, activation_decision_id, metadata
            )
            VALUES (
              %(deployment_name)s, %(activation_id)s, %(scope)s,
              %(policy_fingerprint)s, 'active', %(activated_by)s,
              %(activation_decision_id)s, %(metadata)s
            )
            ON CONFLICT (deployment_name, activation_id)
            DO UPDATE SET
              status = 'active',
              activated_by = EXCLUDED.activated_by,
              activation_decision_id = EXCLUDED.activation_decision_id,
              metadata = EXCLUDED.metadata,
              activated_at = now(),
              superseded_at = NULL
            """,
            {
                "deployment_name": deployment_name,
                "activation_id": activation_id,
                "scope": scope,
                "policy_fingerprint": bundle.policy_fingerprint,
                "activated_by": activated_by,
                "activation_decision_id": decision_id,
                "metadata": Jsonb(dict(metadata or {})),
            },
        )
    return activation_id


def seed_compatibility_config_set(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    scope: str,
    settings: Mapping[str, Any],
    activated_by: str,
) -> CompiledConfigSet:
    """Seed a Nomos config set from bootstrap settings.

    The normal config compiler performs the safety check: secret-looking
    keys with literal values are rejected and must be represented as
    ``secret_ref`` objects by the caller.
    """
    entries = []
    for key, raw_value in sorted(settings.items()):
        if isinstance(raw_value, Mapping) and "secret_ref" in raw_value:
            entries.append({
                "config_key": str(key),
                "secret_ref": str(raw_value["secret_ref"]),
                "classification": str(raw_value.get("classification") or "restricted"),
                "secret": True,
            })
        else:
            entries.append({
                "config_key": str(key),
                "value": raw_value,
                "classification": "internal",
            })
    config_set = compile_config_graph({
        "config_set_id": f"nomos:compat:config:{deployment_name}",
        "config_version": "compat-v0",
        "scope": {"deployment": deployment_name, "scope": scope},
        "entries": entries,
    })
    upsert_config_set(
        conn,
        deployment_name=deployment_name,
        config_set=config_set,
        status="active",
        metadata={"compatibility_seed": True},
    )
    activate_config_set(
        conn,
        deployment_name=deployment_name,
        config_set=config_set,
        scope=scope,
        activated_by=activated_by,
        metadata={"compatibility_seed": True},
    )
    return config_set


def load_active_policy_bundle(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    scope: str,
) -> CompiledPolicyBundle | None:
    # The active policy bundle is invariant within an ingest commit: policy
    # activation and deployment seeding are separate admin paths that never run
    # inside an ingest run, so the read-before-write on those paths is never in
    # a commit scope. Resolve at most once per commit; the frozen
    # CompiledPolicyBundle is read-only, so sharing one instance is safe.
    # Outside a commit scope this falls through to a live read.
    def _read() -> CompiledPolicyBundle | None:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT b.bundle
                  FROM okg.nomos_policy_activations a
                  JOIN okg.nomos_policy_bundles b
                    ON b.deployment_name = a.deployment_name
                   AND b.policy_fingerprint = a.policy_fingerprint
                 WHERE a.deployment_name = %s
                   AND a.scope = %s
                   AND a.status = 'active'
                   AND b.status = 'active'
                 ORDER BY a.activated_at DESC
                 LIMIT 1
                """,
                (deployment_name, scope),
            )
            row = cur.fetchone()
        if row is None:
            return None
        return _bundle_from_mapping(row["bundle"])

    return memoized(
        ("policy_bundle", db_scope_key(conn), deployment_name, scope),
        _read,
    )


def evaluate_with_active_policy(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    scope: str,
    decision_input: DecisionInput,
    enforcement_point: str,
    target: Mapping[str, Any] | None = None,
    performed_obligations: Iterable[str] | None = None,
    payload: Any | None = None,
) -> PolicyDecision:
    surface = surface_for_action(
        decision_input.action,
        fallback_surface=enforcement_point,
    )
    outcome = evaluate_runtime_boundary_with_active_policy(
        conn,
        deployment_name=deployment_name,
        scope=scope,
        request=NomosRuntimeRequest(
            surface=surface,
            subject=decision_input.subject,
            resource=decision_input.resource,
            classification=decision_input.classification,
            context=decision_input.context,
        ),
        target=target or {"kind": "active_policy_evaluation", "scope": scope},
        performed_obligations=performed_obligations,
        payload=payload,
    )
    return _effective_decision_for_callers(outcome)


_HIDE_DECISION_EFFECTS = frozenset({"deny", "review", "quarantine"})


def _effective_decision_for_callers(
    outcome: NomosRuntimeOutcome,
) -> PolicyDecision:
    """Return the decision callers should act on for the boundary outcome.

    Read-side callers (``read_filter``, ``source_lifecycle``) gate visibility on
    ``decision.effect not in {deny, review, quarantine}`` rather than on
    ``outcome.allowed``. In audit (log-and-allow) mode the recorded decision is
    the strict deny/review/quarantine (the audit trail, already persisted), but
    callers must not block: surface an allow-effect decision that carries the
    observed strict effect under context. In enforce mode this returns the
    strict decision unchanged (byte-identical).
    """
    decision = outcome.decision
    if not outcome.allowed or decision.effect not in _HIDE_DECISION_EFFECTS:
        return decision
    context = {
        **dict(decision.context),
        "nomos_audit_observed_effect": decision.effect,
        "nomos_audit_observed_reason_code": decision.reason_code,
        "nomos_enforcement_mode": "audit",
    }
    return PolicyDecision(
        decision_id=decision.decision_id,
        policy_bundle_id=decision.policy_bundle_id,
        policy_version=decision.policy_version,
        policy_fingerprint=decision.policy_fingerprint,
        input_hash=decision.input_hash,
        subject=decision.subject,
        action=decision.action,
        resource=decision.resource,
        classification=decision.classification,
        context=context,
        effect="allow",
        reason_code="nomos.runtime.audit_log_and_allow",
        matched_rule_ids=decision.matched_rule_ids,
        obligations=decision.obligations,
        enforcement_point=decision.enforcement_point,
    )


def evaluate_runtime_boundary_with_active_policy(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    scope: str,
    request: NomosRuntimeRequest,
    target: Mapping[str, Any] | None = None,
    primitive_checks: Iterable[NomosPrimitiveCheck] = (),
    performed_obligations: Iterable[str] | None = None,
    payload: Any | None = None,
    require_active: bool = True,
    enforcement_mode: str | None = None,
    storage_adapter: RuntimeNomosStorageAdapter | None = None,
) -> NomosRuntimeOutcome | None:
    """Evaluate one production runtime boundary against active Nomos state.

    ``enforcement_mode`` defaults to the deployment's resolved tri-state mode
    (``deployment_nomos_enforcement_mode``) so audit deployments evaluate and
    record the boundary but do not block; pass ``"enforce"`` explicitly to force
    strict behavior regardless of the manifest.

    The boundary owns the facts in ``RESERVED_BOUNDARY_FACTS``. A request whose
    resource or context sets one of them to a different value is refused with
    ``NomosReservedFactConflict`` before any policy is loaded or any decision
    is recorded; an equal value is accepted. A resource or context that is not
    a mapping is refused with ``NomosBoundaryRequestMalformed``.
    """
    trusted = _trusted_boundary_facts(
        deployment_name=deployment_name,
        scope=scope,
        request=request,
    )
    _refuse_reserved_fact_conflicts(
        trusted=trusted,
        surface=str(request.surface),
        resource=request.resource,
        context=request.context,
    )
    if enforcement_mode is None:
        enforcement_mode = deployment_nomos_enforcement_mode(deployment_name)
    if storage_adapter is None:
        bundle = load_active_policy_bundle(
            conn,
            deployment_name=deployment_name,
            scope=scope,
        )
    else:
        # Do not infer an adapter's shape and do not fall back to the direct
        # store when it is incomplete.  The external-action role-safe adapter
        # owns both methods and runs them on this exact caller transaction.
        try:
            load_bundle = storage_adapter.load_active_policy_bundle
            persist_decision = storage_adapter.persist_nomos_decision
        except AttributeError as exc:
            raise NomosPolicyError(
                "runtime Nomos storage adapter is incomplete; refusing the "
                "direct-store fallback"
            ) from exc
        bundle = load_bundle(
            conn,
            deployment_name=deployment_name,
            scope=scope,
        )
    if bundle is None and not require_active:
        return None
    outcome = evaluate_runtime_boundary(
        bundle,
        NomosRuntimeRequest(
            surface=request.surface,
            subject=request.subject,
            # Trusted facts are written last (see _with_trusted_facts).
            resource=_with_trusted_facts(request.resource, trusted),
            classification=request.classification,
            context=_with_trusted_facts(request.context, trusted),
        ),
        active_policy_fingerprint=(
            bundle.policy_fingerprint if bundle is not None else None
        ),
        primitive_checks=primitive_checks,
        performed_obligations=performed_obligations,
        payload=payload,
        enforcement_mode=enforcement_mode,
    )
    decision_target = target or {
        "kind": "runtime_boundary",
        "surface": request.surface,
        "scope": scope,
    }
    if storage_adapter is None:
        persist_nomos_decision(
            conn,
            deployment_name=deployment_name,
            decision=outcome.decision,
            target=decision_target,
        )
    else:
        persist_outcome = getattr(
            storage_adapter, "persist_nomos_runtime_outcome", None
        )
        if callable(persist_outcome):
            persist_outcome(
                conn,
                deployment_name=deployment_name,
                decision=outcome.decision,
                outcome=outcome,
                target=decision_target,
            )
        else:
            persist_decision(
                conn,
                deployment_name=deployment_name,
                decision=outcome.decision,
                target=decision_target,
            )
    return outcome


def resolve_active_config_entry(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    scope: str,
    config_key: str,
) -> ConfigEntry:
    """Resolve one Nomos-owned config key from active Postgres state.

    Runtime callers use this for scopes that have moved under Nomos.
    Absence of an active set or key is a hard error, not a YAML/env
    fallback opportunity.
    """
    config_set = load_active_config_set(
        conn,
        deployment_name=deployment_name,
        scope=scope,
    )
    if config_set is None:
        raise NomosPolicyError(
            "nomos config scope has no active config set: "
            f"deployment={deployment_name!r} scope={scope!r}"
        )
    for entry in config_set.entries:
        if entry.config_key == config_key:
            return entry
    raise NomosPolicyError(
        "nomos config key missing from active config set: "
        f"deployment={deployment_name!r} scope={scope!r} key={config_key!r}"
    )


def resolve_source_authorization_identity(
    *,
    producer_id: str,
    source: str,
    authority: Mapping[str, Any],
) -> str:
    """Return the immutable registry ownership bound to an admission."""
    ownership_id = authority.get("ownership_id")
    bound_producer_id = authority.get("producer_id")
    legacy_equal_identity = (
        bound_producer_id is None
        and ownership_id == producer_id
        and producer_id == source
    )
    if (
        not isinstance(ownership_id, str)
        or not ownership_id.strip()
        or ownership_id != source
        or (
            not legacy_equal_identity
            and (
                not isinstance(bound_producer_id, str)
                or not bound_producer_id.strip()
                or bound_producer_id != producer_id
            )
        )
    ):
        raise NomosPolicyError(
            "source admission identity binding is missing or inconsistent"
        )
    return ownership_id


def authorize_source_run_outcome_with_active_policy(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    scope: str,
    producer_id: str,
    source: str,
    run_id: str,
    fact_ref_count: int,
    admission_mode: str,
    graph_branch: str = "default",
    classification: str = "internal",
    require_active: bool | None = None,
) -> NomosRuntimeOutcome | None:
    """Evaluate source-run admission through the active Nomos bundle.

    Returns the whole enforcement outcome. ``outcome.allowed`` is the
    authorization answer and the only field a caller may gate on;
    ``outcome.decision.effect`` is the recorded audit verdict, which in
    ``audit`` mode deliberately preserves a strict deny that the deployment
    has chosen not to act on (issue #2123).

    Existing deployments can adopt Nomos incrementally: when
    ``require_active`` is false and no active bundle exists, this returns
    ``None`` so legacy producer-profile checks remain authoritative.
    """
    if require_active is None:
        require_active = deployment_requires_nomos_runtime(deployment_name)
    outcome = evaluate_runtime_boundary_with_active_policy(
        conn,
        deployment_name=deployment_name,
        scope=scope,
        request=NomosRuntimeRequest(
            surface="source.emit",
            subject=_source_subject(producer_id),
            classification=classification,
            resource={
                "deployment": deployment_name,
                "scope": scope,
                "source": source,
                "run_id": run_id,
                "fact_ref_count": fact_ref_count,
                "admission_mode": admission_mode,
                "graph_branch": graph_branch,
            },
        ),
        require_active=require_active,
        target={
            "kind": "source_run",
            "source": source,
            "run_id": run_id,
        },
    )
    return outcome


def authorize_source_run_with_active_policy(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    scope: str,
    producer_id: str,
    source: str,
    run_id: str,
    fact_ref_count: int,
    admission_mode: str,
    graph_branch: str = "default",
    classification: str = "internal",
    require_active: bool | None = None,
) -> PolicyDecision | None:
    """Return the decision a gating caller may act on for a source run.

    Like every other decision-returning helper in this module, the outcome
    is routed through :func:`_effective_decision_for_callers`: in ``enforce``
    mode the strict decision is returned unchanged, and in ``audit``
    (log-and-allow) mode the strict deny is surfaced as an allow decision
    carrying ``nomos_audit_observed_effect`` under context, while the strict
    verdict stays in the persisted audit row.

    This function used to return the raw recorded decision, whose ``effect``
    is the strict verdict even when the deployment has chosen not to act on
    it. It was the only decision-returning helper in the package that did,
    and gating on that effect is exactly what wedged every audit-mode
    deployment in #2123. A caller that needs the enforcement outcome itself
    uses :func:`authorize_source_run_outcome_with_active_policy` and reads
    ``outcome.allowed``.
    """
    outcome = authorize_source_run_outcome_with_active_policy(
        conn,
        deployment_name=deployment_name,
        scope=scope,
        producer_id=producer_id,
        source=source,
        run_id=run_id,
        fact_ref_count=fact_ref_count,
        admission_mode=admission_mode,
        graph_branch=graph_branch,
        classification=classification,
        require_active=require_active,
    )
    return None if outcome is None else _effective_decision_for_callers(outcome)


def _source_subject(source_id: str) -> str:
    normalized = str(source_id).strip()
    return normalized if normalized.startswith("source:") else f"source:{normalized}"


def _source_subject_variants(deployment_name: str, source_id: str) -> tuple[str, ...]:
    normalized = str(source_id).strip()
    if not normalized:
        return ()
    if normalized.startswith("source:"):
        normalized = normalized.removeprefix("source:")
    prefix = f"{deployment_name}."
    variants = [normalized]
    if normalized.startswith(prefix):
        unqualified = normalized[len(prefix):].strip()
        if unqualified:
            variants.append(unqualified)
    else:
        variants.append(f"{deployment_name}.{normalized}")
    return tuple(dict.fromkeys(_source_subject(name) for name in variants if name))


def authorize_graph_export_with_active_policy(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    scope: str,
    subject: str,
    graph_id: str,
    classification: str,
    redaction_evidence: bool = False,
    declassification_evidence: bool = False,
) -> PolicyDecision:
    """Gate graph/Poros export through the active Nomos policy."""
    performed: list[str] = []
    if redaction_evidence:
        performed.append("redaction")
    if declassification_evidence:
        performed.append("declassification")
    return evaluate_with_active_policy(
        conn,
        deployment_name=deployment_name,
        scope=scope,
        decision_input=DecisionInput(
            subject=subject,
            action="graph.export",
            classification=classification,
            resource={
                "deployment": deployment_name,
                "scope": scope,
                "resource_kind": "graph_export_boundary",
                "graph_id": graph_id,
                "redaction_evidence": redaction_evidence,
                "declassification_evidence": declassification_evidence,
            },
        ),
        enforcement_point="graph.export",
        target={
            "kind": "graph_export",
            "graph_id": graph_id,
        },
        performed_obligations=performed,
    )


def read_nodes_with_active_policy(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    scope: str,
    subject: str,
    generation_id: int,
    audit: bool = False,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Read pinned node rows through the Nomos Postgres interface."""
    visible: list[dict[str, Any]] = []
    action = "graph.read.audit" if audit else "graph.read"
    for row in _read_node_rows(conn, generation_id=generation_id, limit=limit):
        attrs = dict(row["attrs"] or {})
        lineage = _source_lineage_for_node_row(row, attrs)
        visible_row = _read_node_row_with_active_policy(
            conn,
            deployment_name=deployment_name,
            scope=scope,
            subject=subject,
            action=action,
            audit=audit,
            row=row,
            attrs=attrs,
            lineage=lineage,
        )
        if visible_row is not None:
            visible.append(visible_row)
    return visible


def read_nodes_with_identity_claim_from_postgres(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    scope: str,
    claim: Mapping[str, Any],
    generation_id: int,
    audit: bool = False,
    limit: int = 100,
    now: str | None = None,
) -> list[dict[str, Any]]:
    """Resolve an institutional claim before reading pinned node rows."""
    from okg.substrate.nomos.identity_store import (
        evaluate_identity_claim_from_postgres,
    )

    action = "graph.read.audit" if audit else "graph.read"
    visible: list[dict[str, Any]] = []
    for row in _read_node_rows(conn, generation_id=generation_id, limit=limit):
        attrs = dict(row["attrs"] or {})
        lineage = _source_lineage_for_node_row(row, attrs)
        source_ids = source_ids_from_lineage(lineage)
        if not source_ids:
            continue
        classification = str(attrs.get("classification") or "internal")
        resolved_subject: str | None = None
        source_decision_ids: list[str] = []
        hidden = False
        for source_id in source_ids:
            authorization = evaluate_identity_claim_from_postgres(
                conn,
                deployment_name=deployment_name,
                bundle=None,
                claim=claim,
                action=action,
                resource={
                    # generation_id stays OUT of the identity-bearing resource
                    # (issue #635: volatile publish position in identity defeats
                    # decision dedup); the target below records it for audit.
                    "deployment": deployment_name,
                    "scope": scope,
                    "node_id": row["node_id"],
                    "subtype": row["subtype"],
                    "audit": audit,
                    "source_id": source_id,
                    "source_ids": list(source_ids),
                    "source_lineage": dict(lineage),
                },
                scope=scope,
                classification=classification,
                now=now,
                enforcement_point="postgres.read.identity.source",
                # This library read path is generation-pinned but has no MCP
                # session, so no branch_name/catalog_version_id exists to
                # stamp; the target records the coordinates that do.
                target={
                    "kind": "identity_source_read_gate",
                    "generation_id": generation_id,
                    "node_id": row["node_id"],
                    "source_id": source_id,
                    "audit": audit,
                },
            )
            if (
                authorization.decision.effect in {"deny", "review", "quarantine"}
                or not authorization.resolved_subject
            ):
                hidden = True
                break
            if resolved_subject is None:
                resolved_subject = authorization.resolved_subject
            elif resolved_subject != authorization.resolved_subject:
                hidden = True
                break
            source_decision_ids.append(authorization.decision.decision_id)
        if hidden or not resolved_subject:
            continue
        visible_row = _read_node_row_with_active_policy(
            conn,
            deployment_name=deployment_name,
            scope=scope,
            subject=resolved_subject,
            action=action,
            audit=audit,
            row=row,
            attrs=attrs,
            lineage=lineage,
        )
        if visible_row is not None:
            visible_row["nomos_identity_source_decision_ids"] = source_decision_ids
            visible.append(visible_row)
    return visible


def _read_node_rows(
    conn: psycopg.Connection,
    *,
    generation_id: int,
    limit: int,
) -> list[dict[str, Any]]:
    from okg.substrate.graph_store import ChronosGraphStore

    graph = ChronosGraphStore.from_connection(
        conn, ensure_schema=False
    ).checkout_generation(generation_id)
    return list(graph.query(
        """
        SELECT node_id, subtype, attrs, source, source_record_id, source_revision
          FROM okg.graph_nodes
         ORDER BY node_id
         LIMIT :limit
        """,
        {"limit": limit},
    ))


def _source_lineage_for_node_row(
    row: Mapping[str, Any],
    attrs: Mapping[str, Any],
) -> dict[str, Any]:
    return lineage_for_row(
        attrs,
        source_id=row.get("source"),
        source_record_id=row.get("source_record_id"),
        source_revision=row.get("source_revision"),
    )


def _read_node_row_with_active_policy(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    scope: str,
    subject: str,
    action: str,
    audit: bool,
    row: Mapping[str, Any],
    attrs: Mapping[str, Any],
    lineage: Mapping[str, Any],
) -> dict[str, Any] | None:
    attrs = attrs_with_lineage(attrs, lineage)
    classification = str(attrs.get("classification") or "internal")
    source_ids = source_ids_from_lineage(lineage)
    if not source_ids:
        return None
    decisions: list[PolicyDecision] = []
    for source_id in source_ids:
        decision = evaluate_with_active_policy(
            conn,
            deployment_name=deployment_name,
            scope=scope,
            decision_input=DecisionInput(
                subject=subject,
                action=action,
                classification=classification,
                resource={
                    "deployment": deployment_name,
                    "scope": scope,
                    "node_id": row["node_id"],
                    "subtype": row["subtype"],
                    "audit": audit,
                    "source_id": source_id,
                    "source_ids": list(source_ids),
                    "source_lineage": dict(lineage),
                },
            ),
            enforcement_point="postgres.read",
            target={
                "kind": "node_read",
                "node_id": row["node_id"],
                "source_id": source_id,
                "audit": audit,
            },
            performed_obligations=("redaction",),
            payload=safe_decision_payload(attrs),
        )
        if decision.effect in {"deny", "review", "quarantine"}:
            return None
        decisions.append(decision)
    redaction_decision = next(
        (decision for decision in decisions if decision.effect == "redact"),
        None,
    )
    decision = redaction_decision or decisions[0]
    return {
        "node_id": row["node_id"],
        "subtype": row["subtype"],
        # `okg_producer_text` (issue #2991) repeats the end of `text`; a read
        # returns the body once, redacted or not.
        "attrs": (
            _redacted_attrs(attrs) if redaction_decision
            else without_producer_text(dict(attrs))
        ),
        "nomos_decision_id": decision.decision_id,
        "nomos_effect": decision.effect,
        "nomos_reason_code": decision.reason_code,
    }


def upsert_config_set(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    config_set: CompiledConfigSet,
    status: str = "compiled",
    metadata: Mapping[str, Any] | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.nomos_config_sets (
              deployment_name, config_set_id, config_version,
              config_fingerprint, compiler_version, source_generation_id,
              scope, entries, config_set, status,
              effective_from, effective_until, compiled_at, metadata
            )
            VALUES (
              %(deployment_name)s, %(config_set_id)s, %(config_version)s,
              %(config_fingerprint)s, %(compiler_version)s,
              %(source_generation_id)s, %(scope)s, %(entries)s,
              %(config_set)s, %(status)s, %(effective_from)s,
              %(effective_until)s, %(compiled_at)s, %(metadata)s
            )
            ON CONFLICT (deployment_name, config_fingerprint)
            DO UPDATE SET
              config_set_id = EXCLUDED.config_set_id,
              config_version = EXCLUDED.config_version,
              compiler_version = EXCLUDED.compiler_version,
              source_generation_id = EXCLUDED.source_generation_id,
              scope = EXCLUDED.scope,
              entries = EXCLUDED.entries,
              config_set = EXCLUDED.config_set,
              status = EXCLUDED.status,
              effective_from = EXCLUDED.effective_from,
              effective_until = EXCLUDED.effective_until,
              metadata = EXCLUDED.metadata,
              updated_at = now()
            """,
            {
                "deployment_name": deployment_name,
                "config_set_id": config_set.config_set_id,
                "config_version": config_set.config_version,
                "config_fingerprint": config_set.config_fingerprint,
                "compiler_version": config_set.compiler_version,
                "source_generation_id": config_set.source_generation_id,
                "scope": Jsonb(dict(config_set.scope)),
                "entries": Jsonb([entry.to_dict() for entry in config_set.entries]),
                "config_set": Jsonb(config_set.to_dict()),
                "status": status,
                "effective_from": config_set.effective_from,
                "effective_until": config_set.effective_until,
                "compiled_at": config_set.compiled_at,
                "metadata": Jsonb(dict(metadata or {})),
            },
        )


def activate_config_set(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    config_set: CompiledConfigSet,
    scope: str,
    activated_by: str,
    decision_id: str | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> str:
    decision_id = _authorize_activation_boundary(
        conn,
        deployment_name=deployment_name,
        scope=scope,
        surface="config.activate",
        subject=activated_by,
        resource={
            "config_set_id": config_set.config_set_id,
            "config_fingerprint": config_set.config_fingerprint,
            "config_version": config_set.config_version,
            "metadata": dict(metadata or {}),
        },
        target={
            "kind": "config_activation",
            "scope": scope,
            "config_set_id": config_set.config_set_id,
            "config_fingerprint": config_set.config_fingerprint,
        },
    )
    upsert_config_set(
        conn,
        deployment_name=deployment_name,
        config_set=config_set,
        status="active",
        metadata=metadata,
    )
    activation_id = (
        f"nomos:config_activation:{deployment_name}:"
        f"{scope}:{config_set.config_fingerprint}"
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE okg.nomos_config_activations
               SET status = 'superseded', superseded_at = now()
             WHERE deployment_name = %(deployment_name)s
               AND scope = %(scope)s
               AND status = 'active'
               AND config_fingerprint <> %(config_fingerprint)s
            """,
            {
                "deployment_name": deployment_name,
                "scope": scope,
                "config_fingerprint": config_set.config_fingerprint,
            },
        )
        cur.execute(
            """
            INSERT INTO okg.nomos_config_activations (
              deployment_name, activation_id, scope, config_fingerprint,
              status, activated_by, activation_decision_id, metadata
            )
            VALUES (
              %(deployment_name)s, %(activation_id)s, %(scope)s,
              %(config_fingerprint)s, 'active', %(activated_by)s,
              %(activation_decision_id)s, %(metadata)s
            )
            ON CONFLICT (deployment_name, activation_id)
            DO UPDATE SET
              status = 'active',
              activated_by = EXCLUDED.activated_by,
              activation_decision_id = EXCLUDED.activation_decision_id,
              metadata = EXCLUDED.metadata,
              activated_at = now(),
              superseded_at = NULL
            """,
            {
                "deployment_name": deployment_name,
                "activation_id": activation_id,
                "scope": scope,
                "config_fingerprint": config_set.config_fingerprint,
                "activated_by": activated_by,
                "activation_decision_id": decision_id,
                "metadata": Jsonb(dict(metadata or {})),
            },
        )
    return activation_id


def _authorize_activation_boundary(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    scope: str,
    surface: str,
    subject: str,
    resource: Mapping[str, Any],
    target: Mapping[str, Any],
    bootstrap: bool = False,
) -> str:
    # Every activation path (runtime opt-out, first-policy bootstrap, active
    # policy) places ``resource`` next to the trusted deployment and scope,
    # so refuse a conflicting copy once, before any of them runs.
    _refuse_reserved_fact_conflicts(
        trusted=_trusted_boundary_facts(
            deployment_name=deployment_name,
            scope=scope,
            request=NomosRuntimeRequest(
                surface=surface,
                subject=subject,
                classification="internal",
            ),
        ),
        surface=surface,
        resource=resource,
    )
    if not deployment_requires_nomos_runtime(deployment_name):
        return _record_activation_runtime_opt_out_decision(
            conn,
            deployment_name=deployment_name,
            scope=scope,
            surface=surface,
            subject=subject,
            resource=resource,
            target=target,
        )
    if bootstrap:
        if surface != "policy.activate":
            raise NomosPolicyError(
                f"Nomos bootstrap is not allowed for {surface}",
            )
        if load_active_policy_bundle(
            conn,
            deployment_name=deployment_name,
            scope=scope,
        ) is not None:
            raise NomosPolicyError(
                "Nomos bootstrap refused: active policy already exists for "
                f"deployment={deployment_name!r} scope={scope!r}",
            )
        outcome = _bootstrap_policy_activation_outcome(
            deployment_name=deployment_name,
            scope=scope,
            surface=surface,
            subject=subject,
            resource=resource,
        )
        persist_nomos_decision(
            conn,
            deployment_name=deployment_name,
            decision=outcome.decision,
            target={
                **dict(target),
                "bootstrap": True,
                "kind": "policy_activation_bootstrap",
            },
        )
    else:
        outcome = evaluate_runtime_boundary_with_active_policy(
            conn,
            deployment_name=deployment_name,
            scope=scope,
            request=NomosRuntimeRequest(
                surface=surface,
                subject=subject,
                classification="internal",
                resource=resource,
            ),
            target=target,
            performed_obligations=("audit",),
        )
    conn.commit()
    if outcome is not None and outcome.allowed:
        return outcome.decision.decision_id
    reason = (
        outcome.decision.reason_code
        if outcome is not None else "nomos.runtime.denied"
    )
    raise NomosPolicyError(f"Nomos denied {surface}: {reason}")


def _record_activation_runtime_opt_out_decision(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    scope: str,
    surface: str,
    subject: str,
    resource: Mapping[str, Any],
    target: Mapping[str, Any],
) -> str:
    """Record explicit evidence for deployments not enforcing Nomos gates."""
    opt_out_bundle = compile_policy_graph({
        "policy_bundle_id": f"nomos:runtime-opt-out:activation:{deployment_name}",
        "policy_version": "runtime-opt-out-v1",
        "default_effect": "deny",
        "scope": {"deployment": deployment_name, "scope": scope},
        "rules": [
            {
                "rule_id": "allow_activation_runtime_opt_out",
                "effect": "allow",
                "actions": [surface],
                "subjects": [subject],
                "match": {
                    "deployment": deployment_name,
                    "scope": scope,
                },
                "priority": 1000,
                "reason_code": "nomos.runtime.opt_out.activation",
                "obligations": [{"type": "audit"}],
            },
        ],
    })
    outcome = evaluate_runtime_boundary(
        opt_out_bundle,
        NomosRuntimeRequest(
            surface=surface,
            subject=subject,
            classification="internal",
            resource={
                **dict(resource),
                "deployment": deployment_name,
                "scope": scope,
            },
            context={
                "deployment": deployment_name,
                "scope": scope,
                "runtime_enforcement": "disabled",
            },
        ),
        active_policy_fingerprint=opt_out_bundle.policy_fingerprint,
        performed_obligations=("audit",),
    )
    persist_nomos_decision(
        conn,
        deployment_name=deployment_name,
        decision=outcome.decision,
        target={
            **dict(target),
            "activation_path": "runtime_enforcement_opt_out",
            "runtime_enforcement": "disabled",
        },
    )
    conn.commit()
    if outcome.allowed:
        return outcome.decision.decision_id
    raise NomosPolicyError(
        f"Nomos denied {surface}: {outcome.decision.reason_code}",
    )


def _bootstrap_policy_activation_outcome(
    *,
    deployment_name: str,
    scope: str,
    surface: str,
    subject: str,
    resource: Mapping[str, Any],
) -> NomosRuntimeOutcome:
    bundle = compile_policy_graph({
        "policy_bundle_id": (
            f"nomos:bootstrap:{deployment_name}:first-policy-activation"
        ),
        "policy_version": "v1",
        "default_effect": "deny",
        "scope": {"deployment": deployment_name, "scope": scope},
        "rules": [
            {
                "rule_id": "allow_explicit_first_policy_activation",
                "effect": "allow",
                "actions": ["policy.activate"],
                "subjects": [subject],
                "match": {
                    "deployment": deployment_name,
                    "scope": scope,
                },
                "priority": 1000,
                "reason_code": "nomos.bootstrap.first_policy_activation",
                "obligations": [{"type": "audit"}],
            },
        ],
    })
    return evaluate_runtime_boundary(
        bundle,
        NomosRuntimeRequest(
            surface=surface,
            subject=subject,
            classification="internal",
            resource={
                **dict(resource),
                "deployment": deployment_name,
                "scope": scope,
            },
            context={"bootstrap": True},
        ),
        active_policy_fingerprint=bundle.policy_fingerprint,
        performed_obligations=("audit",),
    )


def load_active_config_set(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    scope: str,
) -> CompiledConfigSet | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT c.config_set
              FROM okg.nomos_config_activations a
              JOIN okg.nomos_config_sets c
                ON c.deployment_name = a.deployment_name
               AND c.config_fingerprint = a.config_fingerprint
             WHERE a.deployment_name = %s
               AND a.scope = %s
               AND a.status = 'active'
               AND c.status = 'active'
             ORDER BY a.activated_at DESC
             LIMIT 1
            """,
            (deployment_name, scope),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return _config_set_from_mapping(row["config_set"])


def active_nomos_summary(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    scope: str,
) -> dict[str, Any]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT b.policy_bundle_id, b.policy_version,
                   b.policy_fingerprint, a.activated_at
              FROM okg.nomos_policy_activations a
              JOIN okg.nomos_policy_bundles b
                ON b.deployment_name = a.deployment_name
               AND b.policy_fingerprint = a.policy_fingerprint
             WHERE a.deployment_name = %s
               AND a.scope = %s
               AND a.status = 'active'
             ORDER BY a.activated_at DESC
             LIMIT 1
            """,
            (deployment_name, scope),
        )
        policy = cur.fetchone()
        cur.execute(
            """
            SELECT c.config_set_id, c.config_version,
                   c.config_fingerprint, a.activated_at
              FROM okg.nomos_config_activations a
              JOIN okg.nomos_config_sets c
                ON c.deployment_name = a.deployment_name
               AND c.config_fingerprint = a.config_fingerprint
             WHERE a.deployment_name = %s
               AND a.scope = %s
               AND a.status = 'active'
             ORDER BY a.activated_at DESC
             LIMIT 1
            """,
            (deployment_name, scope),
        )
        config = cur.fetchone()
        cur.execute(
            """
            SELECT
              (SELECT count(*) FROM okg.nomos_subject_bindings
                WHERE deployment_name = %s AND scope = %s AND status = 'active')
                AS subject_binding_count,
              (SELECT count(*) FROM okg.nomos_permission_grants
                WHERE deployment_name = %s AND scope = %s AND status = 'active')
                AS permission_grant_count,
              (SELECT count(*) FROM okg.nomos_identity_providers
                WHERE deployment_name = %s AND status = 'active')
                AS active_identity_provider_count,
              (SELECT count(*) FROM okg.nomos_external_claims
                WHERE deployment_name = %s
                  AND observed_at >= now() - interval '24 hours')
                AS recent_claim_count,
              (SELECT count(*) FROM okg.nomos_decisions
                WHERE deployment_name = %s
                  AND created_at >= now() - interval '24 hours')
                AS recent_decision_count
            """,
            (
                deployment_name,
                scope,
                deployment_name,
                scope,
                deployment_name,
                deployment_name,
                deployment_name,
            ),
        )
        counts = cur.fetchone()
    return {
        "deployment_name": deployment_name,
        "scope": scope,
        "active_policy": dict(policy) if policy else None,
        "active_config": dict(config) if config else None,
        "subject_binding_count": int(counts["subject_binding_count"]),
        "permission_grant_count": int(counts["permission_grant_count"]),
        "active_identity_provider_count": int(
            counts["active_identity_provider_count"]
        ),
        "recent_claim_count": int(counts["recent_claim_count"]),
        "recent_decision_count": int(counts["recent_decision_count"]),
    }


def _redacted_attrs(attrs: Mapping[str, Any]) -> dict[str, Any]:
    # The read/export redact effect, routed through the single classification-
    # driven transform (mask_attrs). With the behavior-preserving legacy
    # classifier + READ_DROP_CLASSES this drops the same free-text keys the old
    # hardcoded blocklist did; identifier classes (once annotated) pseudonymize.
    redacted = mask_attrs(
        attrs,
        classifier=legacy_attr_classifier,
        drop_classes=READ_DROP_CLASSES,
    )
    redacted[REDACTION_MARKER] = True
    return redacted


def _manifest_deployment_posture(manifest: Mapping[str, Any]) -> Mapping[str, Any]:
    nomos = manifest.get("nomos")
    if not isinstance(nomos, Mapping):
        raise NomosPolicyError("manifest nomos block is required")
    posture = nomos.get("deployment_posture")
    if not isinstance(posture, Mapping):
        raise NomosPolicyError("manifest nomos.deployment_posture is required")
    return posture


def _posture_principals(
    posture: Mapping[str, Any],
    field: str,
) -> tuple[str, ...]:
    values = posture.get(field)
    if not isinstance(values, Iterable) or isinstance(values, (str, bytes)):
        return ()
    return tuple(sorted({
        str(value).strip()
        for value in values
        if str(value).strip()
    }))


def _subjects_for_principals(principals: Iterable[str]) -> tuple[str, ...]:
    return tuple(f"subject:{principal}" for principal in sorted(set(principals)))


def _slug(value: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in value).strip("_") or "x"


def _bundle_from_mapping(value: Mapping[str, Any]) -> CompiledPolicyBundle:
    return policy_bundle_from_mapping(value)


def _config_set_from_mapping(value: Mapping[str, Any]) -> CompiledConfigSet:
    return config_set_from_mapping(value)
