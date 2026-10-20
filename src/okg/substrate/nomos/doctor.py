"""Operational doctor checks for Nomos policy and identity state."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import psycopg
import yaml
from okg.substrate import fastyaml
from psycopg.rows import dict_row

from okg.substrate.nomos.core import SECRET_KEY_MARKERS, looks_secret_key
from okg.substrate.nomos.source_lineage import LINEAGE_ATTR, source_ids_from_lineage
from okg.substrate.nomos.vocabulary import READ_BOUNDARY_RESOURCE_KINDS
from okg.substrate.support.user_hints import okg_prefix

# Deployment classes that carry private/regulated data and therefore MUST run
# under runtime enforcement (audit or enforce). A bypass (off) for one of these
# is a governance hole even outside the ci/dogfood readiness profiles, so the
# doctor errors on it rather than reporting healthy. Genuinely-public
# (public_reference_demo) and the profile-covered ci/dogfood classes are not
# keyed here.
GOVERNED_DEPLOYMENT_CLASSES: frozenset[str] = frozenset({
    "org_operational_private",
    "private_product_engineering",
    "personal_okg",
    "governed_research_metadata",
    "synthetic_healthcare_demo",
})


def _deployment_class_for(deployment_name: str | None) -> str | None:
    """Read nomos.deployment_posture.deployment_class from the manifest."""
    if not deployment_name:
        return None
    try:
        from okg.substrate.deployments import find_deployment_dir

        deployment_dir = find_deployment_dir(deployment_name)
        if deployment_dir is None:
            return None
        manifest_path = deployment_dir / "deployment.yaml"
        raw = fastyaml.safe_load(
            manifest_path.read_text(encoding="utf-8"),
        ) or {}
    except Exception:  # noqa: BLE001 - readiness reports, not raises.
        return None
    if not isinstance(raw, Mapping):
        return None
    nomos = raw.get("nomos")
    if not isinstance(nomos, Mapping):
        return None
    posture = nomos.get("deployment_posture")
    if not isinstance(posture, Mapping):
        return None
    value = posture.get("deployment_class")
    return str(value).strip() if isinstance(value, str) and value.strip() else None

# Build the SQL config-key secret alternation from the SHARED marker set so the
# DB-side config-set scan can never drift from the Python detector (it previously
# omitted 'accesskey' and missed later markers). Markers are a trusted constant
# (no injection risk); `_` is a SIMILAR TO single-char wildcard, which slightly
# over-matches `private_key`/`api_key` exactly as the prior literal did.
_CONFIG_SECRET_MARKER_ALT = "|".join(SECRET_KEY_MARKERS)


def _value_is_secret_ref(value: Any) -> bool:
    prefixes = ("secret://", "vault://", "keyring://", "aws-sm://")
    if isinstance(value, str):
        return value.startswith(prefixes)
    if isinstance(value, dict):
        ref = value.get("secret_ref")
        return isinstance(ref, str) and ref.startswith(prefixes)
    return False


def scan_source_registry_literal_secrets(
    deployments_root: str | Path,
) -> dict[str, Any]:
    """Scan every deployment's source_registry.yaml for secret-class params
    carrying a LITERAL value (not a secret reference). Value-free by design: the
    report carries only (deployment, source_id, config_key) locations and a
    count, never the secret value itself (per require-secret-refs-not-literals).
    """
    root = Path(deployments_root)
    locations: list[dict[str, str]] = []
    for registry in sorted(root.glob("*/source_registry.yaml")):
        deployment = registry.parent.name
        try:
            raw = fastyaml.safe_load(registry.read_text()) or {}
        except (OSError, yaml.YAMLError):
            continue
        sources = (raw.get("sources") or {}) if isinstance(raw, dict) else {}
        for source_id, entry in sources.items():
            params = (entry or {}).get("params") or {}
            if not isinstance(params, dict):
                continue
            for key, value in params.items():
                if looks_secret_key(str(key)) and not _value_is_secret_ref(value):
                    locations.append({
                        "deployment": str(deployment),
                        "source_id": str(source_id),
                        "config_key": str(key),
                    })
    return {"count": len(locations), "locations": locations}


REQUIRED_TABLES = (
    "okg.nomos_policy_bundles",
    "okg.nomos_policy_activations",
    "okg.nomos_config_sets",
    "okg.nomos_config_activations",
    "okg.nomos_identity_providers",
    "okg.nomos_external_claims",
    "okg.nomos_subject_bindings",
    "okg.nomos_permission_grants",
    "okg.nomos_decisions",
)

REQUIRED_VIEWS = (
    "okg.v_nomos_policy_bundles",
    "okg.v_nomos_policy_activations",
    "okg.v_nomos_config_sets",
    "okg.v_nomos_config_activations",
    "okg.v_nomos_identity_providers",
    "okg.v_nomos_external_claims",
    "okg.v_nomos_subject_bindings",
    "okg.v_nomos_permission_grants",
    "okg.v_nomos_decisions",
)


def nomos_doctor_report(
    conn: psycopg.Connection,
    *,
    deployment_name: str | None = None,
) -> dict[str, Any]:
    """Return a deployment-scoped Nomos health report."""
    schema_objects = _schema_object_report(conn)
    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    missing = [
        name for name, present in schema_objects.items()
        if not present
    ]
    for name in missing:
        errors.append({
            "code": "nomos_schema_object_missing",
            "object": name,
        })

    identity: dict[str, Any] = {
        "deployment_name": deployment_name,
        "provider_count": 0,
        "active_provider_count": 0,
        "binding_count": 0,
        "grant_count": 0,
        "recent_claim_count": 0,
        "recent_decision_count": 0,
        "provider_errors": [],
        "orphan_binding_count": 0,
        "unbound_grant_subject_count": 0,
    }
    policy: dict[str, Any] = {
        "deployment_name": deployment_name,
        "active_policy_count": 0,
        "active_config_count": 0,
        "active_policy_fingerprints": [],
        "active_config_fingerprints": [],
        "config_secret_literal_count": 0,
        "drift_errors": [],
    }
    if not missing:
        policy.update(_active_state_report(conn, deployment_name=deployment_name))
        if deployment_name and policy["active_policy_count"] == 0:
            errors.append({
                "code": "nomos_no_active_policy_bundle",
                "deployment_name": deployment_name,
            })
        if deployment_name and policy["active_config_count"] == 0:
            errors.append({
                "code": "nomos_no_active_config_set",
                "deployment_name": deployment_name,
            })
        if policy["config_secret_literal_count"]:
            errors.append({
                "code": "nomos_config_plaintext_secret",
                "count": policy["config_secret_literal_count"],
            })
        errors.extend(policy["drift_errors"])
        identity.update(_identity_report(conn, deployment_name=deployment_name))
        if identity["active_provider_count"] == 0:
            warnings.append({
                "code": "nomos_no_active_identity_provider",
                "deployment_name": deployment_name,
            })
        errors.extend(identity["provider_errors"])
        if identity["orphan_binding_count"]:
            warnings.append({
                "code": "nomos_orphan_subject_bindings",
                "count": identity["orphan_binding_count"],
            })
        if identity["unbound_grant_subject_count"]:
            warnings.append({
                "code": "nomos_unbound_permission_grant_subjects",
                "count": identity["unbound_grant_subject_count"],
            })
    source_lineage = (
        source_lineage_readiness_report(
            conn,
            deployment_name=deployment_name,
            readiness_profile="doctor",
        )
        if deployment_name and not missing
        else None
    )
    if source_lineage and not source_lineage.get("ok"):
        errors.extend(source_lineage.get("errors") or [])
    read_class = (
        _stored_read_class_report(conn, deployment_name=deployment_name)
        if deployment_name and not missing
        else None
    )
    if read_class is not None:
        if read_class.get("status") == "error":
            errors.append({
                "code": "nomos_read_class_check_failed",
                "error": read_class.get("error"),
                "message": read_class.get("message"),
            })
        for mismatch in read_class.get("mismatches") or ():
            errors.append({"code": "nomos_read_class_mismatch", **mismatch})

    status = "ERROR" if errors else ("WARN" if warnings else "OK")
    return {
        "ok": not errors,
        "status": status,
        "deployment_name": deployment_name,
        "schema_objects": schema_objects,
        "policy": policy,
        "identity": identity,
        "source_lineage": source_lineage,
        "read_class": read_class,
        "errors": errors,
        "warnings": warnings,
    }


def _stored_read_class_report(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
) -> dict[str, Any]:
    """Stored Nomos read classes of the latest published generation (PACT
    lane-h-nomos-read-class-stored): counts and a verification sample."""
    try:
        from okg.substrate.graph_store import ChronosGraphStore
        from okg.substrate.nomos.read_class import stored_read_class_report

        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('okg.graph_nodes') IS NOT NULL")
            graph_present = bool(cur.fetchone()[0])
        if not graph_present:
            return {"status": "missing", "mismatches": []}
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        checkpoint = store.latest_generation(
            deployment_name=deployment_name,
            status="published",
        )
        if checkpoint is None:
            return {"status": "missing", "mismatches": []}
        graph = store.checkout_generation(checkpoint.checkpoint_id)
        report = stored_read_class_report(graph)
    except Exception as exc:  # noqa: BLE001 - reported as a doctor error.
        return {
            "status": "error",
            "error": type(exc).__name__,
            "message": str(exc),
            "mismatches": [],
        }
    report["status"] = "mismatch" if report["mismatches"] else "ok"
    report["generation_id"] = checkpoint.checkpoint_id
    return report


def source_lineage_readiness_report(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    readiness_profile: str = "general",
    generation_id: str | None = None,
) -> dict[str, Any]:
    """Return the operational state of Nomos source-lineage enforcement."""
    from okg.substrate.nomos import (
        deployment_nomos_audit_readiness_exception,
        deployment_nomos_enforcement_mode,
        deployment_nomos_enforcement_mode_source,
        deployment_requires_nomos_runtime,
    )

    runtime_enforcement_mode = deployment_nomos_enforcement_mode(deployment_name)
    runtime_enforcement_mode_source = deployment_nomos_enforcement_mode_source(
        deployment_name,
    )
    runtime_enforcement = deployment_requires_nomos_runtime(deployment_name)
    blocks_actions = runtime_enforcement_mode == "enforce"
    rollout_exception = deployment_nomos_audit_readiness_exception(
        deployment_name,
        readiness_profile=readiness_profile,
    )
    deployment_class = _deployment_class_for(deployment_name)
    governed_class = deployment_class in GOVERNED_DEPLOYMENT_CLASSES
    # A governed-private deployment running ungoverned is a bypass error even
    # outside the ci/dogfood readiness profiles (keyed off deployment_class
    # severity), so it is flagged rather than reported healthy.
    protected_profile = readiness_profile in {"ci", "dogfood"} or governed_class
    schema_objects = _schema_object_report(conn)
    missing_schema = [
        name for name, present in schema_objects.items()
        if not present
    ]
    if missing_schema:
        errors: list[dict[str, Any]] = []
        mode = "bypassed"
        if runtime_enforcement or protected_profile:
            mode = "missing_schema"
            errors.append({
                "code": "nomos_source_lineage_schema_missing",
                "deployment_name": deployment_name,
                "objects": missing_schema,
            })
        return _source_lineage_readiness_payload(
            deployment_name=deployment_name,
            readiness_profile=readiness_profile,
            ok=not errors,
            mode=mode,
            runtime_enforcement=runtime_enforcement,
            runtime_enforcement_mode=runtime_enforcement_mode,
            runtime_enforcement_mode_source=runtime_enforcement_mode_source,
            blocks_actions=blocks_actions,
            rollout_exception=rollout_exception,
            active_policy_count=0,
            active_policy_fingerprints=[],
            registry=_registry_source_ids(deployment_name),
            source_policy=_empty_source_policy_coverage(),
            sources_missing_policy=[],
            generation=_empty_lineage_graph_report("not_checked")["generation"],
            missing_lineage=_empty_lineage_graph_report("not_checked")[
                "missing_lineage"
            ],
            laundered_derived_lineage=_empty_lineage_graph_report(
                "not_checked",
            )["laundered_derived_lineage"],
            graph_source_ids=[],
            stale_policy_sources=[],
            errors=errors,
            missing_schema_objects=missing_schema,
        )
    scope = f"deployment:{deployment_name}"
    active_policy = _active_policy_state_for_scope(
        conn,
        deployment_name=deployment_name,
        scope=scope,
    )
    active_fingerprints = set(active_policy["active_policy_fingerprints"])
    registry = _registry_source_ids(deployment_name)
    active_bundles = _active_policy_bundles(
        conn,
        deployment_name,
        scope=scope,
    )
    graph = _source_lineage_graph_report(
        conn,
        deployment_name=deployment_name,
        generation_id=generation_id,
        active_policy_fingerprints=active_fingerprints,
    )
    policy_required_sources = sorted({
        *registry["source_ids"],
        *graph.get("source_ids", []),
    })
    coverage = _source_policy_coverage(active_bundles, policy_required_sources)

    missing_policy_sources = (
        []
        if coverage["wildcard_source_policy"]
        else sorted(set(policy_required_sources) - set(coverage["covered_source_ids"]))
    )
    missing_lineage_count = (
        int(graph["missing_lineage"]["node_count"])
        + int(graph["missing_lineage"]["edge_count"])
    )
    stale_policy_sources = graph["stale_policy_sources"]
    laundered = graph["laundered_derived_lineage"]

    errors: list[dict[str, Any]] = []
    mode = "active"
    if not runtime_enforcement:
        mode = "bypassed"
        if protected_profile:
            errors.append({
                "code": "nomos_source_lineage_runtime_bypassed",
                "deployment_name": deployment_name,
                "readiness_profile": readiness_profile,
                "deployment_class": deployment_class,
                "governed_class": governed_class,
            })
    elif active_policy["active_policy_count"] == 0:
        mode = "missing_policy"
        errors.append({
            "code": "nomos_source_lineage_policy_missing",
            "deployment_name": deployment_name,
        })
    elif missing_policy_sources:
        mode = "missing_policy"
        errors.append({
            "code": "nomos_source_lineage_source_policy_missing",
            "deployment_name": deployment_name,
            "sources": missing_policy_sources,
        })
    elif graph["generation"].get("status") == "error":
        mode = "quarantine"
        errors.append({
            "code": "nomos_source_lineage_graph_scan_unavailable",
            "deployment_name": deployment_name,
            "generation": graph["generation"],
        })
    elif missing_lineage_count:
        mode = "quarantine"
        errors.append({
            "code": "nomos_source_lineage_missing_or_malformed_facts",
            "deployment_name": deployment_name,
            "missing_lineage": graph["missing_lineage"],
        })
    elif not laundered["measured"]:
        # An unmeasured laundering count is NOT a clean one. Reporting the
        # deployment healthy here would reproduce exactly the blindness #1544
        # is about, so this is an error with the reason attached.
        mode = "quarantine"
        errors.append({
            "code": "nomos_source_lineage_laundered_scan_unavailable",
            "deployment_name": deployment_name,
            "laundered_derived_lineage": laundered,
        })
    elif laundered["total_count"]:
        mode = "quarantine"
        errors.append({
            "code": "nomos_source_lineage_derived_producer_laundered",
            "deployment_name": deployment_name,
            "laundered_derived_lineage": laundered,
        })

    return _source_lineage_readiness_payload(
        deployment_name=deployment_name,
        readiness_profile=readiness_profile,
        ok=not errors,
        mode=mode,
        runtime_enforcement=runtime_enforcement,
        runtime_enforcement_mode=runtime_enforcement_mode,
        runtime_enforcement_mode_source=runtime_enforcement_mode_source,
        blocks_actions=blocks_actions,
        rollout_exception=rollout_exception,
        active_policy_count=active_policy["active_policy_count"],
        active_policy_fingerprints=sorted(active_fingerprints),
        registry=registry,
        source_policy=coverage,
        sources_missing_policy=missing_policy_sources,
        generation=graph["generation"],
        missing_lineage=graph["missing_lineage"],
        laundered_derived_lineage=laundered,
        graph_source_ids=graph.get("source_ids", []),
        stale_policy_sources=stale_policy_sources,
        errors=errors,
        missing_schema_objects=missing_schema,
    )


def _source_lineage_readiness_payload(
    *,
    deployment_name: str,
    readiness_profile: str,
    ok: bool,
    mode: str,
    runtime_enforcement: bool,
    runtime_enforcement_mode: str,
    runtime_enforcement_mode_source: str,
    blocks_actions: bool,
    rollout_exception: Mapping[str, Any] | None,
    active_policy_count: int,
    active_policy_fingerprints: list[str],
    registry: Mapping[str, Any],
    source_policy: Mapping[str, Any],
    sources_missing_policy: list[str],
    generation: Mapping[str, Any],
    missing_lineage: Mapping[str, Any],
    laundered_derived_lineage: Mapping[str, Any],
    graph_source_ids: list[str],
    stale_policy_sources: list[str],
    errors: list[dict[str, Any]],
    missing_schema_objects: list[str],
) -> dict[str, Any]:
    return {
        "schema": "okg.nomos.source_lineage_readiness.v1",
        "deployment_name": deployment_name,
        "readiness_profile": readiness_profile,
        "ok": ok,
        "mode": mode,
        "runtime_enforcement": runtime_enforcement,
        "runtime_enforcement_mode": runtime_enforcement_mode,
        "runtime_enforcement_mode_source": runtime_enforcement_mode_source,
        "blocks_actions": blocks_actions,
        "rollout_exception": (
            dict(rollout_exception) if isinstance(rollout_exception, Mapping) else None
        ),
        "active_policy_count": active_policy_count,
        "active_policy_fingerprints": active_policy_fingerprints,
        "missing_schema_objects": missing_schema_objects,
        "registry": registry,
        "source_policy": dict(source_policy),
        "sources_missing_policy": sources_missing_policy,
        "graph_source_ids": sorted(graph_source_ids),
        "generation": dict(generation),
        "missing_lineage": dict(missing_lineage),
        "laundered_derived_lineage": dict(laundered_derived_lineage),
        "stale_policy_sources": stale_policy_sources,
        "read_surfaces": {
            "mcp": (
                "source_lineage_filtered"
                if runtime_enforcement else "compatibility_bypass"
            ),
            "run_sql": (
                "graph_backed_sql_refused"
                if runtime_enforcement else "view_layer_allowed"
            ),
            "poros_export": (
                "source_lineage_filtered"
                if runtime_enforcement else "compatibility_bypass"
            ),
            "source_admin": (
                "source_lineage_filtered"
                if runtime_enforcement else "compatibility_bypass"
            ),
        },
        "errors": errors,
    }


def _schema_object_report(conn: psycopg.Connection) -> dict[str, bool]:
    report: dict[str, bool] = {}
    with conn.cursor() as cur:
        for name in (*REQUIRED_TABLES, *REQUIRED_VIEWS):
            cur.execute("SELECT to_regclass(%s) IS NOT NULL", (name,))
            report[name] = bool(cur.fetchone()[0])
    return report


def _registry_source_ids(deployment_name: str) -> dict[str, Any]:
    try:
        from okg.substrate.deployments import find_deployment_dir

        deployment_dir = find_deployment_dir(deployment_name)
        if deployment_dir is None:
            return {
                "status": "unavailable",
                "source_ids": [],
                "reason": "deployment_dir_not_found",
            }
        manifest_path = deployment_dir / "deployment.yaml"
        manifest = (
            fastyaml.safe_load(manifest_path.read_text(encoding="utf-8")) or {}
            if manifest_path.is_file()
            else {}
        )
        raw_registry = (
            manifest.get("source_registry", "source_registry.yaml")
            if isinstance(manifest, Mapping)
            else "source_registry.yaml"
        )
        registry_path = Path(str(raw_registry))
        if not registry_path.is_absolute():
            registry_path = deployment_dir / registry_path
        if not registry_path.exists():
            return {
                "status": "missing",
                "source_ids": [],
                "path": str(registry_path),
            }
        raw = fastyaml.safe_load(registry_path.read_text(encoding="utf-8")) or {}
        sources = raw.get("sources") if isinstance(raw, Mapping) else {}
        if not isinstance(sources, Mapping):
            sources = {}
        return {
            "status": "ok",
            "source_ids": sorted(str(source_id) for source_id in sources),
            "path": str(registry_path),
        }
    except Exception as exc:  # noqa: BLE001 - readiness reports, not raises.
        return {
            "status": "error",
            "source_ids": [],
            "error": type(exc).__name__,
            "message": str(exc),
        }


def _active_policy_bundles(
    conn: psycopg.Connection,
    deployment_name: str,
    *,
    scope: str | None = None,
) -> list[dict[str, Any]]:
    params: list[Any] = [deployment_name]
    scope_filter = ""
    if scope is not None:
        scope_filter = "AND a.scope = %s"
        params.append(scope)
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT a.policy_fingerprint, b.bundle
              FROM okg.nomos_policy_activations a
              JOIN okg.nomos_policy_bundles b
                ON b.deployment_name = a.deployment_name
               AND b.policy_fingerprint = a.policy_fingerprint
             WHERE a.deployment_name = %s
               {scope_filter}
               AND a.status = 'active'
               AND b.status = 'active'
             ORDER BY a.scope, a.activated_at DESC
            """.format(scope_filter=scope_filter),
            params,
        )
        return [dict(row) for row in cur.fetchall()]


def _active_policy_state_for_scope(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    scope: str,
) -> dict[str, Any]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT count(*) AS active_policy_count,
                   COALESCE(array_agg(policy_fingerprint ORDER BY activated_at)
                     FILTER (WHERE policy_fingerprint IS NOT NULL),
                     ARRAY[]::text[]) AS active_policy_fingerprints
              FROM okg.nomos_policy_activations
             WHERE deployment_name = %s
               AND scope = %s
               AND status = 'active'
            """,
            (deployment_name, scope),
        )
        row = cur.fetchone()
    return {
        "deployment_name": deployment_name,
        "scope": scope,
        "active_policy_count": int(row["active_policy_count"]),
        "active_policy_fingerprints": list(
            row["active_policy_fingerprints"] or []
        ),
    }


def _source_policy_coverage(
    bundles: list[dict[str, Any]],
    registry_source_ids: list[str],
) -> dict[str, Any]:
    explicit: set[str] = set()
    families: set[str] = set()
    bundles_named: set[str] = set()
    generic_read_policy_count = 0
    boundary_read_policy_count = 0
    wildcard = False
    for row in bundles:
        bundle = row.get("bundle") or {}
        rules = bundle.get("rules") if isinstance(bundle, Mapping) else []
        if not isinstance(rules, list):
            continue
        for rule in rules:
            if not isinstance(rule, Mapping):
                continue
            if rule.get("effect") not in {"allow", "redact"}:
                continue
            actions = rule.get("actions") or []
            if "*" not in actions and not any(
                str(action).startswith("graph.read")
                for action in actions
            ):
                continue
            match = rule.get("match") or {}
            if not isinstance(match, Mapping):
                continue
            if match.get("resource_kind") in READ_BOUNDARY_RESOURCE_KINDS:
                boundary_read_policy_count += 1
                continue
            if not any(
                key in match
                for key in ("source_id", "source_ids", "source_family", "source_bundle")
            ):
                generic_read_policy_count += 1
                continue
            if _match_value_contains_wildcard(
                match.get("source_id"),
                match.get("source_ids"),
                match.get("source_family"),
                match.get("source_bundle"),
            ):
                wildcard = True
                continue
            _add_string_or_list(explicit, match.get("source_id"))
            _add_string_or_list(explicit, match.get("source_ids"))
            _add_string_or_list(families, match.get("source_family"))
            _add_string_or_list(bundles_named, match.get("source_bundle"))
    covered = set(explicit)
    for source_id in registry_source_ids:
        if any(
            source_id == family or source_id.startswith(f"{family}:")
            for family in families
        ):
            covered.add(source_id)
        if any(
            source_id == bundle or source_id.startswith(f"{bundle}:")
            for bundle in bundles_named
        ):
            covered.add(source_id)
    if wildcard:
        covered.update(registry_source_ids)
    return {
        "covered_source_ids": sorted(covered),
        "explicit_source_ids": sorted(explicit),
        "source_families": sorted(families),
        "source_bundles": sorted(bundles_named),
        "wildcard_source_policy": wildcard,
        "generic_read_policy_count": generic_read_policy_count,
        "boundary_read_policy_count": boundary_read_policy_count,
    }


def _empty_source_policy_coverage() -> dict[str, Any]:
    return {
        "covered_source_ids": [],
        "explicit_source_ids": [],
        "source_families": [],
        "source_bundles": [],
        "wildcard_source_policy": False,
        "generic_read_policy_count": 0,
        "boundary_read_policy_count": 0,
    }


def _add_string_or_list(target: set[str], value: Any) -> None:
    if isinstance(value, str) and value:
        target.add(value)
    elif isinstance(value, list):
        for item in value:
            if isinstance(item, str) and item:
                target.add(item)


def _match_value_contains_wildcard(*values: Any) -> bool:
    for value in values:
        if value == "*":
            return True
        if isinstance(value, list) and "*" in value:
            return True
    return False


def _source_lineage_graph_report(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    generation_id: str | None,
    active_policy_fingerprints: set[str],
) -> dict[str, Any]:
    try:
        from okg.substrate.graph_store import ChronosGraphStore

        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        checkpoint_id = generation_id
        checkpoint = None
        if checkpoint_id is None:
            checkpoint = store.latest_generation(
                deployment_name=deployment_name,
                status="published",
            )
            checkpoint_id = checkpoint.checkpoint_id if checkpoint else None
        if checkpoint_id is None:
            return _empty_lineage_graph_report("missing")
        graph = store.checkout_generation(checkpoint_id)
        node_rows = graph.query(
            """
            SELECT attrs, source
              FROM okg.graph_nodes
             ORDER BY node_id
             LIMIT 10000
            """
        )
        edge_rows = graph.query(
            """
            SELECT attrs, source
              FROM okg.graph_edges
             ORDER BY edge_id
             LIMIT 10000
            """
        )
        # Counted on the SAME checkout, and exactly rather than from the
        # 10000-row sample above: a health flag that has to reach zero cannot
        # be read off a partial scan (#1544).
        laundered = _laundered_derived_lineage_report(
            conn,
            graph,
            deployment_name=deployment_name,
        )
    except Exception as exc:  # noqa: BLE001 - readiness reports, not raises.
        report = _empty_lineage_graph_report("error")
        report["generation"]["error"] = type(exc).__name__
        report["generation"]["message"] = str(exc)
        return report

    missing_nodes = _missing_lineage_count(node_rows)
    missing_edges = _missing_lineage_count(edge_rows)
    stale_sources = sorted(
        _stale_policy_sources(
            [*node_rows, *edge_rows],
            active_policy_fingerprints=active_policy_fingerprints,
        )
    )
    source_ids = sorted(_graph_lineage_source_ids([*node_rows, *edge_rows]))
    return {
        "generation": {
            "status": "published",
            "generation_id": checkpoint_id,
            "sample_limit": 10000,
            "node_sample_truncated": len(node_rows) >= 10000,
            "edge_sample_truncated": len(edge_rows) >= 10000,
        },
        "missing_lineage": {
            "node_count": missing_nodes,
            "edge_count": missing_edges,
            "sampled": True,
        },
        "laundered_derived_lineage": laundered,
        "source_ids": source_ids,
        "stale_policy_sources": stale_sources,
    }


def _empty_lineage_graph_report(status: str) -> dict[str, Any]:
    return {
        "generation": {"status": status, "generation_id": None},
        "missing_lineage": {"node_count": 0, "edge_count": 0, "sampled": True},
        "laundered_derived_lineage": _unmeasured_laundered_report(status),
        "source_ids": [],
        "stale_policy_sources": [],
    }


def _unmeasured_laundered_report(status: str) -> dict[str, Any]:
    """A laundered-lineage block that cannot be read as clean.

    Counts are ``None``, never ``0``. A graph the doctor could not scan is an
    ABSENT measurement, and an absent measurement that reports zero is exactly
    how #1544's laundered rows stayed invisible for six weeks.
    """
    return {
        "schema": "okg.nomos.laundered_derived_lineage_health.v1",
        "measured": False,
        "status": status,
        "node_count": None,
        "edge_count": None,
        "total_count": None,
        "producer_id_count": None,
        "sampled": False,
    }


def _laundered_derived_lineage_report(
    conn: psycopg.Connection,
    graph: Any,
    *,
    deployment_name: str,
) -> dict[str, Any]:
    """Count derived rows still carrying their EMITTER as a lineage source.

    These rows read as perfectly valid lineage — ``status: ok`` with a source
    id — so ``_missing_lineage_count`` above cannot see them. What is wrong is
    WHICH id they name: a producer, not a root source, so a reader inherits the
    producer's declared trust label instead of the root's. Every derived row
    written between PR #788 and PR #1535 has this shape (#1534, #1544).

    Fail-loud, not fail-quiet: when the producer inventory cannot be read the
    block reports ``measured: False`` with ``None`` counts.
    """
    from okg.substrate.nomos.source_lineage_repair import (
        count_laundered_derived_rows,
        derived_producer_inventory,
    )

    inventory = derived_producer_inventory(
        conn,
        deployment_name=deployment_name,
    )
    if not inventory["measured"]:
        report = _unmeasured_laundered_report(inventory["status"])
        report["errors"] = inventory["errors"]
        return report
    producer_ids = frozenset(inventory["producer_ids"])
    counts = count_laundered_derived_rows(graph, producer_ids=producer_ids)
    nodes = counts["nodes"]
    edges = counts["edges"]
    node_count = nodes["producer_named"]
    edge_count = edges["producer_named"]
    return {
        "schema": "okg.nomos.laundered_derived_lineage_health.v1",
        "measured": True,
        "status": inventory["status"],
        "node_count": node_count,
        "edge_count": edge_count,
        "total_count": node_count + edge_count,
        "producer_id_count": len(producer_ids),
        # Exact, not sampled: the count is narrowed to producer-attributed
        # rows and scanned to the end of the generation.
        "sampled": False,
        # Producer-attributed rows with NO usable lineage are already counted
        # by missing_lineage; reported here for the operator, not added in.
        "producer_attributed_without_lineage": {
            "node_count": nodes["missing"],
            "edge_count": edges["missing"],
        },
        "repair_command": (
            f"{okg_prefix()} deployment nomos-source-lineage-rollout-audit "
            f"--repair-laundered-derived {deployment_name} --json"
        ),
    }


def _missing_lineage_count(rows: list[Mapping[str, Any]]) -> int:
    count = 0
    for row in rows:
        attrs = row.get("attrs") or {}
        lineage = attrs.get(LINEAGE_ATTR) if isinstance(attrs, Mapping) else None
        if source_ids_from_lineage(lineage):
            continue
        if lineage is not None or row.get("source") is None:
            count += 1
    return count


def _stale_policy_sources(
    rows: list[Mapping[str, Any]],
    *,
    active_policy_fingerprints: set[str],
) -> set[str]:
    if not active_policy_fingerprints:
        return set()
    stale: set[str] = set()
    for row in rows:
        attrs = row.get("attrs") or {}
        lineage = attrs.get(LINEAGE_ATTR) if isinstance(attrs, Mapping) else None
        if not isinstance(lineage, Mapping):
            continue
        for source in lineage.get("sources") or []:
            if not isinstance(source, Mapping):
                continue
            fingerprints = {
                str(fingerprint)
                for fingerprint in source.get("policy_fingerprints") or []
                if fingerprint
            }
            if fingerprints and fingerprints.isdisjoint(active_policy_fingerprints):
                source_id = source.get("source_id") or source.get("id")
                if source_id:
                    stale.add(str(source_id))
    return stale


def _graph_lineage_source_ids(rows: list[Mapping[str, Any]]) -> set[str]:
    source_ids: set[str] = set()
    for row in rows:
        attrs = row.get("attrs") or {}
        lineage = attrs.get(LINEAGE_ATTR) if isinstance(attrs, Mapping) else None
        lineage_source_ids = source_ids_from_lineage(lineage)
        if lineage_source_ids:
            source_ids.update(lineage_source_ids)
        elif lineage is None and row.get("source"):
            source_ids.add(str(row["source"]))
    return source_ids


def _active_state_report(
    conn: psycopg.Connection,
    *,
    deployment_name: str | None,
) -> dict[str, Any]:
    params: dict[str, Any] = {}
    deployment_filter = ""
    if deployment_name:
        deployment_filter = "WHERE deployment_name = %(deployment_name)s"
        params["deployment_name"] = deployment_name
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            SELECT count(*) AS active_policy_count,
                   COALESCE(array_agg(policy_fingerprint ORDER BY scope)
                     FILTER (WHERE policy_fingerprint IS NOT NULL),
                     ARRAY[]::text[]) AS active_policy_fingerprints
              FROM okg.nomos_policy_activations
              {deployment_filter}
               { _and_or_where(deployment_filter) }
                   status = 'active'
            """,
            params,
        )
        policy = cur.fetchone()
        cur.execute(
            f"""
            SELECT count(*) AS active_config_count,
                   COALESCE(array_agg(config_fingerprint ORDER BY scope)
                     FILTER (WHERE config_fingerprint IS NOT NULL),
                     ARRAY[]::text[]) AS active_config_fingerprints
              FROM okg.nomos_config_activations
              {deployment_filter}
               { _and_or_where(deployment_filter) }
                   status = 'active'
            """,
            params,
        )
        config = cur.fetchone()
        cur.execute(
            f"""
            SELECT count(*) AS config_secret_literal_count
              FROM okg.nomos_config_sets
              {deployment_filter}
               { _and_or_where(deployment_filter) }
                   EXISTS (
                     SELECT 1
                       FROM jsonb_array_elements(config_set->'entries') entry
                      WHERE entry ? 'value'
                        AND (
                          entry->>'secret_ref' IS NULL
                          AND lower(entry->>'config_key') SIMILAR TO
                            '%%({_CONFIG_SECRET_MARKER_ALT})%%'
                        )
                   )
            """,
            params,
        )
        secret_count = int(cur.fetchone()["config_secret_literal_count"])
        cur.execute(
            f"""
            SELECT deployment_name, config_set_id, config_fingerprint,
                   metadata
              FROM okg.nomos_config_sets
              {deployment_filter}
               { _and_or_where(deployment_filter) }
                   metadata ? 'expected_config_fingerprint'
               AND metadata->>'expected_config_fingerprint'
                   <> config_fingerprint
            """,
            params,
        )
        config_drift = list(cur.fetchall())

    drift_errors = [
        {
            "code": "nomos_config_drift",
            "deployment_name": row["deployment_name"],
            "config_set_id": row["config_set_id"],
            "expected_config_fingerprint": (
                dict(row["metadata"] or {}).get("expected_config_fingerprint")
            ),
            "actual_config_fingerprint": row["config_fingerprint"],
        }
        for row in config_drift
    ]
    return {
        "deployment_name": deployment_name,
        "active_policy_count": int(policy["active_policy_count"]),
        "active_config_count": int(config["active_config_count"]),
        "active_policy_fingerprints": list(
            policy["active_policy_fingerprints"] or []
        ),
        "active_config_fingerprints": list(
            config["active_config_fingerprints"] or []
        ),
        "config_secret_literal_count": secret_count,
        "drift_errors": drift_errors,
    }


def _identity_report(
    conn: psycopg.Connection,
    *,
    deployment_name: str | None,
) -> dict[str, Any]:
    params: dict[str, Any] = {}
    deployment_filter = ""
    if deployment_name:
        deployment_filter = "WHERE deployment_name = %(deployment_name)s"
        params["deployment_name"] = deployment_name

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            SELECT count(*) AS provider_count,
                   count(*) FILTER (WHERE status = 'active')
                     AS active_provider_count
              FROM okg.nomos_identity_providers
              {deployment_filter}
            """,
            params,
        )
        provider_counts = cur.fetchone()
        cur.execute(
            f"""
            SELECT count(*) AS binding_count
              FROM okg.nomos_subject_bindings
              {deployment_filter}
            """,
            params,
        )
        binding_count = int(cur.fetchone()["binding_count"])
        cur.execute(
            f"""
            SELECT count(*) AS grant_count
              FROM okg.nomos_permission_grants
              {deployment_filter}
            """,
            params,
        )
        grant_count = int(cur.fetchone()["grant_count"])
        cur.execute(
            f"""
            SELECT count(*) AS recent_claim_count
              FROM okg.nomos_external_claims
              {deployment_filter}
               { _and_or_where(deployment_filter) }
                   observed_at >= now() - interval '24 hours'
            """,
            params,
        )
        recent_claim_count = int(cur.fetchone()["recent_claim_count"])
        cur.execute(
            f"""
            SELECT count(*) AS recent_decision_count
              FROM okg.nomos_decisions
              {deployment_filter}
               { _and_or_where(deployment_filter) }
                   created_at >= now() - interval '24 hours'
            """,
            params,
        )
        recent_decision_count = int(cur.fetchone()["recent_decision_count"])
        cur.execute(
            f"""
            SELECT deployment_name, provider_id, provider_type, issuer,
                   audiences, key_reference, secret_references,
                   claim_allowlist, required_assurance, mfa_required,
                   group_claims_approved, metadata_fingerprint,
                   realm, service_principal, freshness_seconds, status,
                   metadata
              FROM okg.nomos_identity_providers
              {deployment_filter}
             ORDER BY deployment_name, provider_id
            """,
            params,
        )
        providers = list(cur.fetchall())
        cur.execute(
            f"""
            SELECT count(*) AS orphan_binding_count
              FROM okg.nomos_subject_bindings b
              LEFT JOIN okg.nomos_identity_providers p
                ON p.deployment_name = b.deployment_name
               AND p.provider_id = b.provider_id
             WHERE p.provider_id IS NULL
               { _optional_deployment_clause("b", deployment_name) }
            """,
            params,
        )
        orphan_binding_count = int(cur.fetchone()["orphan_binding_count"])
        cur.execute(
            f"""
            SELECT count(DISTINCT g.subject) AS unbound_grant_subject_count
              FROM okg.nomos_permission_grants g
              LEFT JOIN okg.nomos_subject_bindings b
                ON b.deployment_name = g.deployment_name
               AND b.subject = g.subject
               AND b.status = 'active'
             WHERE b.binding_id IS NULL
               AND g.status = 'active'
               { _optional_deployment_clause("g", deployment_name) }
            """,
            params,
        )
        unbound_grant_subject_count = int(
            cur.fetchone()["unbound_grant_subject_count"]
        )

    return {
        "deployment_name": deployment_name,
        "provider_count": int(provider_counts["provider_count"]),
        "active_provider_count": int(provider_counts["active_provider_count"]),
        "binding_count": binding_count,
        "grant_count": grant_count,
        "recent_claim_count": recent_claim_count,
        "recent_decision_count": recent_decision_count,
        "provider_errors": _provider_errors(providers),
        "orphan_binding_count": orphan_binding_count,
        "unbound_grant_subject_count": unbound_grant_subject_count,
    }


def _provider_errors(providers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    errors: list[dict[str, Any]] = []
    for provider in providers:
        base = {
            "deployment_name": provider["deployment_name"],
            "provider_id": provider["provider_id"],
        }
        provider_type = str(provider["provider_type"] or "")
        status = str(provider["status"] or "")
        metadata = dict(provider["metadata"] or {})
        is_production = _metadata_bool(metadata.get("production"))
        if not str(provider["issuer"] or "").strip():
            errors.append({
                **base,
                "code": "nomos_identity_provider_missing_issuer",
            })
        if not provider["audiences"]:
            errors.append({
                **base,
                "code": "nomos_identity_provider_missing_audience",
            })
        if not str(provider["key_reference"] or "").strip():
            errors.append({
                **base,
                "code": "nomos_identity_provider_missing_key_reference",
            })
        if provider["freshness_seconds"] is None:
            errors.append({
                **base,
                "code": "nomos_identity_provider_missing_freshness_policy",
            })
        if status == "revoked":
            errors.append({
                **base,
                "code": "nomos_identity_provider_revoked",
            })
        if status == "disabled":
            errors.append({
                **base,
                "code": "nomos_identity_provider_disabled",
            })
        if status == "rotating":
            errors.append({
                **base,
                "code": "nomos_identity_provider_rotating",
            })
        if provider_type == "kerberos_gssapi":
            if not str(provider["realm"] or "").strip():
                errors.append({
                    **base,
                    "code": "nomos_identity_provider_missing_realm",
                })
            if not str(provider["service_principal"] or "").strip():
                errors.append({
                    **base,
                    "code": "nomos_identity_provider_missing_service_principal",
                })
            refs = list(provider["secret_references"] or [])
            if not refs and not str(provider["key_reference"] or "").strip():
                errors.append({
                    **base,
                    "code": "nomos_identity_provider_missing_keytab_reference",
                })
        if provider_type in {"oidc", "saml", "touchstone_oidc"}:
            if not provider["mfa_required"]:
                errors.append({
                    **base,
                    "code": "nomos_identity_provider_missing_mfa_requirement",
                })
            if not provider["required_assurance"]:
                errors.append({
                    **base,
                    "code": "nomos_identity_provider_missing_required_assurance",
                })
        if provider["group_claims_approved"] and not provider["claim_allowlist"]:
            errors.append({
                **base,
                "code": "nomos_identity_provider_unbounded_group_claims",
            })
        if is_production:
            errors.extend(_production_provider_errors(provider, metadata, base))
        expected_issuer = metadata.get("expected_issuer")
        if expected_issuer and expected_issuer != provider["issuer"]:
            errors.append({
                **base,
                "code": "nomos_identity_provider_drift",
                "field": "issuer",
                "expected": expected_issuer,
                "actual": provider["issuer"],
            })
        expected_key = metadata.get("expected_key_reference")
        if expected_key and expected_key != provider["key_reference"]:
            errors.append({
                **base,
                "code": "nomos_identity_provider_drift",
                "field": "key_reference",
                "expected": expected_key,
                "actual": provider["key_reference"],
            })
        expected_audiences = metadata.get("expected_audiences")
        expected_audience_values = (
            [str(item) for item in expected_audiences]
            if isinstance(expected_audiences, list | tuple)
            else []
        )
        if expected_audience_values and sorted(expected_audience_values) != sorted(
            provider["audiences"] or [],
        ):
            errors.append({
                **base,
                "code": "nomos_identity_provider_drift",
                "field": "audiences",
                "expected": sorted(expected_audiences),
                "actual": sorted(provider["audiences"] or []),
            })
        expected_status = metadata.get("expected_status")
        if expected_status and expected_status != provider["status"]:
            errors.append({
                **base,
                "code": "nomos_identity_provider_drift",
                "field": "status",
                "expected": expected_status,
                "actual": provider["status"],
            })
        expected_assurance = metadata.get("expected_required_assurance")
        expected_assurance_values = (
            [str(item) for item in expected_assurance]
            if isinstance(expected_assurance, list | tuple)
            else []
        )
        if expected_assurance_values and sorted(expected_assurance_values) != sorted(
            provider["required_assurance"] or [],
        ):
            errors.append({
                **base,
                "code": "nomos_identity_provider_drift",
                "field": "required_assurance",
                "expected": sorted(expected_assurance_values),
                "actual": sorted(provider["required_assurance"] or []),
            })
        expected_freshness_seconds = metadata.get("expected_freshness_seconds")
        if (
            expected_freshness_seconds is not None
            and int(expected_freshness_seconds) != provider["freshness_seconds"]
        ):
            errors.append({
                **base,
                "code": "nomos_identity_provider_drift",
                "field": "freshness_seconds",
                "expected": int(expected_freshness_seconds),
                "actual": provider["freshness_seconds"],
            })
        expected_metadata_fingerprint = metadata.get("expected_metadata_fingerprint")
        if (
            expected_metadata_fingerprint
            and expected_metadata_fingerprint != provider["metadata_fingerprint"]
        ):
            errors.append({
                **base,
                "code": "nomos_identity_provider_drift",
                "field": "metadata_fingerprint",
                "expected": expected_metadata_fingerprint,
                "actual": provider["metadata_fingerprint"],
            })
        expected_service_principal = metadata.get("expected_service_principal")
        if (
            expected_service_principal
            and expected_service_principal != provider["service_principal"]
        ):
            errors.append({
                **base,
                "code": "nomos_identity_provider_drift",
                "field": "service_principal",
                "expected": expected_service_principal,
                "actual": provider["service_principal"],
            })
    return errors


def _production_provider_errors(
    provider: dict[str, Any],
    metadata: dict[str, Any],
    base: dict[str, Any],
) -> list[dict[str, Any]]:
    errors: list[dict[str, Any]] = []
    if metadata.get("fixture_profile"):
        errors.append({
            **base,
            "code": "nomos_identity_provider_fixture_in_production",
        })
    key_reference = str(provider["key_reference"] or "")
    secret_references = [str(item) for item in provider["secret_references"] or []]
    if _has_fixture_secret_reference([key_reference, *secret_references]):
        errors.append({
            **base,
            "code": "nomos_identity_provider_fixture_secret_reference",
        })
    if not str(provider["metadata_fingerprint"] or "").strip():
        errors.append({
            **base,
            "code": "nomos_identity_provider_missing_metadata_fingerprint",
        })
    trusted_verifiers = metadata.get("trusted_verifiers")
    if not isinstance(trusted_verifiers, list) or not trusted_verifiers:
        errors.append({
            **base,
            "code": "nomos_identity_provider_missing_trusted_verifier",
        })
    for field in (
        "expected_issuer",
        "expected_audiences",
        "expected_key_reference",
        "expected_required_assurance",
        "expected_metadata_fingerprint",
        "expected_freshness_seconds",
        "expected_status",
    ):
        if field not in metadata:
            errors.append({
                **base,
                "code": "nomos_identity_provider_missing_expected_metadata",
                "field": field,
            })
    if (
        provider["provider_type"] == "kerberos_gssapi"
        and "expected_service_principal" not in metadata
    ):
        errors.append({
            **base,
            "code": "nomos_identity_provider_missing_expected_metadata",
            "field": "expected_service_principal",
        })
    return errors


def _has_fixture_secret_reference(references: list[str]) -> bool:
    return any("/fixtures/" in reference for reference in references if reference)


def _metadata_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _and_or_where(existing_where: str) -> str:
    return "AND" if existing_where else "WHERE"


def _optional_deployment_clause(alias: str, deployment_name: str | None) -> str:
    if not deployment_name:
        return ""
    return f"AND {alias}.deployment_name = %(deployment_name)s"
