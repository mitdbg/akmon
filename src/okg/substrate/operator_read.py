"""Operator read identity and the operator UI's read boundary.

Two surfaces read the graph on behalf of the local operator: the CLI
(``graph.read.cli_search``) and the operator UI (``graph.read.local_operator``).
Both present the same subject to Nomos, resolved here once, so a deployment
cannot grant the operator one identity at the terminal and another in the
browser by accident.

The operator UI authorizes each read at its own boundary before touching the
graph and then filters rows under the same action (PACT
operator-ui-search-view), so its decisions are audited as the UI and a
deployment can allow or refuse the UI separately from the CLI.
"""
from __future__ import annotations

import getpass
import hashlib
import json
import os
from typing import Any, Iterable, Mapping

import psycopg

from okg.substrate.errors import SubstrateError

LOCAL_OPERATOR_READ_ACTION = "graph.read.local_operator"
LOCAL_OPERATOR_READ_SURFACE = "local_operator"
LOCAL_OPERATOR_BOUNDARY_KIND = "local_operator_boundary"

#: The only resource attributes the operator UI boundary sends to Nomos. This
#: module fixes them; none is read from the caller's request (#2115).
#: ``request_digest`` is a hash of the request parameters: it keeps one
#: decision per distinct request in the insert-once audit trail, as before,
#: without letting a request key become a policy attribute. No policy reads a
#: request parameter today (the baseline boundary rule matches deployment,
#: scope and ``resource_kind`` only), so the per-read allowlist is empty; a
#: policy that needs a request parameter must be given it here, by name.
LOCAL_OPERATOR_RESOURCE_ATTRIBUTES: tuple[str, ...] = (
    "resource_kind",
    "read",
    "request_digest",
)

#: Request keys refused outright. A request key never reaches the policy, but
#: one of these names says the caller believes it sets a policy fact, so the
#: read fails closed instead of quietly ignoring it. The set is the fixed
#: attributes above, the facts the Nomos runtime adds (``deployment``,
#: ``scope``), the facts the rule matcher builds itself (``subject``,
#: ``action``, ``classification``), the protected identity facts, and the
#: source-lineage keys the baseline policy matches.
RESERVED_OPERATOR_REQUEST_KEYS: frozenset[str] = frozenset({
    *LOCAL_OPERATOR_RESOURCE_ATTRIBUTES,
    "deployment",
    "scope",
    "subject",
    "action",
    "classification",
    "principal_kind",
    "graph_branch",
    "source",
    "source_id",
    "source_ids",
})


class OperatorReadRequestKeyReserved(SubstrateError):
    """An operator read's request named a key reserved for policy facts."""

    code = "operator_read_request_key_reserved"


class OperatorReadRequestUnencodable(SubstrateError):
    """An operator read's request carried a value that cannot be recorded as JSON."""

    code = "operator_read_request_unencodable"


#: Refusals that mean the calling route is wrong, not that authority or the
#: graph failed: retrying or republishing never fixes them, so a route
#: answers them as a server fault.
OPERATOR_READ_ROUTE_FAULTS: tuple[type[SubstrateError], ...] = (
    OperatorReadRequestKeyReserved,
    OperatorReadRequestUnencodable,
)


def _safe_getuser() -> str:
    try:
        return getpass.getuser()
    except Exception:  # noqa: BLE001 — no resolvable OS user
        return ""


def _nomos_subject(*, default_kind: str) -> str:
    raw = (
        os.getenv("OKG_NOMOS_SUBJECT")
        or os.getenv("OKG_PRINCIPAL")
        or os.getenv("USER")
        or getpass.getuser()
        or default_kind
    )
    subject = str(raw).strip()
    return subject if subject.startswith("subject:") else f"subject:{subject}"


def manifest_read_principals(deployment: str) -> list[str]:
    """The deployment manifest's declared read principals, in declared order.

    Empty when the deployment has no resolvable manifest, the manifest has no
    ``nomos.deployment_posture.read_principals``, or the block is malformed —
    every failure mode collapses to "no declared principals", which keeps the
    caller on the legacy `$USER` path (still denied under an enforce
    baseline; fail-closed).
    """
    try:
        from okg.substrate import fastyaml
        from okg.substrate.deployments import find_deployment_dir

        deployment_dir = find_deployment_dir(deployment)
        if deployment_dir is None:
            return []
        manifest_path = deployment_dir / "deployment.yaml"
        if not manifest_path.is_file():
            return []
        manifest = fastyaml.safe_load(
            manifest_path.read_text(encoding="utf-8")
        ) or {}
    except Exception:  # noqa: BLE001 — unreadable manifest = no principals
        return []
    if not isinstance(manifest, Mapping):
        return []
    nomos = manifest.get("nomos")
    if not isinstance(nomos, Mapping):
        return []
    posture = nomos.get("deployment_posture")
    if not isinstance(posture, Mapping):
        return []
    raw_principals = posture.get("read_principals")
    if isinstance(raw_principals, (str, bytes)) or not isinstance(
        raw_principals, Iterable,
    ):
        return []
    principals: list[str] = []
    for value in raw_principals:
        name = str(value).strip()
        if name and name not in principals:
            principals.append(name)
    return principals


def resolve_operator_read_subject(
    *,
    deployment: str,
    default_kind: str,
) -> tuple[str, dict[str, Any]]:
    """Resolve the subject an operator read presents to Nomos.

    Per ``fix-provision-enforce-bringup`` (req.provision.cli-read-subject;
    design in ``pact/changes/fix-provision-enforce-bringup/design.md``):

    1. An explicit ``OKG_NOMOS_SUBJECT`` / ``OKG_PRINCIPAL`` wins unchanged.
    2. Otherwise present the manifest's canonical operator read principal:
       ``subject:human_operator`` when declared among ``read_principals``,
       else the first declared read principal.
    3. No declared read principals -> legacy behavior (``$USER`` /
       ``getpass.getuser()``), which stays denied under an enforce-mode
       default-deny baseline (fail-closed; no grant is widened).

    Returns ``(subject, meta)``; ``meta`` records the resolution source and
    the OS username so the boundary decision context is auditable.
    """
    os_user = os.getenv("USER") or _safe_getuser() or ""
    explicit = (
        os.getenv("OKG_NOMOS_SUBJECT") or os.getenv("OKG_PRINCIPAL") or ""
    ).strip()
    if explicit:
        subject = (
            explicit if explicit.startswith("subject:")
            else f"subject:{explicit}"
        )
        return subject, {"source": "env_override", "os_user": os_user}
    principals = manifest_read_principals(deployment)
    if principals:
        principal = (
            "human_operator"
            if "human_operator" in principals
            else principals[0]
        )
        return (
            f"subject:{principal}",
            {
                "source": "manifest_read_principal",
                "principal": principal,
                "os_user": os_user,
            },
        )
    return (
        _nomos_subject(default_kind=default_kind),
        {"source": "os_user_fallback", "os_user": os_user},
    )


def resolve_operator_search_scope(
    conn: psycopg.Connection,
    session: Any,
    *,
    deployment: str,
    requested_profile: str | None,
    subtype_filter: str | None,
    method: str,
    requested_families: Any = None,
    requested_max_per_parent: Any = None,
    skip_absent_profile_subtypes: bool = False,
) -> Any:
    """Resolve the deployment search selection for an operator search.

    Shared by the CLI and the operator UI so both apply the same deployment
    search profile to the same query.

    `skip_absent_profile_subtypes` is for a surface that shows the operator
    what it left out (the console, issue #2545). A profile subtype the pinned
    catalog lacks is then left out and named on `absent_subtypes` -- but only
    when the deployment's own ontology modules define it, which is a catalog
    that is behind its manifest. A name the modules do not define is a broken
    profile and still raises `ProfileSubtypeAbsent`. Without the flag every
    absent subtype raises, as the CLI and MCP surfaces require.

    A deployment that declares `search:` and whose manifest cannot be resolved
    RAISES rather than running unscoped — proceeding would silently return the
    bookkeeping-heavy page the profile exists to prevent, with an envelope
    claiming no scope was requested.
    """
    from okg.substrate.deployments import (
        DeploymentResolutionError,
        resolve_deployment,
    )
    from okg.substrate.mcp.search_profiles import (
        RESERVED_ALL,
        UNSCOPED,
        ResolvedScope,
        assert_method_can_filter_family,
        assert_method_can_scope,
        resolve_for_surface,
    )

    # The reserved escape hatch is answerable without a manifest, and MUST be:
    # a broken manifest is exactly when a caller needs a way back to the
    # unscoped search.
    if (
        requested_profile == RESERVED_ALL
        and requested_families is None
        and requested_max_per_parent is None
    ):
        return ResolvedScope(profile=RESERVED_ALL, subtypes=None)
    requested_anything = (
        requested_profile is not None
        or requested_families is not None
        or requested_max_per_parent is not None
    )
    try:
        manifest = resolve_deployment(deployment)
    except DeploymentResolutionError:
        # ONLY "no such deployment" may degrade to unscoped, and only when the
        # caller named no profile: there is genuinely no `search:` block to
        # honour. Every other failure -- a malformed `search:` block
        # (DeploymentConfigError), an unresolvable ${VAR}, a permission error --
        # MUST propagate. Swallowing those would let a typo'd `default_profle:`
        # turn every search on a profiled deployment silently unscoped, which is
        # the precise defect the load-time strictness exists to catch.
        if not requested_anything:
            return UNSCOPED
        raise
    if not requested_anything and getattr(manifest, "search", None) is None:
        return UNSCOPED
    assert_method_can_scope(method, requested_profile)
    assert_method_can_filter_family(method, requested_families)
    return resolve_for_surface(
        conn,
        catalog_version_id=session.catalog_version_id,
        manifest_search_block=getattr(manifest, "search", None),
        manifest_path=getattr(manifest, "deployment_dir", None),
        requested_profile=requested_profile,
        subtype_filter=subtype_filter,
        method=method,
        requested_families=requested_families,
        requested_max_per_parent=requested_max_per_parent,
        deployment_name=getattr(manifest, "name", None) or deployment,
        absent_subtypes_allowed=(
            _defined_by_deployment_modules(manifest)
            if skip_absent_profile_subtypes
            else None
        ),
    )


def _defined_by_deployment_modules(manifest: Any) -> Any:
    """Accept an absent profile subtype only if the manifest's modules define it.

    The composed catalog is built only when a profile subtype is actually
    absent, so a deployment whose catalog matches its manifest pays nothing.
    A composition failure propagates: it is not evidence either way.
    """

    def accepted(absent: tuple[str, ...]) -> tuple[str, ...]:
        from okg.substrate.catalog.modules_compose import compose_catalog

        defined = compose_catalog(manifest).subtypes
        return tuple(name for name in absent if name in defined)

    return accepted


def nomos_denied_error(
    conn: psycopg.Connection,
    *,
    deployment: str,
    surface: str,
    outcome: Any,
) -> SubstrateError:
    decision = outcome.decision
    message = f"Nomos denied {surface}: {decision.reason_code}"
    data: dict[str, Any] = {
        "surface": surface,
        "decision_id": decision.decision_id,
        "effect": decision.effect,
        "final_effect": outcome.final_effect,
        "reason_code": decision.reason_code,
        "missing_obligations": list(outcome.missing_obligations),
    }
    fix_hint = (
        "activate an appropriate Nomos policy for this deployment or "
        "run with a subject that has graph-read permission"
    )
    # fix-nomos-read-path-ux (req.readux.deny-names-grants, #640): the deny
    # names the presented subject and the subjects the ACTIVE bundle grants
    # for this surface, and the fix_hint points at presenting a granted
    # subject when a policy IS active (the activate-a-policy hint above
    # stays only for the no-active-bundle / enrichment-failure cases).
    from okg.substrate.nomos import (
        describe_read_surface_deny,
        is_read_deny_surface,
    )

    if is_read_deny_surface(surface):
        detail = describe_read_surface_deny(
            conn,
            deployment_name=deployment,
            scope=f"deployment:{deployment}",
            surface=surface,
            presented_subject=decision.subject,
            subject_env_hint="OKG_NOMOS_SUBJECT",
        )
        data.update(detail["data"])
        if detail["fix_hint"]:
            fix_hint = detail["fix_hint"]
        if detail["summary"]:
            message = f"{message} — {detail['summary']}"
    err = SubstrateError(message, data=data, fix_hint=fix_hint)
    err.code = "nomos_runtime_denied"
    return err


def operator_read_request_params(
    *, read: str, request: Mapping[str, Any],
) -> dict[str, Any]:
    """Copy an operator read's request parameters, refusing reserved keys."""
    params = dict(request)
    reserved = sorted(key for key in params if key in RESERVED_OPERATOR_REQUEST_KEYS)
    if reserved:
        raise OperatorReadRequestKeyReserved(
            f"operator read {read!r} names reserved request key(s) {reserved}; "
            "a request parameter is never a policy attribute",
            data={"read": read, "reserved_keys": reserved},
            fix_hint=(
                "rename the request parameter (for example `source_filter` "
                "for a source filter); a policy fact the boundary needs is "
                "added to LOCAL_OPERATOR_RESOURCE_ATTRIBUTES by name"
            ),
        )
    return params


def operator_read_resource_attributes(
    *, read: str, request_params: Mapping[str, Any],
) -> dict[str, Any]:
    """The Nomos resource attributes for one operator read: fixed fields only.

    ``request_digest`` is a sha256 over the canonical JSON of the request, so
    two reads with different parameters stay two decisions in the audit trail.
    A parameter that is not JSON-serializable raises
    ``OperatorReadRequestUnencodable`` rather than being hashed under a lossy
    stand-in. The refusal names the keys and value types, never the values.
    """
    params = dict(request_params)
    try:
        canonical = json.dumps(params, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError) as exc:
        unencodable: dict[str, str] = {}
        for key in sorted(params, key=str):
            try:
                json.dumps(params[key])
            except (TypeError, ValueError):
                unencodable[str(key)] = type(params[key]).__name__
        non_string_keys = sorted(
            type(key).__name__ for key in params if not isinstance(key, str)
        )
        raise OperatorReadRequestUnencodable(
            f"operator read {read!r} carries a request that cannot be recorded "
            f"as JSON: value types {unencodable}, non-string key types "
            f"{non_string_keys}",
            data={
                "read": read,
                "unencodable": unencodable,
                "non_string_keys": non_string_keys,
            },
            fix_hint=(
                "pass request parameters as JSON values (str, int, float, "
                "bool, None, list, dict with string keys)"
            ),
        ) from exc
    return {
        "resource_kind": LOCAL_OPERATOR_BOUNDARY_KIND,
        "read": read,
        "request_digest": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
    }


def authorize_local_operator_read(
    conn: psycopg.Connection,
    *,
    deployment: str,
    session: Any,
    read: str,
    request: Mapping[str, Any],
    subject: str,
    subject_meta: Mapping[str, Any] | None = None,
) -> Any | None:
    """Authorize one operator UI read at the graph.read.local_operator boundary.

    Runs before the read touches the graph. ``read`` names the kind of read
    (``search``, ``list``, ``record``, ``types``) and ``request`` its
    parameters. The policy sees only ``LOCAL_OPERATOR_RESOURCE_ATTRIBUTES``:
    ``read`` and a digest of ``request`` are part of the decision's identity
    because they describe the access that will happen, and the request itself
    is recorded in the audit target. No request key becomes a policy
    attribute, so none can overwrite ``resource_kind`` or ``read`` (#2115).
    The pinned generation is recorded only in the audit target, so a new
    publication does not re-mint the decision.

    Returns ``None`` when the deployment does not require the Nomos runtime.
    Raises ``OperatorReadRequestKeyReserved`` when ``request`` names a
    reserved key and ``OperatorReadRequestUnencodable`` when it cannot be
    recorded as JSON, whether or not the runtime is required (both are in
    ``OPERATOR_READ_ROUTE_FAULTS``). Raises the same
    typed denial as the CLI when the policy refuses.
    """
    from okg.substrate.nomos import (
        NomosRuntimeRequest,
        deployment_requires_nomos_runtime,
        evaluate_runtime_boundary_with_active_policy,
    )

    request_params = operator_read_request_params(read=read, request=request)
    resource = operator_read_resource_attributes(
        read=read, request_params=request_params,
    )
    if not deployment_requires_nomos_runtime(deployment):
        return None
    meta = dict(subject_meta or {})
    decision_context = {
        key: value
        for key, value in (
            ("subject_source", meta.get("source")),
            ("os_user", meta.get("os_user")),
        )
        if value
    }
    outcome = evaluate_runtime_boundary_with_active_policy(
        conn,
        deployment_name=deployment,
        scope=f"deployment:{deployment}",
        request=NomosRuntimeRequest(
            surface=LOCAL_OPERATOR_READ_ACTION,
            subject=subject,
            resource=resource,
            context=decision_context,
        ),
        target={
            "kind": LOCAL_OPERATOR_READ_SURFACE,
            "read": read,
            "generation_id": getattr(session, "generation_id", None),
            "branch_name": getattr(session, "branch_name", "default"),
            "catalog_version_id": getattr(session, "catalog_version_id", None),
            "request": request_params,
        },
        performed_obligations=("audit",),
    )
    if outcome is None:
        return None
    conn.commit()
    if not outcome.allowed:
        err = nomos_denied_error(
            conn,
            deployment=deployment,
            surface=LOCAL_OPERATOR_READ_ACTION,
            outcome=outcome,
        )
        if not active_policy_has_local_operator_boundary_rule(conn, deployment=deployment):
            # The generic hint names subjects the policy grants for record reads,
            # which reads as "present one of these" when the subject already is
            # one. The real gap is that no rule addresses this boundary at all.
            err.message = (
                f"Nomos denied {LOCAL_OPERATOR_READ_ACTION}: "
                f"{outcome.decision.reason_code} — the active policy has no rule for "
                f"operator UI reads ({LOCAL_OPERATOR_READ_ACTION} at "
                f"{LOCAL_OPERATOR_BOUNDARY_KIND})"
            )
            err.fix_hint = (
                f"a baseline policy activated before this rule existed converges with "
                f"`okg deployment apply {deployment}` (the baseline grants record reads "
                f"only for registered sources); a custom policy needs an explicit "
                f"allow rule for {LOCAL_OPERATOR_READ_ACTION} with resource_kind "
                f"{LOCAL_OPERATOR_BOUNDARY_KIND}"
            )
            err.data = {**(err.data or {}), "boundary_rule_present": False}
        raise err
    return outcome


def active_policy_has_local_operator_boundary_rule(
    conn: psycopg.Connection,
    *,
    deployment: str,
) -> bool:
    """True when the active policy has any rule addressing the UI read boundary."""
    from okg.substrate.nomos import load_active_policy_bundle

    bundle = load_active_policy_bundle(
        conn, deployment_name=deployment, scope=f"deployment:{deployment}",
    )
    if bundle is None:
        return False
    return any(
        LOCAL_OPERATOR_READ_ACTION in rule.actions
        and rule.match.get("resource_kind") == LOCAL_OPERATOR_BOUNDARY_KIND
        for rule in bundle.rules
    )
