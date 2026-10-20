"""Read-path discoverability helpers over the ACTIVE Nomos bundle.

Per ``fix-nomos-read-path-ux`` (GitHub #640): with a healthy seeded
baseline the read path was undiscoverable — deny errors never named the
presented subject or the subjects the active bundle grants, and the MCP
server defaulted to ``subject:$USER``, which no rollout baseline grants.

Everything here is READ-ONLY over the active policy state: enumerate
which subjects the active bundle's allow rules name for a read surface,
recognize the deployment rollout baseline, and build the enrichment a
read-surface deny error carries. No policy mutation, no grant widening —
a caller that presents an ungranted subject is still denied exactly as
before; it just learns why and what the granted subjects are.
"""
from __future__ import annotations

from typing import Any, Iterable, Mapping

from okg.substrate.nomos.core import (
    CompiledPolicyBundle,
    NomosPolicyError,
    compile_policy_graph,
)
from okg.substrate.nomos.runtime_enforcement import ALLOW_EFFECTS
from okg.substrate.nomos.store import (
    deployment_baseline_policy_graph,
    load_active_policy_bundle,
)

MCP_CLIENT_SUBJECT = "subject:mcp_client"

#: Surfaces whose deny errors carry grant enumeration (graph reads + the
#: MCP tool-call boundary). Kept explicit so write/admin surfaces never
#: grow a "who could do this" hint from this module.
READ_DENY_SURFACE_PREFIXES = ("graph.read",)
READ_DENY_SURFACES = ("mcp.tool.call",)


def deployment_baseline_bundle_id(deployment_name: str) -> str:
    """The bundle id the rollout baseline seed mints for a deployment.

    Must stay in sync with
    ``store.deployment_baseline_policy_graph`` (``nomos:rollout:baseline:<name>``);
    ``tests/substrate/cli/test_provision_enforce_bringup.py`` pins the format.
    """
    return f"nomos:rollout:baseline:{deployment_name}"


def is_read_deny_surface(surface: str) -> bool:
    """True when a denied surface is a read surface this module enriches."""
    return surface in READ_DENY_SURFACES or any(
        surface == prefix or surface.startswith(prefix + ".")
        for prefix in READ_DENY_SURFACE_PREFIXES
    )


def granted_subjects_for_surface(
    bundle: CompiledPolicyBundle | None,
    surface: str,
) -> tuple[str, ...]:
    """Subjects the bundle's allow rules name for ``surface``.

    A read-only scan over the compiled rules, matching on the rule's
    action list only (the denied surface/action). Resource ``match``
    facts are deliberately NOT evaluated: the output is a
    discoverability hint naming who the policy grants this surface to,
    not an authorization decision — the evaluator remains the only
    authority. ``*`` subjects are surfaced verbatim.
    """
    if bundle is None:
        return ()
    subjects: list[str] = []
    for rule in bundle.rules:
        if rule.effect not in ALLOW_EFFECTS:
            continue
        if "*" not in rule.actions and surface not in rule.actions:
            continue
        for subject in rule.subjects:
            if subject not in subjects:
                subjects.append(subject)
    return tuple(sorted(subjects))


def active_bundle_is_deployment_baseline(
    bundle: CompiledPolicyBundle | None,
    deployment_name: str,
) -> bool:
    """True when ``bundle`` is the recognized deployment rollout baseline."""
    return (
        bundle is not None
        and bundle.policy_bundle_id == deployment_baseline_bundle_id(deployment_name)
    )


def baseline_default_mcp_subject(
    conn: Any,
    *,
    deployment_name: str,
    scope: str,
) -> str | None:
    """``subject:mcp_client`` when the baseline makes it the working default.

    Returns the subject an out-of-the-box ``okg mcp-serve`` should present
    when no explicit subject env is set: exactly when the ACTIVE bundle is
    the recognized deployment rollout baseline AND that baseline grants
    ``subject:mcp_client`` the ``mcp.tool.call`` boundary. No active
    bundle, a non-baseline bundle, or a baseline without the grant return
    ``None`` so callers keep the login-name default (which stays denied
    under a default-deny baseline -- no grant is widened by defaulting
    differently).

    A failure to LOAD the active bundle is raised, not answered with
    ``None`` (issue #2343): ``None`` would switch the subject to the login
    name because the policy could not be read, so every MCP caller surfaces
    the error as an authority-evaluation failure instead.
    """
    bundle = load_active_policy_bundle(
        conn,
        deployment_name=deployment_name,
        scope=scope,
    )
    if not active_bundle_is_deployment_baseline(bundle, deployment_name):
        return None
    if MCP_CLIENT_SUBJECT not in granted_subjects_for_surface(
        bundle, "mcp.tool.call",
    ):
        return None
    return MCP_CLIENT_SUBJECT


def describe_read_surface_deny(
    conn: Any,
    *,
    deployment_name: str,
    scope: str,
    surface: str,
    presented_subject: str,
    subject_env_hint: str = "OKG_NOMOS_SUBJECT",
) -> dict[str, Any]:
    """Enrichment for a ``nomos.default_deny`` error on a read surface.

    Returns ``{"data": {...}, "fix_hint": str | None, "summary": str | None}``:

    - ``data`` always carries ``presented_subject``; with a readable active
      bundle it adds ``active_policy_bundle_id`` and ``granted_subjects``
      (the subjects the active bundle grants for ``surface``).
    - ``fix_hint`` points at presenting a granted subject when a policy IS
      active, and at activating a policy only when none is (``None`` when
      enrichment failed — the caller keeps its legacy hint).
    - ``summary`` is the one line that unblocks an operator: "you presented
      X; the active policy grants <surface> to: A, B".

    Never raises: the deny already happened; enrichment failure degrades to
    the un-enriched error rather than masking the deny with a new exception.
    """
    data: dict[str, Any] = {"presented_subject": presented_subject}
    try:
        bundle = load_active_policy_bundle(
            conn,
            deployment_name=deployment_name,
            scope=scope,
        )
    except Exception as exc:  # noqa: BLE001 — degrade to the un-enriched error
        data["grant_enumeration_error"] = type(exc).__name__
        return {"data": data, "fix_hint": None, "summary": None}

    if bundle is None:
        data["active_policy_bundle_id"] = None
        data["granted_subjects"] = []
        return {
            "data": data,
            "fix_hint": (
                "no Nomos policy bundle is active for this deployment: "
                "activate one (okg deployment up seeds the rollout "
                "baseline), then run with a subject it grants"
            ),
            "summary": (
                f"you presented {presented_subject}; no policy bundle is "
                f"active for {deployment_name}, so everything is denied"
            ),
        }

    granted = granted_subjects_for_surface(bundle, surface)
    data["active_policy_bundle_id"] = bundle.policy_bundle_id
    data["granted_subjects"] = list(granted)
    if not granted:
        return {
            "data": data,
            "fix_hint": (
                f"the active policy ({bundle.policy_bundle_id}) has no "
                f"allow rule for {surface}; a policy IS active, so "
                "activating another one is not the first fix — extend the "
                "policy or use a read path it does grant"
            ),
            "summary": (
                f"you presented {presented_subject}; the active policy "
                f"grants {surface} to no subject"
            ),
        }
    granted_text = ", ".join(granted)
    return {
        "data": data,
        "fix_hint": (
            f"present a granted subject: set {subject_env_hint} to one of "
            f"{granted_text} (a policy IS active; activating another "
            "policy is not the fix)"
        ),
        "summary": (
            f"you presented {presented_subject}; the active policy grants "
            f"{surface} to: {granted_text}"
        ),
    }


def manifest_read_principal_subjects(
    manifest: Mapping[str, Any] | None,
) -> tuple[str, ...]:
    """``subject:<principal>`` for each declared manifest read principal.

    Tolerant of missing/malformed ``nomos.deployment_posture.read_principals``
    (returns ``()``), unlike the seed path which requires the posture —
    readiness/bring-up surfaces call this on arbitrary manifests.
    """
    if not isinstance(manifest, Mapping):
        return ()
    nomos = manifest.get("nomos")
    if not isinstance(nomos, Mapping):
        return ()
    posture = nomos.get("deployment_posture")
    if not isinstance(posture, Mapping):
        return ()
    raw = posture.get("read_principals")
    if isinstance(raw, (str, bytes)) or not isinstance(raw, Iterable):
        return ()
    principals: list[str] = []
    for value in raw:
        name = str(value).strip()
        if name and name not in principals:
            principals.append(name)
    return tuple(f"subject:{principal}" for principal in principals)


def _manifest_baseline_bundle(
    manifest: Mapping[str, Any],
) -> CompiledPolicyBundle:
    """The rollout baseline ``manifest`` seeds, compiled.

    Built by the same function the seed uses
    (``store.deployment_baseline_policy_graph``), so a reader of the result
    sees exactly the grants the deployment will run under. The deployment
    name only fills each rule's ``match``, which
    :func:`granted_subjects_for_surface` does not read.
    """
    deployment_name = str(manifest.get("name") or "").strip() or "manifest"
    return compile_policy_graph(
        deployment_baseline_policy_graph(
            manifest,
            deployment_name=deployment_name,
            scope=f"deployment:{deployment_name}",
            activated_by="mcp_subject_hint",
        )
    )


def _manifest_declares_posture(manifest: Mapping[str, Any] | None) -> bool:
    if not isinstance(manifest, Mapping):
        return False
    nomos = manifest.get("nomos")
    return isinstance(nomos, Mapping) and isinstance(
        nomos.get("deployment_posture"), Mapping,
    )


def mcp_subject_hint_for_manifest(manifest: Mapping[str, Any] | None) -> str:
    """One-line MCP subject contract for the ``nomos_baseline`` phase output.

    Emitted by ``okg deployment up`` (``_phase_nomos_baseline``) and
    ``okg provision`` so non-default setups are discoverable at bring-up
    time instead of at the first denied ``mcp.tool.call`` or ``record``.

    Reads and ``record`` present ONE subject (issues #2157, #2343), so the
    hint states it once and then says whether this baseline lets that
    subject write. Both answers are read off the baseline policy the manifest
    compiles to (``mcp.tool.call`` and ``mcp.record`` grants), not restated
    from the manifest, so the hint cannot drift from the grant it describes.
    """
    if not _manifest_declares_posture(manifest):
        return (
            "this manifest declares no nomos.deployment_posture, so no "
            "rollout baseline grants subject:mcp_client; MCP reads and "
            "record will be denied under enforcement unless "
            "OKG_MCP_NOMOS_SUBJECT names a granted subject"
        )
    assert manifest is not None
    try:
        bundle = _manifest_baseline_bundle(manifest)
    except NomosPolicyError as exc:
        return (
            f"this manifest's Nomos rollout baseline does not compile ({exc}); "
            "MCP reads and record will be denied until it does"
        )
    if MCP_CLIENT_SUBJECT not in granted_subjects_for_surface(
        bundle, "mcp.tool.call",
    ):
        return (
            "this baseline grants no subject:mcp_client (mcp_client is not in "
            "nomos.deployment_posture.read_principals); MCP reads and record "
            "will be denied unless OKG_MCP_NOMOS_SUBJECT names a granted "
            "subject"
        )
    reads = (
        "MCP servers present subject:mcp_client by default under this "
        "baseline, for reads and record alike; set OKG_MCP_NOMOS_SUBJECT (or "
        "OKG_NOMOS_SUBJECT) only to present a different subject"
    )
    if MCP_CLIENT_SUBJECT in granted_subjects_for_surface(bundle, "mcp.record"):
        return (
            f"{reads}. record is admitted for review as subject:mcp_client "
            "once the server runs with `okg mcp-serve --allow-writes`"
        )
    return (
        f"{reads}. record is denied under this baseline: the baseline grants "
        "mcp.record only to nomos.deployment_posture.write_principals when "
        "mcp_write_default is review, and mcp_client is not granted it here "
        "-- add mcp_client to write_principals and set mcp_write_default: "
        "review, or set OKG_MCP_NOMOS_SUBJECT to a granted write principal; "
        "record also needs `okg mcp-serve --allow-writes`"
    )
