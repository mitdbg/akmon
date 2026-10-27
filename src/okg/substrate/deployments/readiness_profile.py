"""The readiness profile a deployment is judged by (issue #3958).

A deployment names how strictly `okg dogfood status` should judge it with
``readiness.profile``. This module holds the closed set of names, and the one
resolver every reader uses (`okg dogfood status`, `okg dogfood doctor`, the
chat health banner path, the console readiness read and the environment
readiness gate), so they cannot disagree about which profile applies.

It imports nothing from okg at module level: the deployments package imports
it while that package is still initializing, like ``keys.py``.
"""

from __future__ import annotations

from typing import Any, Mapping

#: The names a deployment may declare. ``general`` is deliberately absent: it
#: is the ``deployment up`` / ``deployment ready`` smoke level, not a status
#: profile.
READINESS_PROFILES: tuple[str, ...] = ("dogfood", "ci", "standard")

#: The one ``readiness`` key a ``definition.yaml`` may carry. Everything else in
#: the composed block is owned by the environment descriptor.
DEFINITION_READINESS_KEYS: tuple[str, ...] = ("profile",)

#: ``readiness`` keys that only environment composition may write. An authored
#: ``deployment.yaml`` or ``definition.yaml`` that sets ``environment_policy``
#: is pretending to be an environment.
COMPOSED_ONLY_READINESS_KEYS: tuple[str, ...] = ("environment_policy",)

#: The profile a deployment that declares nothing is judged by.
DEFAULT_READINESS_PROFILE = "standard"

#: Environment policies that choose a profile. Any other policy (for example
#: ``org-private``) names how the environment is promoted, not how status
#: judges the graph, so it neither chooses a profile nor conflicts with one.
CHOOSING_ENVIRONMENT_POLICIES: tuple[str, ...] = READINESS_PROFILES

#: Where the profile comes from, in the resolver's ``source`` field.
SOURCE_DECLARED = "declared"
SOURCE_ENVIRONMENT = "environment"
SOURCE_DEFAULT = "default"

#: Problem codes. The first two are also the lint codes for the same
#: mistake, so one name follows the problem from lint to status.
PROBLEM_INVALID = "deployment.readiness.profile_invalid"
PROBLEM_AUTHORED_POLICY = "deployment.readiness.environment_policy_authored"
PROBLEM_CONFLICT = "deployment.readiness.profile_conflict"

#: Age limits for required source watermarks, per profile, in seconds. A CLI
#: flag overrides them.
MAX_SOURCE_AGE_SECONDS: dict[str, int] = {
    "dogfood": 60 * 60,
    "ci": 6 * 60 * 60,
    "standard": 14 * 24 * 60 * 60,
}


def readiness_profile_problem(value: Any) -> str | None:
    """Why ``value`` is not a declarable readiness profile, or ``None`` if it is."""
    if isinstance(value, str) and value in READINESS_PROFILES:
        return None
    allowed = ", ".join(READINESS_PROFILES)
    if value == "general":
        return (
            "readiness.profile 'general' is not a status profile; use "
            "'standard'. 'general' is the `deployment up` / `deployment "
            f"ready` smoke level. Allowed: {allowed}"
        )
    return f"readiness.profile {value!r} is not one of: {allowed}"


def declared_readiness_profile(
    manifest: Mapping[str, Any] | None,
    *,
    composed: bool,
) -> dict[str, Any]:
    """Which readiness profile judges this manifest, and why.

    ``composed`` says the manifest came from environment composition (the
    sealed path). Only then is ``readiness.environment_policy`` read; in an
    authored manifest that key is a problem.

    Returns ``{name, source, manifest_value, environment_policy, problem}``.
    With a problem, ``name`` and ``source`` are ``None`` and ``problem`` is
    ``{code, message, field}``: the caller runs no profile's checks.

    Precedence:

    * ``readiness.profile`` invalid (including ``general``): problem.
    * ``environment_policy`` in an authored manifest: problem.
    * both set and both in the closed set, but different: problem.
    * ``readiness.profile`` set: it, ``declared``.
    * a composed ``environment_policy`` in the closed set: it, ``environment``.
    * otherwise ``standard``, ``default``.
    """
    readiness = manifest.get("readiness") if isinstance(manifest, Mapping) else None
    block: Mapping[str, Any] = readiness if isinstance(readiness, Mapping) else {}
    has_profile = "profile" in block
    profile = block.get("profile")
    has_policy = COMPOSED_ONLY_READINESS_KEYS[0] in block
    policy = block.get(COMPOSED_ONLY_READINESS_KEYS[0])
    result: dict[str, Any] = {
        "name": None,
        "source": None,
        "manifest_value": profile if has_profile else None,
        "environment_policy": policy if has_policy else None,
        "problem": None,
    }
    if has_profile:
        invalid = readiness_profile_problem(profile)
        if invalid is not None:
            result["problem"] = {
                "code": PROBLEM_INVALID,
                "message": invalid,
                "field": "readiness.profile",
            }
            return result
    if has_policy and not composed:
        result["problem"] = {
            "code": PROBLEM_AUTHORED_POLICY,
            "message": (
                "readiness.environment_policy is written by environment "
                f"composition and must not be authored (found {policy!r})"
            ),
            "field": "readiness.environment_policy",
        }
        return result
    policy_chooses = (
        composed
        and isinstance(policy, str)
        and policy in CHOOSING_ENVIRONMENT_POLICIES
    )
    if has_profile:
        if policy_chooses and policy != profile:
            result["problem"] = {
                "code": PROBLEM_CONFLICT,
                "message": (
                    f"readiness.profile is {profile!r} but the environment's "
                    f"readiness.policy is {policy!r}; they must agree"
                ),
                "field": "readiness.profile",
            }
            return result
        result["name"] = profile
        result["source"] = SOURCE_DECLARED
        return result
    if policy_chooses:
        result["name"] = policy
        result["source"] = SOURCE_ENVIRONMENT
        return result
    result["name"] = DEFAULT_READINESS_PROFILE
    result["source"] = SOURCE_DEFAULT
    return result


def dogfood_signals(manifest: Mapping[str, Any] | None) -> list[str]:
    """The manifest fields that suggest this deployment is a dogfood one.

    Used only to warn when such a deployment declares no profile and is
    therefore judged as ``standard``. A signal never chooses the profile.
    """
    if not isinstance(manifest, Mapping):
        return []
    signals: list[str] = []
    nomos = manifest.get("nomos")
    posture = nomos.get("deployment_posture") if isinstance(nomos, Mapping) else None
    if isinstance(posture, Mapping):
        if posture.get("deployment_class") == "dogfood_workspace":
            signals.append("nomos.deployment_posture.deployment_class")
        profiles = posture.get("readiness_profiles")
        if isinstance(profiles, (list, tuple)) and "dogfood" in profiles:
            signals.append("nomos.deployment_posture.readiness_profiles")
    sync_root = manifest.get("sync_root")
    if isinstance(sync_root, str) and sync_root.strip():
        signals.append("sync_root")
    return signals
