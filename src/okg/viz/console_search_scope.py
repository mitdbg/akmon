"""Search-profile scope for the console's record search (issue #2545).

Kept out of ``record_endpoints`` so the route file stays a thin router: this is
the one place that decides how a deployment search-profile refusal reaches the
console page.
"""
from __future__ import annotations

import logging
from typing import Any, Callable

from fastapi import HTTPException

logger = logging.getLogger(__name__)


def resolve_console_search_scope(
    resolve: Callable[..., Any],
    conn: Any,
    session: Any,
    *,
    deployment: str,
    subtype_filter: str | None,
    version: str,
) -> Any:
    """Resolve the deployment's search profile for a console search.

    Profile types this version's catalog does not define, but the
    deployment's own modules do, are left out and named on the page
    (`skipped_types`) instead of failing every search (issue #2545). A
    search-profile refusal that remains -- a profile naming a type no module
    defines, a malformed `search:` block -- is a deployment configuration
    fault the operator has to fix, so it reaches the page as a typed reason
    with the refusal's own code, names and fix, not as the opaque 500.
    Those refusals are built from profile, family and catalog names -- except
    the ones raised while the `search:` block loads (a malformed block, an
    undeclared `default_profile`, a profile naming an undeclared source
    family, a malformed or overlapping `concept_groups` entry), whose message
    ends in the manifest's filesystem path; those are
    logged and served as a fixed sentence, like every fault this file keeps
    off the page. The console names no profile or family of its own, so it
    meets `UnknownSearchProfile` and `UnknownSourceFamily` only from that
    load. Any other failure still takes the opaque path.
    """
    from okg.substrate.errors import SubstrateError
    from okg.substrate.mcp import concept_expansion as concepts
    from okg.substrate.mcp import search_profiles as profiles

    refusals = (
        profiles.ProfileSubtypeAbsent,
        profiles.SearchProfileConfigInvalid,
        profiles.UnknownSearchProfile,
        profiles.SubtypeOutsideProfile,
        profiles.SubtypeClosureIncomplete,
        profiles.SourceFamilyConfigInvalid,
        profiles.UnknownSourceFamily,
        profiles.SourceFamilySourceAbsent,
        profiles.SourceFamilyEmptyIntersection,
        profiles.PathGlobInvalid,
        profiles.DiversityBoundInvalid,
        profiles.ProfileNotApplicable,
        profiles.SourceFamilyNotApplicable,
        concepts.ConceptGroupConfigInvalid,
        concepts.ConceptTermDuplicated,
        concepts.ConceptCanonicalShadowed,
    )
    # Every class `parse_search_config` raises with "(Manifest: <path>)" at
    # the end of its text. The other two it can raise while loading,
    # `DiversityBoundInvalid` (a profile's `max_per_parent`) and
    # `PathGlobInvalid` (an `exclude_path_globs` pattern), carry no path, so
    # they keep their own message.
    config_faults = (
        profiles.SearchProfileConfigInvalid,
        profiles.SourceFamilyConfigInvalid,
        profiles.UnknownSearchProfile,
        profiles.UnknownSourceFamily,
        concepts.ConceptGroupConfigInvalid,
        concepts.ConceptTermDuplicated,
        concepts.ConceptCanonicalShadowed,
    )
    try:
        return resolve(
            conn,
            session,
            deployment=deployment,
            requested_profile=None,
            subtype_filter=subtype_filter,
            method="lexical",
            skip_absent_profile_subtypes=True,
        )
    except refusals as exc:
        assert isinstance(exc, SubstrateError)
        config_fault = isinstance(exc, config_faults)
        if config_fault:
            logger.warning("console search: deployment search block refused: %s", exc)
        raise HTTPException(
            status_code=503,
            detail={
                "reason": (
                    exc.code if exc.code.startswith("search_")
                    else f"search_{exc.code}"
                ),
                "version": version,
                "error": (
                    "The deployment's `search:` block did not load; the cause "
                    "is in the server log."
                    if config_fault else exc.message
                ),
                "error_type": type(exc).__name__,
                "data": None if config_fault else exc.data,
                "next_check": exc.fix_hint,
            },
        ) from exc
