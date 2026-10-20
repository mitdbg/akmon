"""Deployment-declared search profiles: a named subtype scope for `search`.

Per `analyst-search-profiles` (issue #797). A deployment that composes
work-tracking modules (pact/openspec/sessions) alongside content ranks
bookkeeping nodes against content nodes for the same fixed relevance budget
(`_LEXICAL_RANK_CAP`), so the row that answers a question-shaped query can be
outranked — or evicted outright — by rows the analyst never wanted. A profile
names the subtypes that count as content for that deployment, so the budget is
spent on them.

Two hard rules run through this module.

**Nothing is absorbed.** An undeclared profile name, a profile naming a subtype
the pinned catalog lacks, an incomplete refinement closure, or a subtype filter
outside the profile all RAISE. None of them may degrade into an unscoped or
empty page: an empty page is indistinguishable from "your query matched
nothing", which is exactly how a scoping bug hides.

**Resolution happens once, at the operator surface.** `search()` holds only a
connection and a session; it has no manifest. So the CLI and the MCP tool
resolve the profile name to a `ResolvedScope` and pass THAT in. `search()`
rejects an unresolved profile name rather than reaching for a manifest itself.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Iterable, Mapping, Optional

import psycopg

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.concept_expansion import ConceptGroup, parse_concept_groups


class SearchProfileConfigInvalid(SubstrateError):
    """A deployment's `search:` block is malformed.

    Raised at manifest load. Strict rather than tolerant, because a
    silently-ignored `default_profle:` leaves a deployment believing it scoped
    its search when it did not."""

    code = "search_profile_config_invalid"


class UnknownSearchProfile(SubstrateError):
    """A caller named a profile the deployment does not declare.

    Never degrades to an unscoped search: a typo'd profile silently returning
    the unscoped page is exactly the bookkeeping-noise result issue #797 is
    about."""

    code = "unknown_search_profile"


class ProfileSubtypeAbsent(SubstrateError):
    """A declared profile names a subtype the pinned catalog lacks.

    Fails loud instead of contributing nothing, because a typo'd subtype would
    otherwise turn into a quietly narrower scope."""

    code = "profile_subtype_absent"


class SubtypeOutsideProfile(SubstrateError):
    """A `subtype` filter falls outside the resolved profile scope.

    Raises rather than returning the empty intersection: an empty page cannot
    be told apart from "your query matched nothing"."""

    code = "subtype_outside_profile"


class SubtypeClosureIncomplete(SubstrateError):
    """The pinned catalog's refinement closure is short of its own declarations.

    Expanding a profile against it would silently omit refinements while
    reporting a confident scope."""

    code = "subtype_closure_incomplete"


class ProfileNotApplicable(SubstrateError):
    """A profile was named on a method that cannot honour a subtype scope."""

    code = "search_profile_not_applicable"


class SourceFamilyConfigInvalid(SubstrateError):
    """A deployment's `search.source_families:` block is malformed.

    Same strictness, and the same reason, as `SearchProfileConfigInvalid`: a
    silently-ignored family declaration leaves a deployment believing its
    authority profiles select something they do not."""

    code = "source_family_config_invalid"


class UnknownSourceFamily(SubstrateError):
    """A caller (or a profile) named a family the deployment does not declare.

    Never degrades to an unfiltered search. A typo'd family that quietly
    returned the whole corpus is the mixed-authority page issue #1444 is
    about."""

    code = "unknown_source_family"


class SourceFamilyEmptyIntersection(SubstrateError):
    """A request-level family falls outside the profile's declared families.

    Raises rather than returning the empty intersection, for the reason
    `SubtypeOutsideProfile` gives: an empty page cannot be told apart from
    "your query matched nothing"."""

    code = "source_family_empty_intersection"


class SourceFamilySourceAbsent(SubstrateError):
    """An applied family names a producer id the deployment never registered.

    Only raised where an authority to check against EXISTS — see
    `validate_family_sources`. Contributing nothing would turn a typo'd
    producer id into a quietly narrower (often empty) page."""

    code = "source_family_source_absent"


class SourceFamilyNotApplicable(SubstrateError):
    """A family filter was named on a method that cannot carry the predicate."""

    code = "source_family_not_applicable"


class PathGlobInvalid(SubstrateError):
    """An `exclude_path_globs` entry uses syntax this translation does not
    implement.

    Accepting it and matching it literally would exclude nothing while the
    envelope reported an exclusion."""

    code = "exclude_path_glob_invalid"


class DiversityBoundInvalid(SubstrateError):
    """`max_per_parent` is not an integer >= 1.

    Zero would mean "return no row that has a parent", which is a page nobody
    asks for and which is indistinguishable from an empty match."""

    code = "invalid_max_per_parent"


#: Always resolves to the unscoped search, and may not be declared. A
#: deployment that sets `default_profile` must never be able to trap a caller
#: inside it, so this escape hatch is reserved rather than deployment-defined.
RESERVED_ALL = "all"

#: Strict key sets. The top-level manifest census
#: (`tests/substrate/test_no_unconsumed_manifest_fields.py`) matches on the
#: literal string `search`, which occurs throughout `src/okg/`, so it cannot
#: detect a misspelled key NESTED under `search:`. These sets do that instead.
_SEARCH_BLOCK_KEYS = frozenset({
    "profiles", "default_profile", "source_families", "concept_groups",
})
_PROFILE_KEYS = frozenset({
    "subtypes",
    "description",
    "source_families",
    "exclude_path_globs",
    "max_per_parent",
})

#: Methods that return node rows and can therefore honour a subtype scope.
PROFILE_APPLICABLE_METHODS = ("lexical", "semantic", "alias", "attrs", "grep")

#: Methods on which a source-family filter has a defined, ENFORCED meaning.
#:
#: `attrs` is deliberately absent, and that is a MEASUREMENT, not an oversight:
#: the typed attrs lookup is served by the effective-attribute relation
#: (`_effective_view_sql_page`), which pages in Postgres and only hydrates the
#: producer id afterwards (`hydrate_node_source_rows_at_pin`), so the predicate
#: cannot be re-checked against a page that is already the final page.
#: `identifier_mentions` returns no node rows at all.
#:
#: Membership here does NOT mean the predicate reaches the ranked SQL on every
#: method — see `FAMILY_APPLICATION_BY_METHOD`, which is what the envelope
#: reports. Conflating the two is what made an earlier version of this comment
#: wrong about `alias`.
FAMILY_APPLICABLE_METHODS = ("lexical", "semantic", "alias", "grep")

#: Methods on which a per-parent diversity bound has a defined meaning. Only
#: `lexical` with `rank_by='self'` produces the ranked candidate set the
#: declared passage relation is followed over.
DIVERSITY_APPLICABLE_METHODS = ("lexical",)

#: Methods whose query is a bag of words a concept group can expand. `semantic`
#: embeds the query -- appending synonyms would MOVE the vector, not add
#: alternatives -- and `alias` resolves one exact string.
CONCEPT_EXPANSION_APPLICABLE_METHODS = ("lexical",)

#: WHERE a selection predicate actually runs, reported per control per method.
#: These two are NOT interchangeable and the difference is caller-visible:
#:
#:   ranked_sql          The predicate is inside the query that ranks and
#:                       limits, so the relevance budget is spent on rows the
#:                       caller asked for and the page fills.
#:   post_fetch_residual The predicate runs in Python over the fetched window,
#:                       exactly as `attr_filters` already does on those paths.
#:                       The rows returned are correct, but the page may
#:                       UNDER-FILL relative to `limit`, and `truncated` /
#:                       `next_offset` remain the authority on whether more
#:                       exist. Disclosed rather than hidden, because a short
#:                       page a caller reads as "that is all there is" is the
#:                       failure this whole contract exists to prevent.
APPLICATION_RANKED_SQL = "ranked_sql"
APPLICATION_RESIDUAL = "post_fetch_residual"

#: The family predicate reaches the ranked SQL on `lexical` (the segment scan
#: projects `source`) and on `semantic` (the lowered IR program binds
#: `scoped_source_ids`). On `alias` it CANNOT: `okg.aliases` has no `source`
#: column, so the filter necessarily runs over the hydrated canonicals after
#: the alias-row LIMIT.
FAMILY_APPLICATION_BY_METHOD = {
    "lexical": APPLICATION_RANKED_SQL,
    "semantic": APPLICATION_RANKED_SQL,
    "alias": APPLICATION_RESIDUAL,
    "grep": APPLICATION_RANKED_SQL,
}

#: Path exclusions reach the ranked SQL only on `lexical`, where the 200-row
#: rank cap makes it load-bearing. The semantic path's ranking is lowered
#: through a TransformProgram whose `like` operator inlines its pattern into the
#: program body, so a per-glob program variant would be minted per deployment
#: pattern; the exclusion runs as a residual there instead, under the same
#: under-fill contract `attr_filters` already carries on that path.
PATH_EXCLUSION_APPLICATION_BY_METHOD = {
    "lexical": APPLICATION_RANKED_SQL,
    "semantic": APPLICATION_RESIDUAL,
    "alias": APPLICATION_RESIDUAL,
    "grep": APPLICATION_RANKED_SQL,
}


@dataclass(frozen=True)
class SourceFamily:
    """One declared family: a non-empty list of producer ids that count as it."""

    name: str
    sources: tuple[str, ...]


@dataclass(frozen=True)
class SearchProfile:
    """One declared profile.

    `subtypes` is the subtype scope (`analyst-search-profiles`). The three
    authority keys are optional and OFF unless declared:

      `source_families`     — the declared families this profile selects.
      `exclude_path_globs`  — path patterns whose rows this profile excludes.
      `max_per_parent`      — the profile's default per-parent diversity bound.
    """

    name: str
    subtypes: tuple[str, ...]
    description: str | None = None
    source_families: tuple[str, ...] = ()
    exclude_path_globs: tuple[str, ...] = ()
    max_per_parent: int | None = None


@dataclass(frozen=True)
class SearchConfig:
    """A deployment's `search:` block, already validated."""

    profiles: Mapping[str, SearchProfile]
    default_profile: str | None = None
    source_families: Mapping[str, SourceFamily] = field(default_factory=dict)
    #: Lexical query-expansion vocabulary (#1218). Empty means the deployment
    #: declares none and expansion is OFF -- there is never a default set.
    concept_groups: tuple[ConceptGroup, ...] = ()

    @property
    def declared_names(self) -> tuple[str, ...]:
        return tuple(sorted(self.profiles))

    @property
    def declared_family_names(self) -> tuple[str, ...]:
        return tuple(sorted(self.source_families or {}))

    def family_by_source(self) -> tuple[tuple[str, str], ...]:
        """(producer_id, family_name) pairs, sorted, for row labelling.

        A producer id declared in two families would make the label ambiguous,
        which `parse_search_config` refuses at load, so this mapping is total
        and single-valued by construction.
        """
        return tuple(sorted(
            (source, family.name)
            for family in (self.source_families or {}).values()
            for source in family.sources
        ))


@dataclass(frozen=True)
class ResolvedScope:
    """The selection a search actually ran under.

    Deliberately ONE object rather than three parallel ones. Every control here
    is pushed into the same ranked queries at the same three sites, and every
    one of them is a way a page can be narrower than the caller believes; three
    objects would be three places a control can be dropped on the way in, and
    the envelope would have no single thing to disclose.

    `subtypes is None` means no subtype scope. `source_ids is None` means no
    family filter. `profile` is the APPLIED profile name — which may be the
    deployment default the caller never typed, or the reserved `all`.

    `declared_family_pairs is None` means the deployment declares no families at
    all, which is a different fact from "it declares families and this row's
    producer is in none of them"; the envelope reports the first once, and rows
    carry an explicit null in both cases.
    """

    profile: str | None = None
    subtypes: tuple[str, ...] | None = None
    #: Applied family NAMES, or None when no family filter applies.
    source_families: tuple[str, ...] | None = None
    #: The producer ids those families resolve to. None iff `source_families`
    #: is None. Empty is impossible: a family declaring no sources is refused
    #: at load, and an empty intersection raises.
    source_ids: tuple[str, ...] | None = None
    #: Path patterns excluded from the ranked query. Empty means none.
    excluded_path_globs: tuple[str, ...] = ()
    #: The per-parent diversity bound in effect, or None for off.
    max_per_parent: int | None = None
    #: Where `max_per_parent` came from: `"argument"` (the caller passed it),
    #: `"profile"` (a profile default), or None (no bound, or a scope built in
    #: process, which counts as explicit). Under `rank_by="records"` a profile
    #: default is not applied and disclosed; an argument is refused there
    #: (PACT search-result-shape, DESIGN.md D1 "Diversity").
    max_per_parent_origin: str | None = None
    #: How the applied families' producer ids were checked. See
    #: `validate_family_sources`. None when no family filter applies.
    family_validation: str | None = None
    #: Every declared (producer_id, family) pair, for labelling result rows.
    #: None when the deployment declares no families.
    declared_family_pairs: tuple[tuple[str, str], ...] | None = None
    #: The deployment's concept groups, carried on the one object both
    #: surfaces already resolve and pass. Unlike every control above it WIDENS
    #: rather than narrows a page, so it is disclosed under its own
    #: `concept_expansion` envelope key rather than the scope keys.
    concept_groups: tuple[ConceptGroup, ...] = ()
    #: Profile subtypes the pinned catalog lacks and this scope therefore
    #: left out. Empty unless the caller passed `absent_subtypes_allowed`
    #: (the operator console does); every other caller refuses instead. A
    #: caller that resolves with the allowance MUST show these names, or the
    #: page is narrower than it says.
    absent_subtypes: tuple[str, ...] = ()

    @property
    def is_scoped(self) -> bool:
        return self.subtypes is not None

    @property
    def is_family_scoped(self) -> bool:
        return self.source_ids is not None

    def family_of(self, source: Any) -> str | None:
        """The declared family for one producer id, or None.

        None covers both "no families declared" and "this producer is in none",
        which the envelope disambiguates once via `source_families_declared`.
        """
        if self.declared_family_pairs is None or source is None:
            return None
        for producer, family in self.declared_family_pairs:
            if producer == source:
                return family
        return None


#: The unscoped scope. Distinct from "no profile was resolvable" — that raises.
UNSCOPED = ResolvedScope()


# ---------- manifest side ----------

def parse_search_config(
    raw: Any, *, manifest_path: Any = None,
) -> SearchConfig | None:
    """Validate and parse a manifest `search:` block. Returns None when absent.

    Strict on every key, because a silently-ignored `default_profle:` would
    leave a deployment believing it had scoped its search when it had not.
    """
    where = f" (Manifest: {manifest_path})" if manifest_path else ""
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise SearchProfileConfigInvalid(
            f"deployment.yaml `search:` must be a mapping; got "
            f"{type(raw).__name__}.{where}",
            fix_hint=(
                "declare `search:` as a mapping with `profiles:` and an "
                "optional `default_profile:`"
            ),
        )
    unknown = sorted(set(raw) - _SEARCH_BLOCK_KEYS)
    if unknown:
        raise SearchProfileConfigInvalid(
            f"deployment.yaml `search:` has unknown key(s) "
            f"{', '.join(repr(k) for k in unknown)}; accepted keys are "
            f"{sorted(_SEARCH_BLOCK_KEYS)}.{where}",
            data={"unknown_keys": unknown,
                  "accepted_keys": sorted(_SEARCH_BLOCK_KEYS)},
            fix_hint="fix the key spelling under `search:`",
        )

    raw_profiles = raw.get("profiles") or {}
    if not isinstance(raw_profiles, dict):
        raise SearchProfileConfigInvalid(
            f"deployment.yaml `search.profiles` must be a mapping of "
            f"name -> profile; got {type(raw_profiles).__name__}.{where}",
            fix_hint="declare `search.profiles` as name -> {subtypes: [...]}",
        )

    families = _parse_source_families(raw.get("source_families"), where=where)

    profiles: dict[str, SearchProfile] = {}
    for name, body in raw_profiles.items():
        profiles[str(name)] = _parse_profile(str(name), body, where=where)

    # A profile naming an undeclared family RAISES at load rather than at the
    # first search: the profile would otherwise select nothing, and an empty
    # page is indistinguishable from "your query matched nothing".
    for profile in profiles.values():
        undeclared = [
            name for name in profile.source_families if name not in families
        ]
        if undeclared:
            raise UnknownSourceFamily(
                f"deployment.yaml search profile {profile.name!r} names source "
                f"famil(ies) {undeclared} that are not declared under "
                f"`search.source_families`; declared families are "
                f"{sorted(families) or 'none'}.{where}",
                data={"profile": profile.name, "undeclared": undeclared,
                      "declared": sorted(families)},
                fix_hint=(
                    f"declare the family under `search.source_families`, or "
                    f"fix the name in `search.profiles.{profile.name}."
                    f"source_families`"
                ),
            )

    default_profile = raw.get("default_profile")
    if default_profile is not None:
        default_profile = str(default_profile)
        if (
            default_profile != RESERVED_ALL
            and default_profile not in profiles
        ):
            raise UnknownSearchProfile(
                f"deployment.yaml `search.default_profile` is "
                f"{default_profile!r}, which is not declared under "
                f"`search.profiles`; declared profiles are "
                f"{sorted(profiles)}.{where}",
                data={"requested": default_profile,
                      "declared": sorted(profiles)},
                fix_hint=(
                    "set `search.default_profile` to a declared profile "
                    f"({sorted(profiles)}) or to the reserved "
                    f"{RESERVED_ALL!r}"
                ),
            )
    return SearchConfig(
        profiles=profiles,
        default_profile=default_profile,
        source_families=families,
        concept_groups=parse_concept_groups(
            raw.get("concept_groups"), where=where,
        ),
    )


def _parse_source_families(
    raw: Any, *, where: str,
) -> dict[str, SourceFamily]:
    """Validate `search.source_families:`. Absent parses to an empty mapping.

    Families are DEPLOYMENT-DECLARED, never derived. The repository's only
    existing source-family table (`source_catalog._SOURCE_FAMILY_RANK`) is
    setup-wizard data for one deployment and carries no authority here; reusing
    it would hard-code one project's opinion into every graph's retrieval.
    """
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise SourceFamilyConfigInvalid(
            f"deployment.yaml `search.source_families` must be a mapping of "
            f"family name -> list of producer ids; got "
            f"{type(raw).__name__}.{where}",
            fix_hint=(
                "declare `search.source_families` as "
                "`<family>: [<producer id>, ...]`"
            ),
        )
    families: dict[str, SourceFamily] = {}
    seen_sources: dict[str, str] = {}
    for raw_name, body in raw.items():
        name = str(raw_name)
        if name == RESERVED_ALL:
            raise SourceFamilyConfigInvalid(
                f"deployment.yaml declares a source family named "
                f"{RESERVED_ALL!r}, which is reserved: it always means every "
                f"source, so that a profile can never trap a caller inside a "
                f"family.{where}",
                fix_hint=(
                    f"rename the family; {RESERVED_ALL!r} cannot be declared"
                ),
            )
        if not isinstance(body, list) or not body:
            raise SourceFamilyConfigInvalid(
                f"deployment.yaml source family {name!r} must be a non-empty "
                f"list of producer ids; a family naming no source would filter "
                f"every result away and return an empty page indistinguishable "
                f"from 'nothing matched'.{where}",
                data={"family": name},
                fix_hint=(
                    f"give `search.source_families.{name}` at least one "
                    f"producer id"
                ),
            )
        sources = [str(item) for item in body]
        if any(not item.strip() for item in sources):
            raise SourceFamilyConfigInvalid(
                f"deployment.yaml source family {name!r} declares a blank "
                f"producer id.{where}",
                data={"family": name},
                fix_hint=(
                    f"remove the blank entry from "
                    f"`search.source_families.{name}`"
                ),
            )
        deduped = tuple(dict.fromkeys(sources))
        for source in deduped:
            other = seen_sources.get(source)
            if other is not None and other != name:
                # A producer in two families has no single family label, so the
                # per-row `source_family` field would have to pick one. Refuse
                # instead of picking.
                raise SourceFamilyConfigInvalid(
                    f"deployment.yaml declares producer id {source!r} in both "
                    f"source families {other!r} and {name!r}; a result row "
                    f"would then have no single `source_family` to report."
                    f"{where}",
                    data={"source": source, "families": sorted([other, name])},
                    fix_hint=(
                        "declare each producer id in exactly one family"
                    ),
                )
            seen_sources[source] = name
        families[name] = SourceFamily(name=name, sources=deduped)
    return families


def _parse_profile(name: str, body: Any, *, where: str) -> SearchProfile:
    if name == RESERVED_ALL:
        raise SearchProfileConfigInvalid(
            f"deployment.yaml declares a search profile named "
            f"{RESERVED_ALL!r}, which is reserved: it always means the "
            f"unscoped search, so that a deployment default can never trap a "
            f"caller inside a scope.{where}",
            fix_hint=f"rename the profile; {RESERVED_ALL!r} cannot be declared",
        )
    if not isinstance(body, dict):
        raise SearchProfileConfigInvalid(
            f"deployment.yaml search profile {name!r} must be a mapping; got "
            f"{type(body).__name__}.{where}",
            fix_hint=f"declare `{name}: {{subtypes: [...]}}`",
        )
    unknown = sorted(set(body) - _PROFILE_KEYS)
    if unknown:
        raise SearchProfileConfigInvalid(
            f"deployment.yaml search profile {name!r} has unknown key(s) "
            f"{', '.join(repr(k) for k in unknown)}; accepted keys are "
            f"{sorted(_PROFILE_KEYS)}.{where}",
            data={"profile": name, "unknown_keys": unknown,
                  "accepted_keys": sorted(_PROFILE_KEYS)},
            fix_hint=f"fix the key spelling under `search.profiles.{name}`",
        )
    subtypes = body.get("subtypes")
    if not isinstance(subtypes, list) or not subtypes:
        raise SearchProfileConfigInvalid(
            f"deployment.yaml search profile {name!r} must declare a "
            f"non-empty `subtypes` list; an empty scope would return an empty "
            f"page indistinguishable from 'nothing matched'.{where}",
            data={"profile": name},
            fix_hint=f"give `search.profiles.{name}.subtypes` at least one subtype",
        )
    names = [str(s) for s in subtypes]
    if any(not s.strip() for s in names):
        raise SearchProfileConfigInvalid(
            f"deployment.yaml search profile {name!r} declares an empty "
            f"subtype name.{where}",
            fix_hint=f"remove the blank entry from `search.profiles.{name}.subtypes`",
        )
    description = body.get("description")
    return SearchProfile(
        name=name,
        subtypes=tuple(dict.fromkeys(names)),
        description=str(description) if description is not None else None,
        source_families=_parse_profile_name_list(
            body.get("source_families"), profile=name, key="source_families",
            where=where,
        ),
        exclude_path_globs=_parse_exclude_path_globs(
            body.get("exclude_path_globs"), profile=name, where=where,
        ),
        max_per_parent=validate_max_per_parent(
            body.get("max_per_parent"),
            origin=f"search.profiles.{name}.max_per_parent",
        ),
    )


def _parse_profile_name_list(
    raw: Any, *, profile: str, key: str, where: str,
) -> tuple[str, ...]:
    if raw is None:
        return ()
    if not isinstance(raw, list) or not raw:
        raise SearchProfileConfigInvalid(
            f"deployment.yaml search profile {profile!r} `{key}` must be a "
            f"non-empty list; an empty one would declare an authority scope "
            f"that selects nothing.{where}",
            data={"profile": profile, "key": key},
            fix_hint=(
                f"give `search.profiles.{profile}.{key}` at least one entry, "
                f"or remove the key"
            ),
        )
    values = [str(item) for item in raw]
    if any(not item.strip() for item in values):
        raise SearchProfileConfigInvalid(
            f"deployment.yaml search profile {profile!r} `{key}` declares a "
            f"blank entry.{where}",
            data={"profile": profile, "key": key},
            fix_hint=f"remove the blank entry from `{key}`",
        )
    return tuple(dict.fromkeys(values))


def _parse_exclude_path_globs(
    raw: Any, *, profile: str, where: str,
) -> tuple[str, ...]:
    globs = _parse_profile_name_list(
        raw, profile=profile, key="exclude_path_globs", where=where,
    )
    for pattern in globs:
        # Proves the pattern is translatable BEFORE any search runs. A pattern
        # accepted here and silently untranslatable later would exclude nothing
        # while the envelope reported the exclusion.
        glob_to_like(pattern)
    return globs


def validate_max_per_parent(value: Any, *, origin: str) -> int | None:
    """Validate a per-parent diversity bound. Pure — no connection, no catalog.

    `None` is off. Anything else must be an integer >= 1; 0, negatives, bools
    and non-integers RAISE rather than being clamped, because a clamped bound
    silently returns a different page from the one the caller asked for."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise DiversityBoundInvalid(
            f"{origin} must be an integer >= 1; got "
            f"{type(value).__name__} {value!r}. It is refused rather than "
            f"coerced: a coerced bound returns a different page from the one "
            f"the caller asked for, and nothing in the response would say so.",
            data={"origin": origin, "value": repr(value)},
            fix_hint=f"set {origin} to an integer >= 1, or omit it",
        )
    if value < 1:
        raise DiversityBoundInvalid(
            f"{origin} must be >= 1; got {value}. Zero would mean 'return no "
            f"row that has a parent', a page indistinguishable from 'nothing "
            f"matched'.",
            data={"origin": origin, "value": value},
            fix_hint=f"set {origin} to an integer >= 1, or omit it",
        )
    return int(value)


# ---------- path exclusion ----------

#: Glob metacharacters this translation does NOT implement. Accepting them and
#: matching them literally would exclude nothing while the envelope reported an
#: exclusion, so they are refused at load.
_UNSUPPORTED_GLOB_CHARS = "[]{}"


def _translate_glob(pattern: str) -> tuple[str, str]:
    """Return `(sql_like_pattern, python_regex)` for one path glob.

    ONE walk produces both forms, so the SQL predicate pushed into the ranked
    query and the Python residual re-check over EFFECTIVE attributes cannot
    drift apart.

    The implemented syntax is deliberately small and is DOCUMENTED as such
    rather than described as "glob": `*` and `**` both match any run of
    characters INCLUDING `/`, and `?` matches exactly one character. Everything
    else is literal. `tests/**` therefore excludes `tests/a/b.py`, and so does
    `tests/*` — which is the intended reading for a path-prefix exclusion and
    is stated instead of left to a caller's POSIX habits.
    """
    if not pattern.strip():
        raise PathGlobInvalid(
            "an empty path glob excludes nothing",
            fix_hint="remove the entry or give it a pattern",
        )
    bad = sorted({ch for ch in pattern if ch in _UNSUPPORTED_GLOB_CHARS})
    if bad:
        raise PathGlobInvalid(
            f"path glob {pattern!r} uses unsupported metacharacter(s) "
            f"{bad}; this translation implements only `*`, `**` and `?`. "
            f"Matching them literally would exclude nothing while the "
            f"envelope reported the exclusion.",
            data={"pattern": pattern, "unsupported": bad},
            fix_hint="rewrite the pattern using only `*`, `**` and `?`",
        )
    like: list[str] = []
    regex: list[str] = ["^"]
    index = 0
    while index < len(pattern):
        char = pattern[index]
        if char == "*":
            while index < len(pattern) and pattern[index] == "*":
                index += 1
            like.append("%")
            regex.append(".*")
            continue
        if char == "?":
            like.append("_")
            regex.append(".")
        elif char in {"%", "_", "\\"}:
            like.append("\\" + char)
            regex.append(re.escape(char))
        else:
            like.append(char)
            regex.append(re.escape(char))
        index += 1
    regex.append("$")
    return "".join(like), "".join(regex)


def glob_to_like(pattern: str) -> str:
    """The SQL `LIKE ... ESCAPE '\\'` pattern for one path glob."""
    return _translate_glob(pattern)[0]


def path_matches_glob(value: Any, pattern: str) -> bool:
    """The Python residual for one path glob, identical to its SQL form."""
    if not isinstance(value, str):
        return False
    return re.match(_translate_glob(pattern)[1], value, flags=re.DOTALL) is not None


def excluded_by_path_globs(attrs: Any, globs: Iterable[str]) -> bool:
    """True when a row's EFFECTIVE `attrs.path` matches any exclusion glob.

    Rows with no `path` attribute are never excluded: an exclusion is a
    statement about paths, and a row without one has not been named.
    """
    patterns = list(globs)
    if not patterns or not isinstance(attrs, dict):
        return False
    value = attrs.get("path")
    if not isinstance(value, str):
        return False
    return any(path_matches_glob(value, pattern) for pattern in patterns)


# ---------- family side ----------

#: `family_validation` values reported in the envelope.
FAMILY_VALIDATED_REGISTRY = "producer_profiles"
FAMILY_UNVALIDATED_REGISTRY_EMPTY = "registry_unpopulated"


def validate_family_sources(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    source_ids: Iterable[str],
    families: Iterable[str],
) -> str:
    """Check applied family producer ids against the deployment's registry.

    MEASURED, not assumed, and the measurement is reported either way.

    `okg.producer_profiles` is the deployment's producer registry. It is the
    only authority in the substrate that lists producer ids without scanning
    the graph: the node segment carries `source` but has no index on it, so a
    per-call `SELECT DISTINCT source` would be a sequential scan on every
    search, and the deployment's `source_registry.yaml` names configured
    SOURCES whose ids need not equal the producer id stamped on a node
    (enrichers, materializers and system producers write nodes with no registry
    entry).

    Two outcomes, both stated:

      `producer_profiles`     — the registry holds rows for this deployment and
                                every declared id was found. An id that is NOT
                                found RAISES.
      `registry_unpopulated`  — the deployment has registered no producer
                                profiles, so there is nothing to check against.
                                Returned and DISCLOSED, never presented as a
                                pass; the empty-page disclosure on the search
                                itself is what covers this case.
    """
    ids = sorted(dict.fromkeys(str(s) for s in source_ids))
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.producer_profiles "
            " WHERE deployment_name = %(deployment)s",
            {"deployment": deployment_name},
        )
        registered = int(cur.fetchone()[0])
        if registered == 0:
            return FAMILY_UNVALIDATED_REGISTRY_EMPTY
        cur.execute(
            "SELECT DISTINCT producer_id FROM okg.producer_profiles "
            " WHERE deployment_name = %(deployment)s "
            "   AND producer_id = ANY(%(ids)s)",
            {"deployment": deployment_name, "ids": ids},
        )
        present = {str(row[0]) for row in cur.fetchall()}
    absent = [source for source in ids if source not in present]
    if absent:
        raise SourceFamilySourceAbsent(
            f"source famil(ies) {sorted(set(families))} name producer id(s) "
            f"{absent} that deployment {deployment_name!r} has never "
            f"registered; the family would silently select less than it "
            f"declares, and on a typo it would select nothing.",
            data={"families": sorted(set(families)), "absent": absent,
                  "deployment": deployment_name,
                  "registered_producers": registered},
            fix_hint=(
                "fix the producer id in the deployment's "
                "`search.source_families` block, or register the producer "
                "profile for that source"
            ),
        )
    return FAMILY_VALIDATED_REGISTRY


def assert_method_can_filter_family(
    method: str, requested_family: Any,
) -> None:
    """Refuse an EXPLICIT family filter on a method that cannot carry it.

    Mirrors `assert_method_can_scope` exactly, including the asymmetry that
    matters: an explicit request RAISES, while a profile-DECLARED family simply
    does not apply on such a method and the envelope reports
    `source_family_applicable: false`. Otherwise a deployment `default_profile`
    that declares families would break every `attrs` lookup on the deployment.

    Pure argument validation — no connection, no manifest.
    """
    if requested_family is None:
        return
    if method in FAMILY_APPLICABLE_METHODS:
        return
    raise SourceFamilyNotApplicable(
        f"search method {method!r} cannot honour a source-family filter: its "
        f"paged result relation does not project the pinned producer id, so "
        f"the predicate cannot go into the query that pages, and applying it "
        f"afterwards would silently under-fill the page. Methods that do: "
        f"{list(FAMILY_APPLICABLE_METHODS)}.",
        data={"method": method,
              "supported_methods": list(FAMILY_APPLICABLE_METHODS),
              "requested": requested_family},
        fix_hint=(
            f"drop the source_family argument, or use one of "
            f"{list(FAMILY_APPLICABLE_METHODS)}"
        ),
    )


def normalize_requested_families(requested: Any) -> tuple[str, ...] | None:
    """Normalize the `source_family` argument to a tuple of names, or None."""
    if requested is None:
        return None
    if isinstance(requested, str):
        names = [requested]
    elif isinstance(requested, (list, tuple)):
        names = [str(item) for item in requested]
    else:
        raise UnknownSourceFamily(
            f"search: source_family must be a family name or a list of them; "
            f"got {type(requested).__name__}",
            data={"requested": repr(requested)},
            fix_hint="pass a declared family name, or a list of them",
        )
    names = [name.strip() for name in names]
    if not names or any(not name for name in names):
        raise UnknownSourceFamily(
            "search: source_family must name at least one declared family "
            "and may not contain a blank name",
            fix_hint="pass a declared family name, or omit the argument",
        )
    return tuple(dict.fromkeys(names))


#: How a refusal says a source is selected (issue #3666).
SOURCE_WHERE_HINT = (
    'select a source with where={"op": "eq", "field": "source", "value": "<source name>"}'
)
#: How a refusal says a record type is selected.
TYPE_WHERE_HINT = (
    'select a record type with where={"op": "eq", "field": "type", "value": "<record type>"}'
)


def family_fix_hint(declared: Iterable[str]) -> str:
    """What to pass instead of an unknown source family, from what exists.

    Never an empty list: a deployment that declares no family says so, and
    says how a source is selected.
    """
    names = sorted(declared)
    if names:
        return f"pass source_family with one of {names}, or {SOURCE_WHERE_HINT}"
    return (
        "omit source_family, which this deployment does not declare, and "
        + SOURCE_WHERE_HINT
    )


def unknown_source_refusal(
    name: str, *, declared: Iterable[str], close: Iterable[str] = (),
) -> UnknownSourceFamily:
    """`source_family=<name>` that is neither a declared family nor a source
    the read path may state. One sentence, the caller's value in it."""
    names = sorted(declared)
    close = list(close)
    return UnknownSourceFamily(
        f"search: source_family {name!r} is neither a source family this "
        "deployment declares "
        + (f"({names})" if names else "(it declares none)")
        + " nor a source it lists",
        data={"requested": [name], "declared": names},
        fix_hint=family_fix_hint(names) + (
            f"; sources with a close name: {', '.join(close)}" if close else ""
        ),
    )


def unknown_profile_refusal(name: str, *, declared: Iterable[str]) -> UnknownSearchProfile:
    """`profile=<name>` that is neither a declared profile nor a record type."""
    names = sorted(declared)
    return UnknownSearchProfile(
        f"search: profile {name!r} is neither a search profile this deployment "
        "declares "
        + (f"({names})" if names else "(it declares none)")
        + " nor a record type",
        data={"requested": name, "declared": names},
        fix_hint=(
            (f"pass profile with one of {names} or {RESERVED_ALL!r}" if names
             else f"omit profile, or pass {RESERVED_ALL!r} for no profile")
            + f", or {TYPE_WHERE_HINT}"
        ),
    )


def resolve_families(
    config: SearchConfig | None,
    *,
    profile: SearchProfile | None,
    requested: tuple[str, ...] | None,
) -> tuple[str, ...] | None:
    """Compose the profile's families with the caller's, or RAISE.

    Composition, stated precisely:

      profile declares none, caller names none  -> no family filter.
      profile declares none, caller names some  -> the caller's families.
      profile declares some, caller names none  -> the profile's families.
      profile declares some, caller names some  -> the INTERSECTION; an empty
                                                   intersection RAISES.

    The last rule is the one that carries the argument: a caller who names a
    family outside the profile has stated a belief about what they will get,
    and the empty page they would otherwise receive is indistinguishable from
    "your query matched nothing" — the same reasoning `SubtypeOutsideProfile`
    makes for subtypes. It also means the caller can NARROW a profile but never
    silently WIDEN past it; widening is what the reserved profile `all` is for.
    """
    declared = (config.source_families if config else {}) or {}
    if requested is not None:
        unknown = [name for name in requested if name not in declared]
        if unknown:
            raise UnknownSourceFamily(
                f"unknown source famil(ies) {unknown}; this deployment "
                f"declares {sorted(declared) or 'no source families'}.",
                data={"requested": list(requested),
                      "declared": sorted(declared)},
                # Built from what exists: never "pass one of []" (#3666).
                fix_hint=family_fix_hint(declared),
            )
    profile_families = tuple(profile.source_families) if profile else ()
    if requested is None:
        return profile_families or None
    if not profile_families:
        return requested
    intersection = tuple(
        name for name in requested if name in set(profile_families)
    )
    if not intersection:
        profile_name = profile.name if profile is not None else None
        raise SourceFamilyEmptyIntersection(
            f"source famil(ies) {list(requested)} fall outside search profile "
            f"{profile_name!r}, which selects {list(profile_families)}. "
            f"Returning the empty page here would be indistinguishable from "
            f"'nothing matched'.",
            data={"requested": list(requested),
                  "profile": profile_name,
                  "profile_families": list(profile_families)},
            fix_hint=(
                f"choose a family the profile selects "
                f"({list(profile_families)}), or pass profile "
                f"{RESERVED_ALL!r} (--scope {RESERVED_ALL} on the command "
                f"line) to leave the profile's authority scope"
            ),
        )
    return intersection


def family_source_ids(
    config: SearchConfig | None, families: Iterable[str],
) -> tuple[str, ...]:
    """The union of producer ids the named families declare, sorted."""
    declared = (config.source_families if config else {}) or {}
    ids: set[str] = set()
    for name in families:
        family = declared.get(name)
        if family is None:  # unreachable: resolve_families validated the names
            raise UnknownSourceFamily(
                f"unknown source family {name!r}",
                data={"requested": name, "declared": sorted(declared)},
            )
        ids.update(family.sources)
    return tuple(sorted(ids))


# ---------- catalog side ----------

def _assert_closure_complete(
    conn: psycopg.Connection, *, catalog_version_id: int,
) -> None:
    """Refuse to resolve against a catalog whose refinement closure is short.

    `okg.subtype_ancestors` is backfilled with depth-0 REFLEXIVE rows only for
    catalog versions compiled before that table existed (see the backfill in
    `db/schema.sql`). Expanding a profile against such a version would silently
    omit every refinement and report a confident `scoped_subtypes` that is
    wrong — a missing closure read as "no refinements exist". So completeness
    is MEASURED against `okg.subtypes.parent_subtype`, never assumed.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT s.name, s.parent_subtype
              FROM okg.subtypes s
             WHERE s.catalog_version_id = %(cv)s
               AND s.parent_subtype IS NOT NULL
               AND NOT EXISTS (
                     SELECT 1 FROM okg.subtype_ancestors a
                      WHERE a.catalog_version_id = s.catalog_version_id
                        AND a.subtype_name = s.name
                        AND a.ancestor_name = s.parent_subtype
                        AND a.depth >= 1
                   )
             ORDER BY s.name
            """,
            {"cv": catalog_version_id},
        )
        missing = cur.fetchall()
    if missing:
        pairs = [f"{name} -> {parent}" for name, parent in missing]
        raise SubtypeClosureIncomplete(
            f"catalog version {catalog_version_id} has an incomplete subtype "
            f"refinement closure: {len(pairs)} subtype(s) declare a parent "
            f"with no depth>=1 ancestor row ({', '.join(pairs[:5])}"
            f"{', ...' if len(pairs) > 5 else ''}). Resolving a search "
            f"profile against it would silently omit those refinements.",
            data={"catalog_version_id": catalog_version_id,
                  "missing": pairs},
            fix_hint=(
                "recompile the catalog so `okg.subtype_ancestors` is "
                "repopulated: `okg catalog load --deployment <deployment> "
                "--apply` then "
                "`okg up <deployment>`"
            ),
        )


#: Which of a profile's absent subtype names a caller accepts leaving out.
#: Called with the absent names; returns the subset it accepts. See
#: `expand_subtypes_skipping_absent`.
AbsentSubtypesAllowed = Callable[[tuple[str, ...]], Iterable[str]]


def expand_subtypes(
    conn: psycopg.Connection,
    *,
    catalog_version_id: int,
    subtypes: Iterable[str],
    profile_name: str | None = None,
) -> tuple[str, ...]:
    """Expand each subtype to itself plus every subtype refined from it.

    A profile naming a parent must not go stale the moment someone refines a
    new subtype under it, so expansion is by the closure table rather than by
    the literal declared names. Raises if any declared name is absent from the
    pinned catalog — an absent name would contribute nothing and turn a typo
    into a quietly narrower scope.
    """
    expanded, _absent = _expand(
        conn,
        catalog_version_id=catalog_version_id,
        subtypes=subtypes,
        profile_name=profile_name,
        absent_allowed=None,
    )
    return expanded


def expand_subtypes_skipping_absent(
    conn: psycopg.Connection,
    *,
    catalog_version_id: int,
    subtypes: Iterable[str],
    profile_name: str | None,
    absent_allowed: AbsentSubtypesAllowed,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Expand like `expand_subtypes`, leaving out absent names the caller accepts.

    Returns `(expanded, absent)`. A subtype the pinned catalog lacks is left
    out only when `absent_allowed` accepts it; any name it does not accept
    still raises `ProfileSubtypeAbsent`, and so does a profile none of whose
    names is present, because an empty scope would read as "nothing matched".
    The caller owns disclosing `absent`: this function only turns the
    narrowing into a stated fact instead of a refusal.
    """
    return _expand(
        conn,
        catalog_version_id=catalog_version_id,
        subtypes=subtypes,
        profile_name=profile_name,
        absent_allowed=absent_allowed,
    )


def _expand(
    conn: psycopg.Connection,
    *,
    catalog_version_id: int,
    subtypes: Iterable[str],
    profile_name: str | None,
    absent_allowed: AbsentSubtypesAllowed | None,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    declared = tuple(dict.fromkeys(str(s) for s in subtypes))
    if not declared:
        raise SearchProfileConfigInvalid(
            "search profile resolved to an empty subtype list",
            fix_hint="declare at least one subtype for the profile",
        )
    _assert_closure_complete(conn, catalog_version_id=catalog_version_id)
    label = f"{(repr(profile_name) + ' ') if profile_name else ''}"
    with conn.cursor() as cur:
        cur.execute(
            "SELECT name FROM okg.subtypes "
            " WHERE catalog_version_id = %(cv)s AND name = ANY(%(names)s)",
            {"cv": catalog_version_id, "names": list(declared)},
        )
        present = {str(row[0]) for row in cur.fetchall()}
        absent = tuple(name for name in declared if name not in present)
        if absent and absent_allowed is None:
            raise ProfileSubtypeAbsent(
                f"search profile {label}"
                f"names subtype(s) {list(absent)} that do not exist in catalog "
                f"version {catalog_version_id}; the profile would silently "
                f"scope to less than it declares.",
                data={"profile": profile_name, "absent": list(absent),
                      "catalog_version_id": catalog_version_id},
                fix_hint=(
                    "fix the subtype spelling in the deployment's "
                    "`search.profiles` block, or add the subtype to the "
                    "ontology and recompose the catalog"
                ),
            )
        if absent:
            assert absent_allowed is not None
            accepted = {str(name) for name in absent_allowed(absent)}
            refused = [name for name in absent if name not in accepted]
            if refused:
                raise ProfileSubtypeAbsent(
                    f"search profile {label}names subtype(s) {refused} that "
                    f"neither catalog version {catalog_version_id} nor the "
                    f"deployment's own ontology modules define; that is a "
                    f"broken profile, not a catalog that is behind its "
                    f"manifest.",
                    data={"profile": profile_name, "absent": refused,
                          "catalog_version_id": catalog_version_id,
                          "undefined_by_deployment": True},
                    fix_hint=(
                        "fix the subtype spelling in the deployment's "
                        "`search.profiles` block, or enroll the module that "
                        "defines it"
                    ),
                )
            if not present:
                raise ProfileSubtypeAbsent(
                    f"search profile {label}names no subtype that exists in "
                    f"catalog version {catalog_version_id}; searching it would "
                    f"return an empty page that reads as 'nothing matched'.",
                    data={"profile": profile_name, "absent": list(absent),
                          "catalog_version_id": catalog_version_id},
                    fix_hint=(
                        "load and publish the catalog the deployment's "
                        "manifest composes, or search with the profile "
                        f"{RESERVED_ALL!r}"
                    ),
                )
        kept = [name for name in declared if name in present]
        cur.execute(
            "SELECT DISTINCT subtype_name FROM okg.subtype_ancestors "
            " WHERE catalog_version_id = %(cv)s "
            "   AND ancestor_name = ANY(%(names)s)",
            {"cv": catalog_version_id, "names": kept},
        )
        expanded = {str(row[0]) for row in cur.fetchall()}
    # The closure carries depth-0 reflexive rows, so `expanded` already holds
    # the declared names; the union is belt-and-braces for a catalog whose
    # reflexive row is missing for a ROOT subtype (no parent, so
    # _assert_closure_complete does not cover it).
    return tuple(sorted(expanded | set(kept))), absent


def resolve_scope(
    conn: psycopg.Connection,
    *,
    catalog_version_id: int,
    config: SearchConfig | None,
    requested_profile: Optional[str],
    subtype_filter: Optional[str] = None,
    requested_families: Any = None,
    requested_max_per_parent: Any = None,
    deployment_name: Optional[str] = None,
    absent_subtypes_allowed: AbsentSubtypesAllowed | None = None,
) -> ResolvedScope:
    """Resolve the selection an operator-surface search should run under.

    `absent_subtypes_allowed`, when given, lets the applied profile leave out
    subtypes the pinned catalog lacks and records them on
    `ResolvedScope.absent_subtypes` (see `expand_subtypes_skipping_absent`).
    Without it an absent subtype raises, as it always has.

    The deployment `default_profile` applies ONLY when the caller named neither
    a profile nor a subtype — a caller who named a subtype has already stated
    their scope, and silently intersecting a default into it would be a second,
    invisible narrowing.

    The three authority controls compose on top of that:

      * `requested_families` intersects the applied profile's declared families
        (`resolve_families`) and resolves to concrete producer ids.
      * `exclude_path_globs` comes ONLY from the applied profile. There is no
        request-level form: an ad-hoc exclusion is a filter, and `filter()`
        already exists; a profile exclusion is a deployment's standing
        statement about what its analysts are asking about.
      * `requested_max_per_parent` overrides the applied profile's default when
        the caller names one.

    The reserved profile `all` is the escape hatch in EVERY axis: no subtype
    scope, no profile-declared families, no exclusions, no profile diversity
    default. A family the CALLER names still applies under `all`, because the
    caller asked for it; that is a narrowing they chose, not one the deployment
    imposed.
    """
    requested_families = normalize_requested_families(requested_families)
    requested_bound = validate_max_per_parent(
        requested_max_per_parent, origin="search: max_per_parent",
    )
    declared_pairs = config.family_by_source() if config else None
    if config is not None and not config.source_families:
        declared_pairs = None

    def _finish(
        *,
        profile_name: str | None,
        subtypes: tuple[str, ...] | None,
        profile: SearchProfile | None,
        absent: tuple[str, ...] = (),
    ) -> ResolvedScope:
        families = resolve_families(
            config, profile=profile, requested=requested_families,
        )
        source_ids: tuple[str, ...] | None = None
        validation: str | None = None
        if families:
            source_ids = family_source_ids(config, families)
            if deployment_name:
                validation = validate_family_sources(
                    conn,
                    deployment_name=deployment_name,
                    source_ids=source_ids,
                    families=families,
                )
        return ResolvedScope(
            profile=profile_name,
            subtypes=subtypes,
            source_families=tuple(families) if families else None,
            source_ids=source_ids,
            excluded_path_globs=(
                tuple(profile.exclude_path_globs) if profile else ()
            ),
            max_per_parent=(
                requested_bound
                if requested_bound is not None
                else (profile.max_per_parent if profile else None)
            ),
            max_per_parent_origin=(
                "argument"
                if requested_bound is not None
                else (
                    "profile"
                    if profile is not None and profile.max_per_parent is not None
                    else None
                )
            ),
            family_validation=validation,
            declared_family_pairs=declared_pairs,
            # Carried under `all` too: the reserved profile turns off the
            # narrowings a deployment IMPOSES, and expansion narrows nothing.
            concept_groups=tuple(config.concept_groups) if config else (),
            absent_subtypes=absent,
        )

    if requested_profile == RESERVED_ALL:
        return _finish(profile_name=RESERVED_ALL, subtypes=None, profile=None)

    declared = config.profiles if config else {}
    name = requested_profile
    if name is None:
        if config is None or config.default_profile is None:
            return _finish(profile_name=None, subtypes=None, profile=None)
        if subtype_filter is not None:
            return _finish(profile_name=None, subtypes=None, profile=None)
        name = config.default_profile
        if name == RESERVED_ALL:
            return _finish(
                profile_name=RESERVED_ALL, subtypes=None, profile=None,
            )

    profile = declared.get(name)
    if profile is None:
        raise UnknownSearchProfile(
            f"unknown search profile {name!r}; this deployment declares "
            f"{sorted(declared) or 'no profiles'}. The reserved profile "
            f"{RESERVED_ALL!r} always means unscoped.",
            data={"requested": name, "declared": sorted(declared)},
            fix_hint=(
                f"pass profile (--scope on the command line) with one of "
                f"{sorted(declared)} or {RESERVED_ALL!r}, or {TYPE_WHERE_HINT}"
            ),
        )
    if absent_subtypes_allowed is None:
        expanded = expand_subtypes(
            conn,
            catalog_version_id=catalog_version_id,
            subtypes=profile.subtypes,
            profile_name=profile.name,
        )
        absent: tuple[str, ...] = ()
    else:
        expanded, absent = expand_subtypes_skipping_absent(
            conn,
            catalog_version_id=catalog_version_id,
            subtypes=profile.subtypes,
            profile_name=profile.name,
            absent_allowed=absent_subtypes_allowed,
        )
    return _finish(
        profile_name=profile.name, subtypes=expanded, profile=profile,
        absent=absent,
    )


def assert_method_can_scope(method: str, requested_profile: Optional[str]) -> None:
    """Refuse an explicit profile on a method that cannot honour a subtype scope.

    Accepting it and quietly discarding it is the fail-open this contract
    exists to prevent: the caller would read an unscoped page believing it was
    scoped. Methods that DO return node rows are in
    `PROFILE_APPLICABLE_METHODS`; the deployment default simply does not apply
    to the others, and their envelopes say so via `profile_applicable`.

    Pure argument validation — no connection, no manifest — so it is provable
    without a database.
    """
    if requested_profile is None:
        return
    if method in PROFILE_APPLICABLE_METHODS:
        return
    raise ProfileNotApplicable(
        f"search method {method!r} cannot honour a search profile; a subtype "
        f"scope has no defined meaning for it. Methods that do: "
        f"{list(PROFILE_APPLICABLE_METHODS)}.",
        data={"method": method,
              "supported_methods": list(PROFILE_APPLICABLE_METHODS),
              "requested": requested_profile},
        fix_hint=(
            f"drop the profile argument, or use one of "
            f"{list(PROFILE_APPLICABLE_METHODS)}"
        ),
    )


def resolve_for_surface(
    conn: psycopg.Connection,
    *,
    catalog_version_id: int,
    manifest_search_block: Any,
    manifest_path: Any = None,
    requested_profile: Optional[str],
    subtype_filter: Optional[str],
    method: str,
    requested_families: Any = None,
    requested_max_per_parent: Any = None,
    deployment_name: Optional[str] = None,
    absent_subtypes_allowed: AbsentSubtypesAllowed | None = None,
) -> ResolvedScope:
    """The one entry point the CLI and the MCP tool both call.

    Both surfaces must resolve the SAME profile, families, exclusions and
    diversity bound from the SAME manifest block, or an agent and an operator
    querying one deployment would see different selections and neither would
    know it.
    """
    assert_method_can_scope(method, requested_profile)
    assert_method_can_filter_family(method, requested_families)
    if method not in PROFILE_APPLICABLE_METHODS:
        return UNSCOPED
    config = parse_search_config(
        manifest_search_block, manifest_path=manifest_path,
    )
    resolved = resolve_scope(
        conn,
        catalog_version_id=catalog_version_id,
        config=config,
        requested_profile=requested_profile,
        subtype_filter=subtype_filter,
        requested_families=requested_families,
        requested_max_per_parent=requested_max_per_parent,
        deployment_name=deployment_name,
        absent_subtypes_allowed=absent_subtypes_allowed,
    )
    return restrict_scope_to_method(resolved, method)


def restrict_scope_to_method(
    scope: ResolvedScope, method: str,
) -> ResolvedScope:
    """Drop the profile-DECLARED controls this method cannot honour.

    Each control is dropped independently, because they do not have the same
    applicability. Getting this wrong is not cosmetic: before this split, a
    profile that declared `max_per_parent` made EVERY `semantic` and `alias`
    search on that deployment raise `diversity_not_applicable` — an error
    naming a parameter the caller had never passed, on a profile they may not
    even have selected, because a deployment `default_profile` applies on its
    own.

    The asymmetry is the same one `assert_method_can_scope` established: an
    EXPLICIT request on a method that cannot honour it RAISES (the caller
    stated a belief and must not be handed a page that ignored it); a
    profile-DECLARED control simply does not apply, and the envelope says so
    with `<control>_applicable: false`. Both halves are refusals to be silent;
    they differ only in who made the claim.
    """
    if (
        method in FAMILY_APPLICABLE_METHODS
        and method in DIVERSITY_APPLICABLE_METHODS
    ):
        return scope
    keeps_family = method in FAMILY_APPLICABLE_METHODS
    return ResolvedScope(
        profile=scope.profile,
        subtypes=scope.subtypes,
        source_families=scope.source_families if keeps_family else None,
        source_ids=scope.source_ids if keeps_family else None,
        excluded_path_globs=(
            scope.excluded_path_globs if keeps_family else ()
        ),
        max_per_parent=(
            scope.max_per_parent
            if method in DIVERSITY_APPLICABLE_METHODS
            else None
        ),
        max_per_parent_origin=(
            scope.max_per_parent_origin
            if method in DIVERSITY_APPLICABLE_METHODS
            else None
        ),
        family_validation=scope.family_validation if keeps_family else None,
        declared_family_pairs=scope.declared_family_pairs,
        concept_groups=(
            scope.concept_groups
            if method in CONCEPT_EXPANSION_APPLICABLE_METHODS
            else ()
        ),
        absent_subtypes=scope.absent_subtypes,
    )


#: `diversity_reason` when `rank_by="records"` leaves a profile bound unapplied.
DIVERSITY_REASON_RECORDS = "records_returns_one_row_per_parent"


def restrict_scope_to_rank_mode(
    scope: ResolvedScope, rank_by: str,
) -> tuple[ResolvedScope, str | None]:
    """Drop a PROFILE-default `max_per_parent` under `rank_by="records"`.

    `records` already returns one row per parent, so a per-parent bound could
    never remove a row; refusing it would refuse every search on a deployment
    whose profile declares one (decision 6, PACT search-result-shape). The
    bound is dropped and the reason returned for the envelope. An argument
    bound is never dropped here: `assert_diversity_rank_mode` refuses it.
    """
    if (
        rank_by != "records"
        or scope.max_per_parent is None
        or scope.max_per_parent_origin != "profile"
    ):
        return scope, None
    return replace(
        scope, max_per_parent=None, max_per_parent_origin=None,
    ), DIVERSITY_REASON_RECORDS


def intersect_subtype_filter(
    scope: ResolvedScope, subtype_filter: Optional[str],
) -> None:
    """Refuse a `subtype` filter that falls outside the resolved scope.

    `filter["subtype"]` stays an EXACT match and is intersected against the
    EXPANDED profile set, so naming a refinement of a profiled parent is legal.
    Naming something outside it raises rather than returning an empty page,
    because an empty page cannot be told apart from "nothing matched".
    """
    if subtype_filter is None or not scope.is_scoped:
        return
    assert scope.subtypes is not None
    if subtype_filter not in scope.subtypes:
        raise SubtypeOutsideProfile(
            f"subtype filter {subtype_filter!r} is outside search profile "
            f"{scope.profile!r}, which scopes to {list(scope.subtypes)}. "
            f"Returning an empty page here would be indistinguishable from "
            f"'nothing matched'.",
            data={"subtype": subtype_filter, "profile": scope.profile,
                  "scope": list(scope.subtypes)},
            fix_hint=(
                f"drop the subtype filter (--subtype on the command line), "
                f"choose one inside the profile, or pass profile "
                f"{RESERVED_ALL!r} (--scope {RESERVED_ALL})"
            ),
        )


def scope_emptiness_disclosure(
    conn: psycopg.Connection,
    session: Any,
    scope: ResolvedScope,
) -> dict[str, Any]:
    """The envelope fields that make an empty SCOPED page legible.

    Always returns the keys. If the live-node count cannot be measured — the
    BM25 index the segment is resolved through is absent, which is a real state
    on a deployment that has never published — it says so with
    `scope_node_counts: None` and `scope_holds_no_nodes: None` rather than
    omitting the keys. An absent key would be read as "the scope is fine",
    which is the failure mode this disclosure exists to prevent.
    """
    from okg.substrate.bm25_node_search import (
        SearchIndexUnavailable,
        branch_point,
        resolve_node_text_bm25_index,
    )

    if scope.subtypes is None:
        return {"scope_node_counts": None, "scope_holds_no_nodes": None}
    try:
        segment, _ = resolve_node_text_bm25_index(conn)
        counts = scope_node_counts(
            conn,
            segment=segment,
            branch_point_value=branch_point(conn, session),
            subtypes=scope.subtypes,
        )
    except SearchIndexUnavailable as exc:
        return {
            "scope_node_counts": None,
            "scope_holds_no_nodes": None,
            "scope_count_unavailable": str(exc),
        }
    return {
        "scope_node_counts": counts,
        "scope_holds_no_nodes": not any(counts.values()),
    }


def scope_node_counts(
    conn: psycopg.Connection,
    *,
    segment: str,
    branch_point_value: int,
    subtypes: Iterable[str],
) -> dict[str, int]:
    """Live node count per subtype at the pin, MEASURED.

    A zero-result scoped page has two very different causes — the query matched
    nothing, or the scope holds nothing — and they are indistinguishable from
    the page itself. This turns the second into a reported state.
    """
    names = list(dict.fromkeys(str(s) for s in subtypes))
    counts = {name: 0 for name in names}
    if not names:
        return counts
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT subtype, count(*) FROM {segment} "
            f" WHERE deleted = FALSE AND live_lo <= %(bp)s AND %(bp)s < live_hi "
            f"   AND subtype = ANY(%(names)s) "
            f" GROUP BY subtype",
            {"bp": branch_point_value, "names": names},
        )
        for subtype, count in cur.fetchall():
            counts[str(subtype)] = int(count)
    return counts


#: Producer ids listed verbatim in an envelope before the list is summarised.
#: A family may name many producers, and a page-sized envelope must not grow
#: with the deployment's producer count.
FAMILY_ID_DISCLOSURE_CAP = 50


#: Every key `family_disclosure` emits. Named so the MCP tool's non-ranked
#: paths (`attrs`, `identifier_mentions`) can prove they emit the SAME set
#: rather than a partial one — a payload carrying `source_family_applicable`
#: but omitting `excluded_path_globs` reads as "no exclusion was requested",
#: when in truth a profile declared one and this method dropped it.
FAMILY_DISCLOSURE_KEYS = (
    "source_families",
    "scoped_source_ids",
    "scoped_source_ids_truncated",
    "scoped_source_id_count",
    "source_family_validation",
    "source_family_applicable",
    "source_family_application",
    "excluded_path_globs",
    "path_exclusion_applicable",
    "excluded_path_globs_application",
    "diversity_applicable",
    "source_families_declared",
)


def family_disclosure(
    scope: ResolvedScope, *, method: str,
) -> dict[str, Any]:
    """The envelope fields that make a selected page legible.

    Always returns EVERY key in `FAMILY_DISCLOSURE_KEYS`. An absent key reads
    as "no authority filter ran", which is exactly the state this disclosure
    exists to distinguish from "one ran and you were not told".

    `method` is required because two of these facts are per-method and were
    previously reported as if they were not. `*_application` says WHERE each
    predicate ran: `ranked_sql` means it is inside the query that ranks and
    limits; `post_fetch_residual` means it ran in Python over the fetched
    window and the page may under-fill. Reporting an exclusion as simply
    "applied" when it ran as a residual is a true-but-incomplete claim, and the
    short page it produces is precisely what a caller misreads as "that is all
    there is".
    """
    ids = list(scope.source_ids or ())
    disclosed = ids[:FAMILY_ID_DISCLOSURE_CAP]
    family_applicable = method in FAMILY_APPLICABLE_METHODS
    return {
        "source_families": (
            list(scope.source_families)
            if scope.source_families is not None
            else None
        ),
        "scoped_source_ids": disclosed if scope.source_ids is not None else None,
        "scoped_source_ids_truncated": (
            len(ids) > len(disclosed) if scope.source_ids is not None else None
        ),
        "scoped_source_id_count": (
            len(ids) if scope.source_ids is not None else None
        ),
        "source_family_validation": scope.family_validation,
        "source_family_applicable": family_applicable,
        "source_family_application": (
            FAMILY_APPLICATION_BY_METHOD.get(method)
            if scope.source_ids is not None
            else None
        ),
        "excluded_path_globs": (
            list(scope.excluded_path_globs)
            if scope.excluded_path_globs
            else None
        ),
        # Exclusions ride the same applicability as the family filter: both are
        # profile-declared selection over node rows, and both are dropped
        # together by `restrict_scope_to_method`.
        "path_exclusion_applicable": family_applicable,
        "excluded_path_globs_application": (
            PATH_EXCLUSION_APPLICATION_BY_METHOD.get(method)
            if scope.excluded_path_globs
            else None
        ),
        "diversity_applicable": method in DIVERSITY_APPLICABLE_METHODS,
        # Whether the deployment declares ANY families. `false` is why every
        # row's `source_family` is null, and saying it once here is what keeps
        # a null row field from reading as "this producer is unclassified".
        "source_families_declared": scope.declared_family_pairs is not None,
    }


def family_node_counts(
    conn: psycopg.Connection,
    *,
    segment: str,
    branch_point_value: int,
    source_ids: Iterable[str],
) -> dict[str, int]:
    """Live node count per producer id at the pin, MEASURED.

    Only ever called on an EMPTY family-filtered page — the same discipline
    `scope_node_counts` follows — because the node segment carries no index on
    `source` and this is a bounded scan, not a free lookup.
    """
    names = list(dict.fromkeys(str(s) for s in source_ids))
    counts = {name: 0 for name in names}
    if not names:
        return counts
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT source, count(*) FROM {segment} "
            f" WHERE deleted = FALSE AND live_lo <= %(bp)s AND %(bp)s < live_hi "
            f"   AND source = ANY(%(names)s) "
            f" GROUP BY source",
            {"bp": branch_point_value, "names": names},
        )
        for source, count in cur.fetchall():
            counts[str(source)] = int(count)
    return counts


def family_emptiness_disclosure(
    conn: psycopg.Connection,
    session: Any,
    scope: ResolvedScope,
) -> dict[str, Any]:
    """Why a family-filtered page came back empty, MEASURED not inferred.

    "Your query matched nothing in this family" and "this family holds no node
    at this pin" are different facts and are indistinguishable from the page.
    The second is the one a typo'd producer id produces, and it is the case the
    `registry_unpopulated` validation state cannot rule out.
    """
    from okg.substrate.bm25_node_search import (
        SearchIndexUnavailable,
        branch_point,
        resolve_node_text_bm25_index,
    )

    if scope.source_ids is None:
        return {"source_family_node_counts": None,
                "source_family_holds_no_nodes": None}
    try:
        segment, _ = resolve_node_text_bm25_index(conn)
        counts = family_node_counts(
            conn,
            segment=segment,
            branch_point_value=branch_point(conn, session),
            source_ids=scope.source_ids,
        )
    except SearchIndexUnavailable as exc:
        return {
            "source_family_node_counts": None,
            "source_family_holds_no_nodes": None,
            "source_family_count_unavailable": str(exc),
        }
    return {
        "source_family_node_counts": counts,
        "source_family_holds_no_nodes": not any(counts.values()),
    }
