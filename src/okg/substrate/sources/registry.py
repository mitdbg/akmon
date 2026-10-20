"""Source-registry loader + validator (M3.1).

Loads `<deployment>/source_registry.yaml` into typed `SourceEntry`
records and validates each entry against the named profile matrix.

Legacy entries — those without a `source_class` key — are accepted
with a `legacy=True` flag. They are NOT scheduled by the M3+ Phase
A executor; they continue to run via the pre-rearchitecture path
until the deployment migrates. This avoids forcing a flag-day
migration of every existing registry.
"""
from __future__ import annotations

import copy
import difflib
import os
import re
import json
import math
import warnings
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Optional

from jsonschema import Draft202012Validator

from okg.substrate import fastyaml
from okg.substrate.schema_resources import schemas_dir

from okg.substrate.producer_authority import (
    PRODUCER_KINDS,
    normalize_allowed_record_kinds,
    normalize_authority_scope,
    normalize_output_signature,
    normalize_policy_clauses,
    normalize_write_envelope,
)
from okg.substrate.sources.profiles import (
    PROFILE_NAMES,
    REGISTRY_ONLY,
    validate_profile_tuple,
)
from okg.substrate.source_sync import (
    SourceSyncCapabilities,
    merge_source_sync_capabilities,
    source_sync_capabilities_from_adapter,
)


# `${VAR}` references expanded from the environment in source_registry.yaml
# values (see _substitute_node). Braced form only, so bare `$x` in globs/regex
# is never touched.
_ENV_VAR_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


_ADMISSION_MODES = frozenset({
    "direct",
    "fast_track",
    "review",
    "quarantine",
})
_PRODUCER_KINDS = frozenset({
    *PRODUCER_KINDS,
})

STRICT_ADMISSION_CONTRACT = "strict_v1"
LEGACY_ADMISSION_CONTRACT = "legacy_v1"
SOURCE_REGISTRY_DOCUMENT_KEYS = frozenset({
    "admission_contract",
    "sources",
    "code_repos",
})
SOURCE_ENTRY_CONTROL_KEYS = frozenset({
    "module",
    "class",
    "ownership_id",
    "source_class",
    "record_identity_kind",
    "record_identity_fields",
    "source_revision_kind",
    "deletion_semantics",
    "publication_mode",
    "record_authority_format",
    "params",
    "credential_refs",
    "credential_aliases",
    "required_for_baseline",
    "enabled_by_default",
    "predecessor_source",
    "sync",
    "admission_policy",
    "source_policy",
    "description",
    "configuration_hints",
    "lab_boundary",
    "mit_boundary",
    "microbench",
})
# Keys a legacy (unmarked) registry document may carry. A key starting with
# ``x-`` is an extension key: it holds YAML anchors (``x-defaults: &d ...``)
# and is never read, as in docker-compose files.
LEGACY_REGISTRY_DOCUMENT_KEYS = frozenset({"sources", "code_repos"})
LEGACY_REGISTRY_EXTENSION_PREFIX = "x-"
CODE_REPOS_KEYS = frozenset({"base", "repos", "expand", "params"})
CODE_REPO_ENTRY_KEYS = frozenset({
    "slug", "dir", "url", "tracked_branch", "manage_checkout", "params",
})
# An entry key a legacy registry may still carry although nothing reads it.
# `okg deployment bootstrap` drafted a per-source `schedule` block before
# issue #2952; promotion to strict_v1 (`guided_add.promote_registry_to_strict`)
# removes it and reports the removal, so a legacy registry keeps loading.
# Loading one warns (RetiredSourceRegistryKeyWarning).
LEGACY_RETIRED_ENTRY_KEYS = frozenset({"schedule"})


class RetiredSourceRegistryKeyWarning(UserWarning):
    """A legacy registry carries a retired key that nothing reads."""


def _warn_retired_schedule(registry_path: Path, sources: list[str]) -> None:
    """One warning per registry load (loads are cached by path and mtime,
    and Python prints a given warning text once per process)."""
    warnings.warn(
        f"{registry_path}: sources {', '.join(sources)} carry a `schedule` "
        "block. Nothing reads it: a scheduled refresh is the source's "
        "`sync.poll` block. Delete the `schedule` blocks; `okg deployment "
        "add-source` removes them when it promotes the registry to strict_v1.",
        RetiredSourceRegistryKeyWarning,
        stacklevel=2,
    )

SOURCE_REGISTRY_UNKNOWN_KEY = "source_registry_unknown_key"


class SourceRegistryUnknownKeyError(ValueError):
    """A legacy registry carries a key the loader would otherwise ignore.

    ``location`` is the dotted path of the mapping that holds the key
    (``<document>``, ``sources.<name>``, ``code_repos``,
    ``code_repos.repos[<i>]``); ``suggestion`` is the closest valid key, or
    ``None`` when nothing is close.
    """

    code = SOURCE_REGISTRY_UNKNOWN_KEY

    def __init__(
        self,
        *,
        registry_path: Path,
        location: str,
        key: str,
        valid: frozenset[str],
        suggestion: str | None,
        hint: str,
    ) -> None:
        self.registry_path = registry_path
        self.location = location
        self.key = key
        self.valid = valid
        self.suggestion = suggestion
        self.hint = hint
        super().__init__(
            f"{SOURCE_REGISTRY_UNKNOWN_KEY}: registry={registry_path} "
            f"path={location}: unknown key {key!r}{hint} "
            f"Valid keys here: {', '.join(sorted(valid))}."
        )


def _unknown_key_error(
    registry_path: Path,
    location: str,
    key: object,
    valid: frozenset[str],
    *,
    source: str | None = None,
) -> SourceRegistryUnknownKeyError:
    text = str(key)
    matches = difflib.get_close_matches(text, sorted(valid), n=1)
    suggestion = matches[0] if matches else None
    if suggestion is not None:
        hint = f"; did you mean {suggestion!r}?"
    elif source is not None:
        hint = (
            f"; if {text!r} is a setting of the source's reader, move it "
            f"under `params:` (sources.{source}.params.{text})."
        )
    elif location == "<document>":
        hint = "; a top-level key is `sources` or `code_repos`."
    elif location.startswith("code_repos.repos["):
        hint = (
            f"; a reader setting for this repository goes under "
            f"`params: {{<block>: {{{text}: ...}}}}`, e.g. "
            f"`params: {{git-files: {{{text}: ...}}}}`."
        )
    else:
        hint = (
            f"; a reader setting for every repository goes under "
            f"`code_repos.params: {{<block>: {{{text}: ...}}}}`, e.g. "
            f"`params: {{git-files: {{{text}: ...}}}}`."
        )
    return SourceRegistryUnknownKeyError(
        registry_path=registry_path,
        location=location,
        key=text,
        valid=valid,
        suggestion=suggestion,
        hint=hint,
    )


def validate_legacy_document_keys(
    document: Mapping[str, Any],
    registry_path: Path,
) -> None:
    """Refuse unknown keys an unmarked registry would otherwise ignore.

    Checks the document, the ``code_repos`` block and each
    ``code_repos.repos`` entry; ``validate_legacy_entry_keys`` checks each
    source entry once ``code_repos`` is expanded. A strict_v1 registry gets
    the same refusal from its closed schema.
    """
    for key in document:
        if key in LEGACY_REGISTRY_DOCUMENT_KEYS:
            continue
        if isinstance(key, str) and key.startswith(
            LEGACY_REGISTRY_EXTENSION_PREFIX
        ):
            continue
        raise _unknown_key_error(
            registry_path, "<document>", key, LEGACY_REGISTRY_DOCUMENT_KEYS,
        )
    code_repos = document.get("code_repos")
    if isinstance(code_repos, Mapping):
        for key in code_repos:
            if key not in CODE_REPOS_KEYS:
                raise _unknown_key_error(
                    registry_path, "code_repos", key, CODE_REPOS_KEYS,
                )
        repos = code_repos.get("repos")
        if isinstance(repos, list):
            for index, repo in enumerate(repos):
                if not isinstance(repo, Mapping):
                    continue
                for key in repo:
                    if key not in CODE_REPO_ENTRY_KEYS:
                        raise _unknown_key_error(
                            registry_path,
                            f"code_repos.repos[{index}]",
                            key,
                            CODE_REPO_ENTRY_KEYS,
                        )


def check_legacy_registry_keys(
    document: Any,
    registry_path: Path,
) -> None:
    """The legacy loader's key check, for callers that read the raw document.

    Nomos baseline seeding (``nomos.baseline.deployment_baseline_spec``) and
    provision read ``source_registry.yaml`` without the loader (they must
    tolerate an unresolved ``code_repos`` base). They call this so a
    misspelled key fails there as it does at ingest. ``okg nomos
    rollout-audit`` reaches it for a deployment through
    ``deployment_baseline_spec`` and reports the refusal as its
    ``baseline_policy_invalid`` finding. A strict_v1 document is left to its
    schema.
    """
    if not isinstance(document, Mapping) or "admission_contract" in document:
        return
    validate_legacy_document_keys(document, registry_path)
    sources = document.get("sources")
    if isinstance(sources, Mapping):
        for name, body in sources.items():
            if isinstance(body, Mapping):
                validate_legacy_entry_keys(str(name), body, registry_path)


def validate_legacy_entry_keys(
    name: str,
    body: Mapping[str, Any],
    registry_path: Path,
) -> None:
    """Refuse an entry key outside ``SOURCE_ENTRY_CONTROL_KEYS``.

    The entry's ``params`` mapping holds the reader's constructor arguments;
    those are checked against the reader (lint, doctor, ingest), not here.
    """
    for key in body:
        if key in LEGACY_RETIRED_ENTRY_KEYS:
            continue
        if key not in SOURCE_ENTRY_CONTROL_KEYS:
            raise _unknown_key_error(
                registry_path,
                f"sources.{name}",
                key,
                SOURCE_ENTRY_CONTROL_KEYS,
                source=name,
            )


@dataclass(frozen=True)
class SourceAdmissionPolicy:
    """Declared admission policy for one source-registry entry."""

    producer_id: str
    producer_kind: str
    trust_label: str
    admission_mode: str
    authority_scope: dict[str, Any]
    allowed_record_kinds: tuple[str, ...]
    output_signature: dict[str, Any]
    policy_clauses: tuple[dict[str, Any], ...]
    output_scope_summary: dict[str, Any]
    raw: dict[str, Any] = field(repr=False)
    # The producer's declared write envelope (spec §9.3), as the JSON summary
    # normalize_write_envelope produces. Explicit ``write_envelope`` block
    # wins; otherwise DERIVED from output_signature (declaration-derived, never
    # output-inferred). ``None`` when the source declares neither — surfaced by
    # the ``producer.envelope_missing`` lint, never invented here. The summary
    # also rides ``output_scope_summary["write_envelope"]`` so the materialized
    # profile carries it and profile_write_envelope() picks it up unchanged.
    write_envelope: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        payload = {
            "producer_id": self.producer_id,
            "producer_kind": self.producer_kind,
            "trust_label": self.trust_label,
            "admission_mode": self.admission_mode,
            "authority_scope": _deep_thaw_json(self.authority_scope),
            "allowed_record_kinds": list(self.allowed_record_kinds),
            "output_signature": _deep_thaw_json(self.output_signature),
            "policy_clauses": _deep_thaw_json(self.policy_clauses),
            "output_scope_summary": _deep_thaw_json(
                self.output_scope_summary
            ),
        }
        if self.write_envelope is not None:
            payload["write_envelope"] = _deep_thaw_json(self.write_envelope)
        return payload

    def raw_dict(self) -> dict[str, Any]:
        """Return a detached JSON-shaped copy of the authored policy."""
        return _deep_thaw_json(self.raw)

    @property
    def is_compat_summary_only(self) -> bool:
        return not self.output_signature


@dataclass(frozen=True)
class SourceEntry:
    """One row of source_registry.yaml after validation.

    `legacy=True` entries lack the M3 contract keys; they're kept
    for backward compatibility and skipped by the new Phase A
    executor.
    """

    name: str
    ownership_id: str
    source_class: Optional[str]   # None ⇒ legacy
    record_identity_kind: Optional[str]
    record_identity_fields: Sequence[str]
    source_revision_kind: Optional[str]
    deletion_semantics: Optional[str]
    publication_mode: Optional[str]
    credential_refs: Sequence[str]
    credential_aliases: Mapping[str, Sequence[str]]
    required_for_baseline: bool
    raw: dict[str, Any] = field(repr=False)
    record_authority_format: str = "legacy_v1"
    sync: SourceSyncCapabilities | None = None
    admission_policy: SourceAdmissionPolicy | None = None
    admission_contract: str = LEGACY_ADMISSION_CONTRACT
    # `enabled_by_default` (per `streamline-academic-pi-install`):
    # marks whether the source can run successfully using only the
    # profile's init answers (ORCID, deployment_name). Sources that
    # need operator-provided config (OAuth tokens, paths, allowlists,
    # URLs) are `False`. Bootstrap-time first publish iterates only
    # the `True` set. Missing field reads as `False` (conservative).
    enabled_by_default: bool = False

    @property
    def legacy(self) -> bool:
        return self.source_class is None

    @property
    def is_registry_only(self) -> bool:
        return self.source_class == REGISTRY_ONLY

    def sync_capabilities_for(
        self,
        adapter: Any,
    ) -> SourceSyncCapabilities | None:
        """Merge adapter defaults with registry-declared sync overrides."""
        return merge_source_sync_capabilities(
            source_sync_capabilities_from_adapter(adapter),
            self.raw.get("sync"),
        )


@dataclass(frozen=True)
class SourceRegistryAdmission:
    """One fully admitted registry document and its immutable source rows."""

    contract: str
    entries: Mapping[str, SourceEntry]
    registry_path: Path

    @property
    def strict(self) -> bool:
        return self.contract == STRICT_ADMISSION_CONTRACT


def _deep_freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({
            key: _deep_freeze(item) for key, item in value.items()
        })
    if isinstance(value, list | tuple):
        return tuple(_deep_freeze(item) for item in value)
    if isinstance(value, set | frozenset):
        return frozenset(_deep_freeze(item) for item in value)
    return value


def _deep_thaw_json(value: Any) -> Any:
    """Return a recursively detached, JSON-serializable value."""
    if isinstance(value, Mapping):
        return {
            key: _deep_thaw_json(item) for key, item in value.items()
        }
    if isinstance(value, list | tuple):
        return [_deep_thaw_json(item) for item in value]
    if isinstance(value, set | frozenset):
        return [
            _deep_thaw_json(item)
            for item in sorted(value, key=repr)
        ]
    return value


def _freeze_admission_policy(
    policy: SourceAdmissionPolicy | None,
) -> SourceAdmissionPolicy | None:
    if policy is None:
        return None
    return SourceAdmissionPolicy(
        producer_id=policy.producer_id,
        producer_kind=policy.producer_kind,
        trust_label=policy.trust_label,
        admission_mode=policy.admission_mode,
        authority_scope=_deep_freeze(policy.authority_scope),
        allowed_record_kinds=tuple(policy.allowed_record_kinds),
        output_signature=_deep_freeze(policy.output_signature),
        policy_clauses=tuple(
            _deep_freeze(clause) for clause in policy.policy_clauses
        ),
        output_scope_summary=_deep_freeze(policy.output_scope_summary),
        write_envelope=(
            _deep_freeze(policy.write_envelope)
            if policy.write_envelope is not None
            else None
        ),
        raw=_deep_freeze(policy.raw),
    )


def _freeze_source_entry(entry: SourceEntry) -> SourceEntry:
    return SourceEntry(
        name=entry.name,
        ownership_id=entry.ownership_id,
        source_class=entry.source_class,
        record_identity_kind=entry.record_identity_kind,
        record_identity_fields=tuple(entry.record_identity_fields),
        source_revision_kind=entry.source_revision_kind,
        deletion_semantics=entry.deletion_semantics,
        publication_mode=entry.publication_mode,
        record_authority_format=entry.record_authority_format,
        credential_refs=tuple(entry.credential_refs),
        credential_aliases=MappingProxyType({
            key: tuple(values)
            for key, values in entry.credential_aliases.items()
        }),
        required_for_baseline=entry.required_for_baseline,
        raw=_deep_freeze(entry.raw),
        sync=entry.sync,
        admission_policy=_freeze_admission_policy(entry.admission_policy),
        enabled_by_default=entry.enabled_by_default,
        admission_contract=STRICT_ADMISSION_CONTRACT,
    )


def source_entry_description(entry: SourceEntry) -> dict[str, Any]:
    """A plain, picklable description of one admitted registry row.

    A strict entry is frozen: its policy fields are read-only mapping views,
    which cannot be pickled, so the entry itself cannot cross into a
    ``spawn`` child process. The child rebuilds it from this description
    with :func:`source_entry_from_description`.
    """
    return {
        "name": entry.name,
        "raw": _deep_thaw_json(entry.raw),
        "strict": entry.admission_contract == STRICT_ADMISSION_CONTRACT,
    }


def source_entry_from_description(description: Mapping[str, Any]) -> SourceEntry:
    """Rebuild the registry row :func:`source_entry_description` described.

    Uses the same parse and freeze steps registry admission uses, so the
    rebuilt entry equals the admitted one.
    """
    strict = bool(description["strict"])
    entry = _entry_from_dict(
        str(description["name"]),
        dict(description["raw"]),
        strict=strict,
    )
    return _freeze_source_entry(entry) if strict else entry


def _entry_from_dict(
    name: str,
    d: dict[str, Any],
    *,
    strict: bool = False,
) -> SourceEntry:
    if "ownership_id" not in d:
        raise ValueError(
            f"source {name!r}: missing required key 'ownership_id'"
        )
    sc = d.get("source_class")
    if sc is not None and sc not in PROFILE_NAMES:
        raise ValueError(
            f"source {name!r}: unknown source_class {sc!r}; "
            f"valid profiles: {sorted(PROFILE_NAMES)}"
        )
    fields = d.get("record_identity_fields", []) or []
    if not isinstance(fields, list) or not all(
        isinstance(x, str) for x in fields
    ):
        raise ValueError(
            f"source {name!r}: record_identity_fields must be a list of "
            f"strings, got {fields!r}"
        )
    credential_refs = d.get("credential_refs", []) or []
    if not isinstance(credential_refs, list) or not all(
        isinstance(x, str) for x in credential_refs
    ):
        raise ValueError(
            f"source {name!r}: credential_refs must be a list of "
            f"strings, got {credential_refs!r}"
        )
    credential_aliases = d.get("credential_aliases", {}) or {}
    if not isinstance(credential_aliases, dict):
        raise ValueError(
            f"source {name!r}: credential_aliases must be a mapping, "
            f"got {type(credential_aliases).__name__}"
        )
    normalized_aliases: dict[str, list[str]] = {}
    for canonical, aliases in credential_aliases.items():
        if not isinstance(canonical, str):
            raise ValueError(
                f"source {name!r}: credential_aliases keys must be "
                f"strings, got {canonical!r}"
            )
        if not isinstance(aliases, list) or not all(
            isinstance(x, str) for x in aliases
        ):
            raise ValueError(
                f"source {name!r}: credential_aliases[{canonical!r}] "
                f"must be a list of strings, got {aliases!r}"
            )
        normalized_aliases[canonical] = list(aliases)
    required_for_baseline = d.get("required_for_baseline", False)
    if not isinstance(required_for_baseline, bool):
        raise ValueError(
            f"source {name!r}: required_for_baseline must be a bool, "
            f"got {required_for_baseline!r}"
        )
    enabled_by_default = d.get("enabled_by_default", False)
    if not isinstance(enabled_by_default, bool):
        raise ValueError(
            f"source {name!r}: enabled_by_default must be a bool, "
            f"got {enabled_by_default!r}"
        )
    record_authority_format = d.get(
        "record_authority_format", "legacy_v1"
    )
    if (
        not isinstance(record_authority_format, str)
        or record_authority_format not in {"legacy_v1", "indexed_v2"}
    ):
        raise ValueError(
            f"source {name!r}: record_authority_format must be one of "
            "['indexed_v2', 'legacy_v1']"
        )
    admission_policy = _admission_policy_from_dict(
        name,
        d.get("admission_policy"),
        fallback_producer_id=str(d["ownership_id"]),
        strict=strict,
    )

    return SourceEntry(
        name=name,
        ownership_id=str(d["ownership_id"]),
        source_class=sc,
        record_identity_kind=d.get("record_identity_kind"),
        record_identity_fields=list(fields),
        source_revision_kind=d.get("source_revision_kind"),
        deletion_semantics=d.get("deletion_semantics"),
        publication_mode=d.get("publication_mode"),
        record_authority_format=str(record_authority_format),
        credential_refs=list(credential_refs),
        credential_aliases=normalized_aliases,
        required_for_baseline=required_for_baseline,
        enabled_by_default=enabled_by_default,
        sync=SourceSyncCapabilities.from_dict(d.get("sync")),
        admission_policy=admission_policy,
        raw=d,
    )


def _admission_policy_from_dict(
    source_name: str,
    raw: Any,
    *,
    fallback_producer_id: str,
    strict: bool = False,
) -> SourceAdmissionPolicy | None:
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ValueError(
            f"source {source_name!r}: admission_policy must be a mapping, "
            f"got {type(raw).__name__}"
        )
    allowed = {
        "producer_id",
        "producer_kind",
        "trust_label",
        "admission_mode",
        "authority_scope",
        "allowed_record_kinds",
        "output_signature",
        "policy_clauses",
        "output_scope_summary",
        "write_envelope",
    }
    extras = sorted(set(raw) - allowed)
    if extras:
        raise ValueError(
            f"source {source_name!r}: admission_policy has unknown keys "
            f"{extras!r}"
        )
    producer_id = str(raw.get("producer_id") or fallback_producer_id)
    producer_kind = str(raw.get("producer_kind") or "source")
    if producer_kind not in _PRODUCER_KINDS:
        raise ValueError(
            f"source {source_name!r}: admission_policy.producer_kind "
            f"must be one of {sorted(_PRODUCER_KINDS)}, got "
            f"{producer_kind!r}"
        )
    trust_label = str(raw.get("trust_label") or "").strip()
    if not trust_label:
        raise ValueError(
            f"source {source_name!r}: admission_policy.trust_label "
            "is required"
        )
    admission_mode = str(raw.get("admission_mode") or "fast_track")
    if admission_mode not in _ADMISSION_MODES:
        raise ValueError(
            f"source {source_name!r}: admission_policy.admission_mode "
            f"must be one of {sorted(_ADMISSION_MODES)}, got "
            f"{admission_mode!r}"
        )
    output_scope = raw.get("output_scope_summary") or {}
    if isinstance(output_scope, str):
        output_scope = {"summary": output_scope}
    if not isinstance(output_scope, dict):
        raise ValueError(
            f"source {source_name!r}: "
            "admission_policy.output_scope_summary must be a mapping "
            f"or string, got {type(output_scope).__name__}"
        )
    authority_scope = normalize_authority_scope(
        raw.get("authority_scope"),
        source_name=source_name,
    )
    output_signature = normalize_output_signature(
        raw.get("output_signature"),
        source_name=source_name,
        strict=strict,
    )
    allowed_record_kinds = normalize_allowed_record_kinds(
        raw.get("allowed_record_kinds"),
        output_signature=output_signature,
        source_name=source_name,
    )
    policy_clauses = normalize_policy_clauses(
        raw.get("policy_clauses"),
        source_name=source_name,
        strict=strict,
    )
    # Write envelope (spec §9.3): an explicit `write_envelope` block wins and
    # is normalized loudly (use it for a genuinely unbounded source). Otherwise
    # DERIVE from the declared output_signature — the node subtypes + edge
    # triples the source already declares, deterministic (a source adapter is
    # not a model call). This is declaration-derived, never output-inferred,
    # and duplicates nothing. A compat-summary-only source (no signature, no
    # explicit envelope) yields no envelope; the gap is surfaced by the
    # `producer.envelope_missing` lint, not invented here.
    output_scope = dict(output_scope)
    write_envelope_summary: dict[str, Any] | None = None
    raw_envelope = raw.get("write_envelope")
    if raw_envelope is not None:
        envelope = normalize_write_envelope(
            raw_envelope,
            source_name=source_name,
            strict=strict,
        )
        write_envelope_summary = envelope.summary()
    elif output_signature:
        envelope = normalize_write_envelope(
            {**output_signature, "nondeterministic": False},
            source_name=source_name,
            strict=strict,
        )
        write_envelope_summary = envelope.summary()
    if write_envelope_summary is not None:
        # Ride the profile's scope summary so materialize_producer_profile
        # persists it and profile_write_envelope() recovers it — the same
        # mechanism the enricher path uses, ZERO evaluator changes.
        output_scope["write_envelope"] = write_envelope_summary
    return SourceAdmissionPolicy(
        producer_id=producer_id,
        producer_kind=producer_kind,
        trust_label=trust_label,
        admission_mode=admission_mode,
        authority_scope=authority_scope,
        allowed_record_kinds=allowed_record_kinds,
        output_signature=output_signature,
        policy_clauses=policy_clauses,
        output_scope_summary=output_scope,
        write_envelope=write_envelope_summary,
        raw=dict(raw),
    )


# Module-level cache keyed by (absolute path, mtime_ns). Worker
# processes call `load_source_registry` on every `run_ingest`
# entry; the YAML parse cost is ~22 ms (≈ half the per-cycle wall
# in steady state). The registry file is immutable across a worker's
# lifetime — caching by `(path, mtime)` makes the second-and-later
# calls a dict lookup.
#
# Callers MUST NOT mutate the returned dict (a mutation would
# persist for every subsequent caller). All current callers iterate
# / read only; the API is `dict[str, SourceEntry]` and the entries
# themselves are frozen dataclasses.
#
# Race-window note: two concurrent first-time callers may both
# parse + write, with the second write winning. The result is
# correctness-equivalent (the file content drives both parses) but
# wastes one parse. Locking would cost more than the rare race.
_REGISTRY_CACHE: dict[
    tuple[str, int, str | None], SourceRegistryAdmission
] = {}


# Canonical per-repo code-source template (single source of truth for the
# git-files + code-structure block shape). `code_repos:` expansion renders
# its `git-files` and `code-structure` entries once per declared repo.
# Params that identify one repo (set by the expansion from the `repos` entry);
# `code_repos.params` applies to every repo, so it may not set these.
_CODE_REPO_PER_REPO_PARAMS = frozenset({
    "repo", "repo_root", "url", "manage_checkout", "tracked_branch",
})
_CODE_REPO_TEMPLATE_PATH = (
    Path(__file__).resolve().parent.parent
    / "library" / "templates" / "codebase-index" / "source_registry.yaml"
)


def _render_code_repo_block(text: str, variables: dict[str, str]) -> str:
    """Minimal ``{{ var }}`` + ``{{ if v }}..{{ endif }}`` renderer for the
    codebase-index source template. Mirrors ``cli._render_template_text``,
    kept local so the registry loader has no dependency on the CLI module.
    """

    def _truthy(value: object) -> bool:
        if isinstance(value, str):
            return value.strip().lower() not in {"", "0", "false", "no"}
        return bool(value)

    out: list[str] = []
    stack: list[bool] = []
    for line in text.splitlines(keepends=True):
        stripped = line.strip()
        m = re.fullmatch(r"(?:#\s*)?\{\{\s*if\s+([a-zA-Z_]\w*)\s*\}\}", stripped)
        if m:
            stack.append(_truthy(variables.get(m.group(1))))
            continue
        m = re.fullmatch(
            r"(?:#\s*)?\{\{\s*endif(?:\s+[a-zA-Z_]\w*)?\s*\}\}", stripped
        )
        if m:
            if stack:
                stack.pop()
            continue
        if all(stack):
            out.append(line)
    if stack:
        raise ValueError(
            "code_repos template has an unbalanced {{ if }} block"
        )
    rendered = "".join(out)

    def _sub(m: re.Match[str]) -> str:
        key = m.group(1)
        return str(variables[key]) if key in variables else m.group(0)

    rendered = re.sub(r"\{\{\s*([a-zA-Z_]\w*)\s*\}\}", _sub, rendered)
    # Legacy `__OKG_*__` sentinels (mirror cli._LEGACY_TEMPLATE_KEYS) — the
    # codebase-index template still uses __OKG_REPO_ROOT__ for sync.watch roots.
    for sentinel, var_key in (
        ("__OKG_REPO_ROOT__", "repo_root"),
        ("__OKG_REPO_NAME__", "repo_name"),
    ):
        rendered = rendered.replace(sentinel, str(variables.get(var_key, "")))
    return rendered


def _expand_code_repos(
    code_repos: dict, registry_path: Path,
) -> dict[str, dict]:
    """Expand a ``code_repos: {base, repos: [{slug, url}], expand: [...]}`` block
    into per-repo source bodies, rendered from the canonical codebase-index
    template. ``expand`` lists which template blocks to generate per repo and
    defaults to ``[git-files, code-structure]``; add e.g. ``interfaces`` /
    ``doc-corpus`` to index those for every repo. Each block ``B`` becomes a
    source named ``B-<slug>``.

    Dependencies: blocks that build on others (``code-structure`` needs
    ``git-files``; ``interfaces`` needs both; ``doc-corpus`` needs ``git-files``)
    reference their siblings as predecessors. If you list a dependent block,
    list its dependencies too — otherwise the predecessor dangles (surfaces as a
    ``predecessor_not_ready`` doctor warning at ingest, not a load error).

    Checkout acquisition: when a repo declares a ``url``, the generated
    ``git-files-<slug>`` source (the checkout owner) is rendered with
    ``manage_checkout: true`` plus that ``url`` (and optional ``tracked_branch``),
    so ``okg ingest``/``okg up`` clone-if-missing / fast-forward-pull it before
    reading — no separate clone step. Set a repo's ``manage_checkout: false`` to
    opt out. Dependent blocks (code-structure/interfaces/doc-corpus) inherit the
    owner's checkout and are NOT given manage_checkout.

    Param overrides: ``params: {<block>: {<param>: value}}`` sets a param of
    block ``<block>`` for every repo (e.g. wider ``doc-corpus`` include globs),
    replacing the template's value. The block must be in ``expand``; per-repo
    keys (``repo``, ``repo_root``, ``url``, ``manage_checkout``,
    ``tracked_branch``) are rejected. A ``repos`` entry may carry its own
    ``params`` of the same shape, applied to that repository only after the
    block-wide ones (see ``_code_repo_entry_params``).
    """
    import os

    repos = code_repos.get("repos") or []
    if not repos:
        return {}
    # `expand` absent -> default blocks; an explicit `expand: []` is honoured as
    # "render no per-repo blocks" (not silently replaced by the defaults).
    blocks = code_repos.get("expand")
    if blocks is None:
        blocks = ["git-files", "code-structure"]
    if not isinstance(blocks, list) or not all(
        isinstance(b, str) and b.strip() for b in blocks
    ):
        raise ValueError(
            f"{registry_path}: code_repos.expand must be a list of source-block "
            f"names (e.g. [git-files, code-structure, interfaces, doc-corpus])"
        )
    base_raw = str(code_repos.get("base") or "").strip()
    if not base_raw:
        raise ValueError(f"{registry_path}: code_repos requires a 'base' path")
    base = Path(os.path.expanduser(os.path.expandvars(base_raw)))
    if not base.is_absolute():
        raise ValueError(
            f"{registry_path}: code_repos.base must resolve to an absolute path "
            f"(got {base_raw!r} -> {base}); set an absolute path or an env var "
            f"like ${{OKG_CODE_REPOS_ROOT}}"
        )
    # Optional per-block parameter overrides applied to every repo, e.g.
    #   code_repos: {params: {doc-corpus: {include_globs: ["**/*"], text_fallback: true}}}
    # Each value replaces the template's value for that key (a list replaces
    # the template's list; nothing is merged).
    block_params = code_repos.get("params")
    if block_params is None:
        block_params = {}
    if not isinstance(block_params, dict) or not all(
        isinstance(k, str) and isinstance(v, dict) for k, v in block_params.items()
    ):
        raise ValueError(
            f"{registry_path}: code_repos.params must map a source-block name to a "
            f"mapping of parameter overrides (e.g. {{doc-corpus: {{include_globs: [...]}}}})"
        )
    unknown_blocks = sorted(set(block_params) - set(blocks))
    if unknown_blocks:
        raise ValueError(
            f"{registry_path}: code_repos.params names blocks not in code_repos.expand: "
            f"{unknown_blocks}"
        )
    per_repo = sorted(
        {key for overrides in block_params.values() for key in overrides}
        & _CODE_REPO_PER_REPO_PARAMS
    )
    if per_repo:
        raise ValueError(
            f"{registry_path}: code_repos.params cannot set per-repo keys {per_repo}; "
            f"set them on the repos entry instead"
        )
    template_text = _CODE_REPO_TEMPLATE_PATH.read_text(encoding="utf-8")
    expanded: dict[str, dict] = {}
    seen_slugs: set[str] = set()
    base_resolved = base.resolve(strict=False)
    for repo in repos:
        if not isinstance(repo, dict):
            raise ValueError(
                f"{registry_path}: each code_repos entry must be a mapping"
            )
        slug = str(repo.get("slug") or "").strip()
        if not slug:
            raise ValueError(
                f"{registry_path}: each code_repos entry needs a non-empty 'slug'"
            )
        if slug in seen_slugs:
            raise ValueError(
                f"{registry_path}: duplicate code_repos slug {slug!r}"
            )
        seen_slugs.add(slug)
        # The on-disk checkout dir often differs from the slug (e.g. slug
        # `wmcore` -> dir `WMCore`). Derive it from `dir` (explicit override),
        # else the URL basename (`.../WMCore.git` -> `WMCore`), else the slug.
        # `slug` remains the node/source identity; `dir` is only the path.
        dir_name = str(repo.get("dir") or "").strip()
        if not dir_name:
            url = str(repo.get("url") or "").strip().rstrip("/")
            if url:
                dir_name = url.rsplit("/", 1)[-1]
                if dir_name.endswith(".git"):
                    dir_name = dir_name[: -len(".git")]
            dir_name = dir_name or slug
        dir_path = Path(dir_name)
        if (
            dir_path.is_absolute()
            or dir_name in {"", "."}
            or any(part == ".." for part in dir_path.parts)
        ):
            raise ValueError(
                f"{registry_path}: code_repos entry {slug!r} dir must be a "
                f"relative path under base (got {dir_name!r})"
            )
        repo_root = (base / dir_path).resolve(strict=False)
        try:
            repo_root.relative_to(base_resolved)
        except ValueError:
            raise ValueError(
                f"{registry_path}: code_repos entry {slug!r} dir must stay "
                f"under base {base_resolved} (got {dir_name!r} -> {repo_root})"
            ) from None
        rendered = _render_code_repo_block(
            template_text,
            {
                "deployment_name": slug,
                "repo_name": dir_name,
                "repo_root": str(repo_root),
                "deployment_dir": str(registry_path.parent),
                # conditional-only sources (openspec/notes) stay off here.
                "openspec_enabled": "",
                "notes_root": "",
                "openspec_root": "",
                "since": "",
            },
        )
        # Checkout-acquisition params for the owner (git-files). manage_checkout
        # defaults on when a url is present; a repo may opt out per-repo.
        repo_params = _code_repo_entry_params(
            repo.get("params"), slug=slug, blocks=blocks,
            registry_path=registry_path,
        )
        repo_url = str(repo.get("url") or "").strip()
        tracked_branch = str(repo.get("tracked_branch") or "").strip()
        manage_checkout = bool(repo.get("manage_checkout", True)) and bool(repo_url)
        rendered_sources = (fastyaml.safe_load(rendered) or {}).get("sources") or {}
        for tmpl_key in blocks:
            body = rendered_sources.get(tmpl_key)
            if not isinstance(body, dict):
                raise ValueError(
                    f"{registry_path}: code_repos.expand lists {tmpl_key!r}, but "
                    f"the codebase-index template provides no such source block"
                )
            # Only the git-files source owns the checkout; dependents inherit it.
            # The url itself flows to git-files/code-structure/doc-corpus
            # regardless of manage_checkout — it also feeds the stored
            # citation urls on emitted nodes (citation-source-urls).
            if tmpl_key == "git-files" and repo_url:
                body = dict(body)
                params = dict(body.get("params") or {})
                params["url"] = repo_url
                if manage_checkout:
                    params["manage_checkout"] = True
                if tracked_branch:
                    params["tracked_branch"] = tracked_branch
                body["params"] = params
            elif tmpl_key in ("code-structure", "doc-corpus") and repo_url:
                # Citation URLs (citation-source-urls): dependent blocks
                # get the repo url (NOT manage_checkout — the checkout
                # still has a single owner) so emitted code_symbol /
                # document / document_chunk nodes carry a stored web url.
                body = dict(body)
                params = dict(body.get("params") or {})
                params["url"] = repo_url
                if tracked_branch:
                    params["tracked_branch"] = tracked_branch
                body["params"] = params
            # Block-wide overrides first, then this repo's own (which win).
            for overrides in (block_params, repo_params):
                if overrides.get(tmpl_key):
                    body = dict(body)
                    params = dict(body.get("params") or {})
                    params.update(copy.deepcopy(overrides[tmpl_key]))
                    body["params"] = params
            expanded[f"{tmpl_key}-{slug}"] = body
    return expanded


def _code_repo_entry_params(
    raw: object, *, slug: str, blocks: list[str], registry_path: Path,
) -> dict[str, dict]:
    """Validate one ``code_repos.repos[]`` entry's ``params``.

    Same shape and rules as ``code_repos.params`` (a mapping of source-block
    name to parameter overrides, blocks in ``expand``, no per-repo identity
    keys), but applied to this repository only, after the block-wide
    overrides -- e.g. ``{slug: app-config, params: {git-history:
    {store_diffs: true}}}`` turns commit diffs on for one repository.
    """
    if raw is None:
        return {}
    if not isinstance(raw, dict) or not all(
        isinstance(k, str) and isinstance(v, dict) for k, v in raw.items()
    ):
        raise ValueError(
            f"{registry_path}: code_repos entry {slug!r} params must map a "
            f"source-block name to a mapping of parameter overrides"
        )
    unknown = sorted(set(raw) - set(blocks))
    if unknown:
        raise ValueError(
            f"{registry_path}: code_repos entry {slug!r} params names blocks "
            f"not in code_repos.expand: {unknown}"
        )
    per_repo = sorted(
        {key for overrides in raw.values() for key in overrides}
        & _CODE_REPO_PER_REPO_PARAMS
    )
    if per_repo:
        raise ValueError(
            f"{registry_path}: code_repos entry {slug!r} params cannot set "
            f"{per_repo}; set them on the repos entry itself"
        )
    return raw


def _remap_predecessor_source(body: dict[str, Any], remap: dict[str, str]) -> None:
    """Rewrite a generated block's ``predecessor_source`` owner references via
    ``remap`` (template ownership_id -> the winning/overridden ownership_id).

    Handles the reference wherever the template puts it — a top-level string
    (code-structure), a top-level list (interfaces), or nested under ``params``
    (doc-corpus) — so an explicit ``git-files-*`` / ``code-structure-*`` override
    is inherited by ALL dependent blocks, not just code-structure.
    """
    def _sub(value: Any) -> Any:
        if isinstance(value, str):
            return remap.get(value, value)
        if isinstance(value, list):
            return [remap.get(v, v) if isinstance(v, str) else v for v in value]
        return value

    if "predecessor_source" in body:
        body["predecessor_source"] = _sub(body["predecessor_source"])
    params = body.get("params")
    if isinstance(params, dict) and "predecessor_source" in params:
        params["predecessor_source"] = _sub(params["predecessor_source"])


def _code_repos_with_resolvable_base(code_repos: dict) -> dict:
    """For Nomos governance enumeration (baseline seed/audit/readiness) the real
    checkout root is irrelevant — only source names, producer_ids and
    source_class feed the policy bundle, none of which depend on ``base``. When
    ``base`` cannot resolve to an absolute path (e.g. ``${OKG_CODE_REPOS_ROOT}``
    unset on an audit-only host), substitute a placeholder absolute base so the
    source set is enumerated identically to a fully-configured host (same bundle
    fingerprint) instead of raising. Genuine structural errors (bad slug, a dir
    escaping base) still surface in ``_expand_code_repos``.
    """
    base_raw = str(code_repos.get("base") or "").strip()
    expanded = os.path.expanduser(os.path.expandvars(base_raw))
    if (
        base_raw
        and _ENV_VAR_RE.search(base_raw)
        and not Path(expanded).is_absolute()
    ):
        return {**code_repos, "base": "/okg-code-repos-unresolved"}
    return code_repos


def runtime_source_subject_ids(sources: dict[str, Any]) -> set[str]:
    """Identities that runtime source-admission may present as the emit subject
    for an (already code_repos-expanded) ``sources`` mapping: each source's name
    PLUS its ``producer_id`` / ``ownership_id``. For most sources these coincide;
    a code_repos source is named ``git-files-<slug>`` while its runtime
    producer_id is ``<slug>.git-files``, so the baseline emit rule must include
    both or that source's ``source.emit`` falls through to ``default_deny``.
    """
    ids: set[str] = set()
    for name, entry in (sources or {}).items():
        if str(name).strip():
            ids.add(str(name))
        if not isinstance(entry, dict):
            continue
        ownership = entry.get("ownership_id")
        if isinstance(ownership, str) and ownership.strip():
            ids.add(ownership.strip())
        admission = entry.get("admission_policy")
        if isinstance(admission, dict):
            producer = admission.get("producer_id")
            if isinstance(producer, str) and producer.strip():
                ids.add(producer.strip())
    return ids


def expand_code_repos_sources(
    doc: dict[str, Any],
    registry_path: Path,
    *,
    allow_unresolved_base: bool = False,
    substitute_placeholders: bool = True,
    repo_root_override: Path | str | None = None,
) -> dict[str, Any]:
    """Return the effective ``sources`` mapping for a raw ``source_registry``
    document, with any ``code_repos:`` block expanded into per-repo
    git-files/code-structure/interfaces/doc-corpus sources and merged in. An
    explicit ``sources:`` entry of the same name always wins.

    Shared by the registry loader and every Nomos baseline seed site so they can
    never disagree about which sources a deployment actually has. (Historically
    the seed enumerated only the literal ``sources:`` block, so every
    ``code_repos``-generated source was invisible to Nomos and hit
    ``default_deny`` at source admission — no code graph could publish.)

    ``allow_unresolved_base`` is for governance-only callers (audit/readiness on
    hosts without ``OKG_CODE_REPOS_ROOT``): the source set is still enumerated
    (identically, same fingerprint) rather than raising. The registry loader
    leaves it False so a misconfigured base fails loudly at ingest.
    """
    if substitute_placeholders:
        doc = _substitute_registry_placeholders(
            doc,
            registry_path,
            repo_root_override=repo_root_override,
        )
    raw = doc.get("sources") or {}
    if not isinstance(raw, dict):
        raise ValueError(
            f"{registry_path}: top-level 'sources' must be a mapping, got "
            f"{type(raw).__name__}"
        )
    raw_sources: dict[str, Any] = dict(raw)

    # `code_repos:` is an optional directive that expands into per-repo
    # git-files/code-structure sources. It is additive: an explicit entry
    # of the same name in `sources:` always wins.
    code_repos = doc.get("code_repos")
    if isinstance(code_repos, dict) and code_repos.get("repos"):
        if allow_unresolved_base:
            code_repos = _code_repos_with_resolvable_base(code_repos)
        expanded = _expand_code_repos(code_repos, registry_path)
        # When an explicit `sources:` entry overrides a generated git-files-* /
        # code-structure-* owner, its ownership_id differs from the template
        # default the other generated blocks reference. Build a
        # template-owner -> winning-owner map so every dependent block
        # (code-structure, interfaces, doc-corpus) inherits the real owner.
        owner_remap: dict[str, str] = {}
        for name, gen_body in expanded.items():
            if not (
                name.startswith("git-files-")
                or name.startswith("code-structure-")
            ):
                continue
            gen_owner = gen_body.get("ownership_id")
            winner = raw_sources.get(name, gen_body)
            winner_owner = (
                winner.get("ownership_id") if isinstance(winner, dict) else None
            )
            if gen_owner and winner_owner and str(gen_owner) != str(winner_owner):
                owner_remap[str(gen_owner)] = str(winner_owner)
        # Insert generated blocks; an explicit entry of the same name wins.
        for name, gen_body in expanded.items():
            if name in raw_sources:
                continue
            if owner_remap:
                _remap_predecessor_source(gen_body, owner_remap)
            raw_sources[name] = gen_body
    return raw_sources


def _entries_from_registry_document(
    doc: Mapping[str, Any],
    path: Path,
    *,
    repo_root_override: Path | str | None,
    strict: bool,
) -> dict[str, SourceEntry]:
    raw_sources = expand_code_repos_sources(
        dict(doc),
        path,
        repo_root_override=repo_root_override,
    )
    if strict:
        _validate_strict_schema(
            {
                "admission_contract": STRICT_ADMISSION_CONTRACT,
                "sources": raw_sources,
            },
            path,
        )
    out: dict[str, SourceEntry] = {}
    retired_schedule: list[str] = []
    ownership_ids: dict[str, str] = {}
    producer_ids: dict[str, str] = {}
    for name, body in raw_sources.items():
        if not isinstance(body, dict):
            raise ValueError(
                f"{path}: source {name!r} body must be a mapping, got "
                f"{type(body).__name__}"
            )
        if not strict:
            validate_legacy_entry_keys(name, body, path)
            if "schedule" in body:
                retired_schedule.append(str(name))
        entry = _entry_from_dict(name, body, strict=strict)
        if not entry.legacy:
            try:
                validate_profile_tuple(
                    source_class=entry.source_class,
                    record_identity_kind=entry.record_identity_kind,
                    source_revision_kind=entry.source_revision_kind,
                    deletion_semantics=entry.deletion_semantics,
                    publication_mode=entry.publication_mode,
                )
            except ValueError as e:
                raise ValueError(
                    f"{path}: source {name!r} fails profile validation: {e}"
                ) from None
            # registry_only sources MUST NOT carry record_identity_fields
            # (they have no runnable factory). Allow but don't require.
        if strict:
            prior_owner = ownership_ids.get(entry.ownership_id)
            if prior_owner is not None:
                raise ValueError(
                    f"{path}: sources {prior_owner!r} and {name!r} have "
                    f"duplicate ownership_id {entry.ownership_id!r}"
                )
            ownership_ids[entry.ownership_id] = name
            producer_id = (
                entry.admission_policy.producer_id
                if entry.admission_policy is not None
                else entry.ownership_id
            )
            prior_producer = producer_ids.get(producer_id)
            if prior_producer is not None:
                raise ValueError(
                    f"{path}: sources {prior_producer!r} and {name!r} have "
                    f"duplicate producer_id {producer_id!r}"
                )
            producer_ids[producer_id] = name
            entry = _freeze_source_entry(entry)
        out[name] = entry
    if retired_schedule:
        _warn_retired_schedule(path, sorted(retired_schedule))
    return out


def _validate_strict_schema(
    document: Mapping[str, Any],
    registry_path: Path,
) -> None:
    schema = json.loads(
        (schemas_dir() / "source_registry.schema.json").read_text(encoding="utf-8")
    )
    errors = sorted(
        Draft202012Validator(schema).iter_errors(document),
        key=lambda error: tuple(str(part) for part in error.absolute_path),
    )
    if not errors:
        return
    error = errors[0]
    parts = tuple(str(part) for part in error.absolute_path)
    location = ".".join(parts) if parts else "<document>"
    hint = ""
    if error.validator == "additionalProperties" and "fixture_path" in str(
        error.message
    ):
        hint = "; move fixture_path under params.fixture_path"
    elif (
        error.validator == "additionalProperties"
        and len(parts) == 2
        and parts[0] == "sources"
        and "'schedule'" in str(error.message)
    ):
        # Issue #2952: `okg deployment bootstrap` used to draft this key, and a
        # registry already on strict_v1 keeps it. Name exactly what to delete.
        hint = (
            f"; delete the `schedule` block of source {parts[1]!r} "
            f"(key sources.{parts[1]}.schedule) from {registry_path}. "
            "Nothing reads it; a scheduled refresh is the source's "
            "`sync.poll` block"
        )
    raise ValueError(
        "source_registry_strict_schema: "
        f"registry={registry_path} path={location}: {error.message}{hint}"
    )


def _validate_strict_semantics(
    document: Mapping[str, Any],
    registry_path: Path,
) -> None:
    code_repos = document.get("code_repos")
    if isinstance(code_repos, Mapping):
        seen_slugs: set[str] = set()
        for index, repo in enumerate(code_repos.get("repos", ())):
            if not isinstance(repo, Mapping):
                continue
            slug = str(repo.get("slug") or "").strip()
            if slug in seen_slugs:
                raise ValueError(
                    f"{registry_path}: duplicate code_repos slug {slug!r} "
                    f"at repos[{index}]"
                )
            seen_slugs.add(slug)
    sources = document.get("sources")
    if not isinstance(sources, Mapping):
        return
    for name, body in sources.items():
        if not isinstance(body, Mapping):
            continue
        fields = body.get("record_identity_fields")
        if isinstance(fields, list) and len(set(fields)) != len(fields):
            raise ValueError(
                f"{registry_path}: source {name!r} repeats "
                "record_identity_fields"
            )
        for key in ("credential_refs",):
            values = body.get(key)
            if isinstance(values, list) and len(set(values)) != len(values):
                raise ValueError(
                    f"{registry_path}: source {name!r} repeats {key}"
                )
        aliases = body.get("credential_aliases")
        if isinstance(aliases, Mapping):
            for canonical, values in aliases.items():
                if isinstance(values, list) and len(set(values)) != len(values):
                    raise ValueError(
                        f"{registry_path}: source {name!r} repeats "
                        f"credential_aliases[{canonical!r}]"
                    )
        policy = body.get("admission_policy")
        if isinstance(policy, Mapping):
            clauses = policy.get("policy_clauses")
            if isinstance(clauses, list):
                clause_ids = [
                    str(clause.get("id"))
                    for clause in clauses
                    if isinstance(clause, Mapping)
                ]
                if len(set(clause_ids)) != len(clause_ids):
                    raise ValueError(
                        f"{registry_path}: source {name!r} has duplicate "
                        "admission_policy.policy_clauses id"
                    )


def _validate_strict_json_values(
    value: Any,
    registry_path: Path,
    *,
    path: str = "",
) -> None:
    """Reject YAML scalars that cannot cross strict JSON boundaries."""
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                location = path or "<document>"
                raise ValueError(
                    "source_registry_strict_json: "
                    f"registry={registry_path} path={location}: "
                    "mapping keys must be strings, got "
                    f"{type(key).__name__}"
                )
            child_path = f"{path}.{key}" if path else key
            _validate_strict_json_values(
                item,
                registry_path,
                path=child_path,
            )
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_strict_json_values(
                item,
                registry_path,
                path=f"{path}[{index}]",
            )
        return
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(
            "source_registry_strict_json: "
            f"registry={registry_path} path={path or '<document>'}: "
            f"non-finite JSON number {value!r}; "
            "quote the value to preserve it as a string"
        )
    if value is None or isinstance(value, str | int | float | bool):
        return
    raise ValueError(
        "source_registry_strict_json: "
        f"registry={registry_path} path={path or '<document>'}: "
        f"unsupported JSON scalar type {type(value).__name__}; "
        "quote the value to preserve it as a string"
    )


def admit_source_registry_document(
    document: Any,
    *,
    registry_path: Path,
    repo_root_override: Path | str | None = None,
) -> SourceRegistryAdmission:
    """Admit one in-memory registry through legacy or strict_v1 semantics."""
    if isinstance(document, Mapping) and "admission_contract" in document:
        marker = document.get("admission_contract")
        if marker != STRICT_ADMISSION_CONTRACT:
            raise ValueError(
                f"{registry_path}: admission_contract must be "
                f"{STRICT_ADMISSION_CONTRACT!r}, got {marker!r}"
            )
        strict_document = dict(document)
        _validate_strict_schema(strict_document, registry_path)
        _validate_strict_json_values(strict_document, registry_path)
        _validate_strict_semantics(strict_document, registry_path)
        entries = _entries_from_registry_document(
            strict_document,
            registry_path,
            repo_root_override=repo_root_override,
            strict=True,
        )
        return SourceRegistryAdmission(
            contract=STRICT_ADMISSION_CONTRACT,
            entries=MappingProxyType(entries),
            registry_path=registry_path,
        )

    # Preserve the current unmarked path, including its absent/null defaults.
    legacy_document = document or {}
    if isinstance(legacy_document, Mapping):
        validate_legacy_document_keys(legacy_document, registry_path)
    entries = _entries_from_registry_document(
        legacy_document,
        registry_path,
        repo_root_override=repo_root_override,
        strict=False,
    )
    return SourceRegistryAdmission(
        contract=LEGACY_ADMISSION_CONTRACT,
        entries=entries,
        registry_path=registry_path,
    )


def _load_source_registry_uncached(
    path: Path,
    *,
    repo_root_override: Path | str | None = None,
) -> SourceRegistryAdmission:
    with path.open("r") as f:
        document = fastyaml.safe_load(f)
    return admit_source_registry_document(
        document,
        registry_path=path,
        repo_root_override=repo_root_override,
    )


def load_source_registry_admission(
    path: str | Path,
    *,
    repo_root_override: Path | str | None = None,
    force_reload: bool = False,
) -> SourceRegistryAdmission:
    """Parse and admit one source registry with a path/mtime cache."""
    p = Path(path).resolve()
    try:
        mtime = p.stat().st_mtime_ns
    except FileNotFoundError:
        return _load_source_registry_uncached(
            Path(path),
            repo_root_override=repo_root_override,
        )
    override_key = (
        str(Path(repo_root_override).expanduser().resolve(strict=False))
        if repo_root_override is not None else None
    )
    key = (str(p), mtime, override_key)
    if not force_reload:
        cached = _REGISTRY_CACHE.get(key)
        if cached is not None:
            return cached
    admitted = _load_source_registry_uncached(
        p,
        repo_root_override=repo_root_override,
    )
    _REGISTRY_CACHE[key] = admitted
    return admitted


def load_source_registry(
    path: str | Path,
    *,
    repo_root_override: Path | str | None = None,
) -> Mapping[str, SourceEntry]:
    """Parse a source_registry.yaml; return name → SourceEntry.

    Validates each entry against the profile matrix. Legacy entries
    (no `source_class`) are returned untouched (no profile checks).

    The result is cached on `(absolute_path, mtime_ns)`. The cache
    invalidates automatically when the YAML's mtime changes; callers
    must not mutate the returned dict.
    """
    return load_source_registry_admission(
        path,
        repo_root_override=repo_root_override,
    ).entries


def validate_source_registry(path: str | Path) -> dict[str, Any]:
    """CLI-friendly entry point. Returns a summary dict.

    Raises ValueError on the first invalid entry; returns counts on
    success.
    """
    entries = load_source_registry(path)
    legacy = [n for n, e in entries.items() if e.legacy]
    profiled = [n for n, e in entries.items() if not e.legacy]
    by_profile: dict[str, list[str]] = {}
    for n, e in entries.items():
        if e.source_class:
            by_profile.setdefault(e.source_class, []).append(n)
    return {
        "total": len(entries),
        "legacy_count": len(legacy),
        "profiled_count": len(profiled),
        "by_profile": {k: sorted(v) for k, v in by_profile.items()},
    }


def _substitute_registry_placeholders(
    raw: Any,
    path: Path,
    *,
    repo_root_override: Path | str | None = None,
) -> Any:
    """Expand stable path anchors in source_registry.yaml values.

    `source_registry.yaml` is often copied across worktrees along with
    its deployment. Keeping repo paths literal makes the registry import
    stale artifacts from an older checkout. These anchors are resolved
    from the registry location so a deployment can move with the repo.
    """
    deployment_dir = path.parent.resolve()
    if repo_root_override is not None:
        repo_root = Path(repo_root_override).expanduser().resolve(strict=False)
    elif deployment_dir.parent.name == "deployments":
        repo_root = deployment_dir.parent.parent.resolve()
    else:
        repo_root = deployment_dir.parent.resolve()
    placeholders = {
        "${source_registry_dir}": str(path.parent.resolve()),
        "${deployment_dir}": str(deployment_dir),
        "${manifest_dir}": str(deployment_dir),
        "${repo_root}": str(repo_root),
        # Sealed environments keep mutable source artifacts outside the
        # read-only release. Legacy deployments retain deployment_dir until
        # their environment controller sets this explicit authority.
        "${environment_data_root}": os.environ.get(
            "OKG_ENVIRONMENT_DATA_ROOT", str(deployment_dir)
        ),
    }
    return _substitute_node(raw, placeholders)


def _substitute_node(node: Any, placeholders: dict[str, str]) -> Any:
    if isinstance(node, str):
        out = node
        for needle, replacement in placeholders.items():
            out = out.replace(needle, replacement)
        # Then expand ${ENV_VAR} from the environment, so a registry stays
        # portable across machines (e.g. repo paths via ${OKG_CODE_REPOS_ROOT}).
        # Undefined vars are left literal so a missing one fails loudly
        # downstream rather than silently resolving to an empty path.
        out = _ENV_VAR_RE.sub(
            lambda m: os.environ.get(m.group(1), m.group(0)), out
        )
        return out
    if isinstance(node, dict):
        return {k: _substitute_node(v, placeholders) for k, v in node.items()}
    if isinstance(node, list):
        return [_substitute_node(v, placeholders) for v in node]
    return node
