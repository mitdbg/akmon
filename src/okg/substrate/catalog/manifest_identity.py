"""Manifest content-hash for deployment publish identity (#709).

A deployment's publish identity historically covered only the resolved
directory path plus the composed ontology fingerprint -- never the
manifest blocks that decide *what publishes* (enrichers, extractor,
maintained_derivations, source_registry, ...). Flipping the worktree to
a branch carrying a retired manifest was therefore absorbed as "config
changed" and published silently (#709, the remediate-audit-2026-07
umbrella's input-closure disease at deployment scope).

This module computes a ``manifest_fingerprint`` over exactly the
publisher-stack + source-sync input closure of ``deployment.yaml`` and
the sibling config files that stack loads. The coverage is a partition
of every known top-level manifest key into ``IN`` (behavior-bearing) and
``OUT`` (ontology / ops / read-side), pinned mechanically by a census
coupling test: when a new top-level key is added to
``deployments._KNOWN_TOP_LEVEL_KEYS`` it must be classified here or the
test fails, so the coverage maintains itself instead of rotting into a
frozen list.

Design ruling R1 (pact/changes/bind-deployment-identity/reports/
design-review.md):
  IN  -- source_registry (hashed as the resolved file's content),
         enrichers, extractor, derived_producers, maintained_derivations,
         consistency, consistency_gates, ir_consistency_checks,
         post_publish_consistency_checks, lenses, nomos, sync_root,
         embeddings/embedding, native_views; from ``runtime`` only
         ``workflows`` and ``source_sync``; plus sibling config files by
         content (enricher_registry, taxonomy, aliases, link_config,
         alias_resolver).
  OUT -- ontology inputs (modules, modules_config, schema_dir -- already
         the catalog_fingerprint's job; no double-count), postgres*/name/
         description/profile, read-side operator surfaces, ops blocks.
  Deployment-owned ``.py`` code is excluded (git provenance records it).
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from okg.substrate.errors import SubstrateError


class ManifestFingerprintError(SubstrateError):
    """A present deployment manifest could not be read for fingerprinting.

    Raised only when a manifest file exists but cannot be parsed or a
    declared sibling config file cannot be read -- error-reading-a-present
    source is a hard failure, never a benign default (umbrella no-benign-
    defaults rule; ruling R3). Absence of a manifest file is *not* an
    error: it returns ``(None, {})`` so pathless/legacy callers stay
    permissive until a stamped ownership row arms enforcement.
    """


# Behavior-bearing publisher-stack + source-sync closure (ruling R1 IN).
# ``runtime`` is partial: only ``workflows`` + ``source_sync`` are hashed.
MANIFEST_FINGERPRINT_IN: frozenset[str] = frozenset({
    "source_registry",
    "checkpoint_attestors",
    "checkpoint_attestor_root",
    "enrichers",
    "enricher_registry",
    "extractor",
    "derived_producers",
    "maintained_derivations",
    "consistency",
    "consistency_gates",
    # Which producer's version of a shared edge the graph stores (item 92a).
    "edge_precedence",
    "ir_consistency_checks",
    "post_publish_consistency_checks",
    "lenses",
    "nomos",
    "sync_root",
    "embeddings",
    "embedding",
    "native_views",
    "taxonomy",
    "aliases",
    "link_config",
    "alias_resolver",
    "runtime",  # partial: only workflows + source_sync (see _hash_block)
})

# Ontology (covered by catalog_fingerprint), ops, and read-side surfaces
# (ruling R1 OUT). A branch flip that changes only these never refuses.
MANIFEST_FINGERPRINT_OUT: frozenset[str] = frozenset({
    # Identity / connection / ontology.
    "name",
    "description",
    "schema_dir",
    "postgres",
    "postgres_dsn",
    "profile",
    "modules",
    "modules_config",
    # `module_providers` names the directories the declared modules are read
    # from. Like `modules` and `schema_dir` it is an ontology input: what it
    # resolves to is already bound by the catalog_fingerprint, which composes
    # from exactly these providers, so binding it here would double-count the
    # same change (ruling R1).
    "module_providers",
    # Read-side operator surfaces (MCP, viz, skills, agent tools).
    "mcp_instructions",
    "skills_dir",
    "mcp_trace_path",
    "audit_questions",
    "tool_description_overrides",
    "skill_triggers",
    "snapshot_path",
    "viz_path",
    "viz",
    "mcp_pre_join_tools",
    "resolvers",            # pre-join resolvers: MCP read-time, not publish
    "search",               # analyst search profiles: read-side ranking scope only; changes no published or synced row
    "coverage",             # deployment-declared known coverage gaps: a read-side disclosure echoed by inspect(coverage); ingests nothing and publishes nothing
    "not_ingested",         # deployment-declared out-of-scope sources: a read-side disclosure in the MCP dataset card, like `coverage`; ingests nothing and publishes nothing
    "citation_attr",        # the record attribute a reader cites (PACT read-names-list-3666): read by okg.substrate.mcp.citation_id only, to name it on search and inspect rows; publishes, ingests and syncs nothing
    "mcp",                   # MCP tool profiles: bounds which tools a server REGISTERS at launch; read-side surface only, changes no published or synced row
    "agent_surface",        # THE declared agent surface: served operators, inspect kind policy, response budget, per-client projections. Read-side by the same reading as `mcp` and `chat` above -- it bounds what a caller may ASK, and publishes, ingests and syncs nothing
    "chat",                 # governed chat-site identity (name, theme, models, preset, tool surface): a read-side human surface; changes no published or synced row
    "agent_tools",
    "session_start_context",
    "record_prefixes",
    "dashboards",
    "live_capabilities",
    # Ops / lifecycle tuning.
    "refreshers",
    "profiling",
    "chronos",
    "release",
    "contract",
    "readiness",
    "doctor_checks",
    "deployment_up",   # deployment-up backup/progress policy (lifecycle)
    "host_storage",    # host disk headroom thresholds (ops governance)
    "docker",          # compose stack written by okg install for okg start/stop/restart (lifecycle); selects no published or synced row
    "code_drift",      # code-drift check policy: what the doctor/readiness probe compares the sync root with; selects no published or synced row
    # Issue #1479: publish CAPACITY and warning policy -- kickoff interval,
    # gate/invariant budgets, the hard per-statement timeout, the lock-hold
    # headroom factor, the raised-timeout authorization and the first-publish
    # row-count warning thresholds. Every one of these bounds how long a
    # publish may take or when it warns; none of them selects WHICH rows are
    # published or synced, so the same inputs produce the same generation
    # under any of these values. Classifying it IN would make raising a
    # timeout change the deployment fingerprint and refuse a branch flip,
    # which is exactly the ops-knob-as-identity trap R1 OUT exists to avoid.
    "publisher",
})

# Only these ``runtime`` sub-keys select what runs at publish / source
# sync; pools/ports/schedules/smoke/python_extras are ops tuning (R1).
_RUNTIME_IN_SUBKEYS: tuple[str, ...] = ("workflows", "source_sync")

# IN keys whose manifest value is a *path* to a sibling config file whose
# CONTENT (not the path string) is behavior-bearing (ruling R1).
_SIBLING_FILE_KEYS: frozenset[str] = frozenset({
    "source_registry",
    "enricher_registry",
    "taxonomy",
    "aliases",
    "link_config",
    "alias_resolver",
})


def _sha256_text(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def _canonical_default(value: Any) -> Any:
    # Sets (YAML `!!set`) have hash-randomized iteration order, so rendering
    # them via `str` made the fingerprint differ per process (#828). Order
    # them by their own canonical serialization -- a total order that works
    # for mixed element types. Every other non-JSON-native value keeps the
    # historical `str` rendering byte-for-byte, so fingerprints of set-free
    # manifests are unchanged by this fix.
    if isinstance(value, (set, frozenset)):
        return sorted(value, key=_canonical_json)
    return str(value)


def _canonical_json(value: Any) -> str:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            default=_canonical_default,
        )
    except TypeError as exc:
        # A parseable manifest whose block cannot be canonically serialized
        # (e.g. mixed-type mapping keys defeat sort_keys) is a typed, loud
        # failure -- never a raw TypeError escaping the fingerprint (#828).
        raise ManifestFingerprintError(
            f"manifest block cannot be canonically serialized: {exc}",
            fix_hint=(
                "remove mixed-type mapping keys (or other unorderable "
                "values) from deployment.yaml, then re-claim"
            ),
        ) from exc


def _hash_file_content(path: Path) -> str:
    try:
        data = path.read_bytes()
    except OSError as exc:  # present-but-unreadable: hard failure (R3).
        raise ManifestFingerprintError(
            f"declared manifest sibling config is unreadable: {path}",
            fix_hint="fix the file or the manifest path, then re-claim",
        ) from exc
    return "sha256:" + hashlib.sha256(data).hexdigest()


def _hash_block(
    key: str,
    raw_value: Any,
    *,
    deployment_dir: Path,
) -> str:
    """Content hash of one behavior-bearing manifest block."""

    if key == "runtime":
        block = raw_value if isinstance(raw_value, dict) else {}
        selected = {
            sub: block.get(sub)
            for sub in _RUNTIME_IN_SUBKEYS
            if sub in block
        }
        return _sha256_text("runtime:" + _canonical_json(selected))

    if key in _SIBLING_FILE_KEYS and isinstance(raw_value, str) and raw_value:
        candidate = Path(raw_value)
        resolved = (
            candidate if candidate.is_absolute() else deployment_dir / candidate
        )
        if resolved.is_file():
            return _hash_file_content(resolved)
        # Configured-but-missing sibling file is a hard failure, not a
        # benign default (R3, no-benign-defaults).
        raise ManifestFingerprintError(
            f"declared manifest sibling config does not exist: {resolved}",
            fix_hint="fix the file or the manifest path, then re-claim",
        )

    return _sha256_text(key + ":" + _canonical_json(raw_value))


def manifest_fingerprint_for(
    deployment_dir: Path | str | None,
) -> tuple[str | None, dict[str, str]]:
    """Compute ``(manifest_fingerprint, per_block_hashes)`` for a deployment.

    Returns ``(None, {})`` when ``deployment_dir`` is None or carries no
    ``deployment.yaml`` -- pathless/legacy callers stay permissive until a
    stamped ownership row arms enforcement (ruling R5b). Raises
    :class:`ManifestFingerprintError` when a present manifest or a declared
    sibling config file cannot be read (ruling R3).
    """

    if deployment_dir is None:
        return None, {}
    dep_dir = Path(deployment_dir)
    manifest_path = dep_dir / "deployment.yaml"
    if not manifest_path.is_file():
        return None, {}

    from okg.substrate import fastyaml

    try:
        raw = fastyaml.safe_load(
            manifest_path.read_text(encoding="utf-8")
        ) or {}
    except Exception as exc:  # noqa: BLE001 - present-but-unreadable is fatal.
        raise ManifestFingerprintError(
            f"deployment manifest is unreadable: {manifest_path}",
            fix_hint="fix deployment.yaml, then re-claim ownership",
        ) from exc
    if not isinstance(raw, dict):
        raise ManifestFingerprintError(
            f"deployment manifest is not a mapping: {manifest_path}",
            fix_hint="fix deployment.yaml, then re-claim ownership",
        )

    block_hashes: dict[str, str] = {}
    # source_registry is behavior-bearing by CONTENT even when the manifest
    # omits the key (it defaults to source_registry.yaml); resolve + hash
    # the file whenever it exists (ruling R1).
    default_registry = dep_dir / "source_registry.yaml"
    if "source_registry" not in raw and default_registry.is_file():
        block_hashes["source_registry"] = _hash_file_content(default_registry)

    for key in sorted(raw):
        if key not in MANIFEST_FINGERPRINT_IN:
            continue
        block_hashes[key] = _hash_block(
            key, raw[key], deployment_dir=dep_dir
        )

    folded = _sha256_text(_canonical_json(block_hashes))
    return folded, block_hashes


def census_gap(known_top_level_keys: frozenset[str]) -> set[str]:
    """Keys in the manifest model classified as neither IN nor OUT.

    A non-empty result means the block-census coupling test must fail:
    every consumed manifest key has to land in exactly one of the two
    sets so the fingerprint coverage cannot silently rot (ruling R1).
    """

    classified = MANIFEST_FINGERPRINT_IN | MANIFEST_FINGERPRINT_OUT
    return set(known_top_level_keys) - classified


def census_overlap() -> set[str]:
    """Keys mistakenly classified as both IN and OUT (must be empty)."""

    return set(MANIFEST_FINGERPRINT_IN & MANIFEST_FINGERPRINT_OUT)
