"""Typed, fail-closed identity resolution for extracted entity nodes.

The legacy linker resolves edge targets.  This module owns the different
boundary where an LLM enricher proposes a *node*: exact curated alias first,
then exact normalized live-name equality, then an optional deployment plugin,
and only then deterministic minting.  A durable identity claim serializes
compare-and-mint before the proposed fact reaches FactBatch digesting.
"""

from __future__ import annotations

import copy
import hashlib
import importlib
import inspect
import json
import time
import unicodedata
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Any, Mapping, Protocol

import psycopg
import regex as bounded_regex

from okg.substrate.errors import SubstrateError


IDENTITY_ALGORITHM_VERSION = "unicode-escape-v1"
MAX_DENY_EXACT_VALUES = 512
MAX_DENY_PATTERNS = 32
DENY_PATTERN_TOTAL_BUDGET_SECONDS = 0.05


class ResolutionStatus(str, Enum):
    MATCHED = "matched"
    NOVEL = "novel"
    REJECTED = "rejected"
    AMBIGUOUS = "ambiguous"
    UNAVAILABLE = "unavailable"


class EntityResolutionError(SubstrateError):
    code = "entity_resolution_failed"


class EntityResolutionRejected(EntityResolutionError):
    code = "entity_resolution_rejected"


class EntityResolutionAmbiguous(EntityResolutionError):
    code = "entity_resolution_ambiguous"


class EntityResolutionUnavailable(EntityResolutionError):
    code = "entity_resolution_unavailable"


@dataclass(frozen=True)
class ResolutionDecision:
    status: ResolutionStatus
    canonical_node_id: str | None
    method: str
    normalized_key: str
    catalog_version_id: int | None
    graph_token: str
    materialize: bool = False


#: Keyword parameters an escalator must be able to accept. `_escalator` refuses
#: a configured class whose `resolve` provably cannot take these, so a signature
#: written against an older substrate fails at load with a fix hint instead of
#: raising a bare TypeError once per extracted entity.
ESCALATOR_REQUIRED_PARAMETERS = ("attrs", "graph")


class EntityResolutionEscalator(Protocol):
    """Optional deployment-owned tier after deterministic local matching.

    Every tier above this one is keyed on the node's *name*: curated alias,
    normalized live-name equality, and the deterministic mint all hash the
    name field. That is right for entity-shaped subtypes, whose name is their
    identity. It is useless for fact-shaped subtypes -- a measurement, an
    estimate, a claim -- whose name is model prose and whose identity is a
    tuple of attribute slots. This tier is where a deployment supplies the
    identity the substrate cannot know, so it is handed both the proposed
    attributes and the checkout to look candidates up in.

    ``attrs`` is the proposed node's attributes as a read-only view over a deep
    copy: an escalator cannot reach back through it and change what the
    extractor goes on to emit.

    ``graph`` is the stage's own generation-pinned Chronos checkout -- query it
    with ``graph.query(sql, params)``. It is passed rather than left to the
    escalator to construct because ``okg.graph_nodes`` is a Chronos-managed
    base table: the same ``SELECT`` issued on the bare ``conn`` reads zero rows
    with no error, so an escalator that built its own connection-level query
    would match nothing silently and mint a duplicate every run -- the exact
    failure this tier exists to prevent. Passing the stage's object also keeps
    the escalator on the branch being written instead of a branch name copied
    into deployment config that can drift.
    """

    def resolve(
        self,
        *,
        conn: psycopg.Connection,
        subtype: str,
        raw_name: str,
        normalized_key: str,
        graph_token: str,
        attrs: Mapping[str, Any],
        graph: Any,
    ) -> ResolutionDecision | None:
        """Return MATCHED, explicit NOVEL, or a non-success typed outcome."""
        raise NotImplementedError


@dataclass(frozen=True)
class EntitySubtypePolicy:
    name_field: str
    id_prefix: str
    identity_fields: tuple[str, ...]
    alias_first: bool
    live_exact: bool
    escalation_class: str | None
    escalation_config: Mapping[str, Any]
    min_length: int
    max_length: int
    deny_exact: frozenset[str]
    deny_patterns: tuple[str, ...]

    @classmethod
    def from_mapping(
        cls, subtype: str, raw: Mapping[str, Any]
    ) -> "EntitySubtypePolicy":
        name_field = str(raw.get("name_field") or "").strip()
        id_prefix = str(raw.get("id_prefix") or "").strip().strip(":")
        if not name_field or not id_prefix:
            raise ValueError(
                f"entity-resolution policy for {subtype!r} requires "
                "name_field and id_prefix"
            )
        if "identity_fields" not in raw:
            raise ValueError(
                f"entity-resolution policy for {subtype!r} requires explicit "
                "identity_fields (use [] only when the subtype has no identity attrs)"
            )
        raw_identity_fields = raw.get("identity_fields")
        if isinstance(raw_identity_fields, (str, bytes)) or not isinstance(
            raw_identity_fields, (list, tuple)
        ):
            raise ValueError(
                f"entity-resolution identity_fields for {subtype!r} must be a list"
            )
        identity_fields = tuple(
            dict.fromkeys(str(value).strip() for value in raw_identity_fields)
        )
        if any(not value for value in identity_fields):
            raise ValueError(
                f"entity-resolution identity_fields for {subtype!r} cannot be blank"
            )
        post_filter = raw.get("post_filter") or {}
        if not isinstance(post_filter, Mapping):
            raise ValueError(
                f"entity-resolution post_filter for {subtype!r} must be a mapping"
            )
        min_length = int(post_filter.get("min_length", 1))
        max_length = int(post_filter.get("max_length", 200))
        if min_length < 1 or max_length < min_length or max_length > 1000:
            raise ValueError(
                f"entity-resolution length bounds for {subtype!r} must satisfy "
                "1 <= min_length <= max_length <= 1000"
            )
        raw_deny_exact = post_filter.get("deny_exact", [])
        if isinstance(raw_deny_exact, (str, bytes)) or not isinstance(
            raw_deny_exact, (list, tuple)
        ):
            raise ValueError("entity-resolution deny_exact must be a list")
        if len(raw_deny_exact) > MAX_DENY_EXACT_VALUES:
            raise ValueError(
                f"entity-resolution deny_exact exceeds {MAX_DENY_EXACT_VALUES} values"
            )
        deny_exact = frozenset(
            normalize_entity_name(str(value)) for value in raw_deny_exact
        )
        raw_deny_patterns = post_filter.get("deny_patterns", [])
        if isinstance(raw_deny_patterns, (str, bytes)) or not isinstance(
            raw_deny_patterns, (list, tuple)
        ):
            raise ValueError("entity-resolution deny_patterns must be a list")
        if len(raw_deny_patterns) > MAX_DENY_PATTERNS:
            raise ValueError(
                f"entity-resolution deny_patterns exceeds {MAX_DENY_PATTERNS} values"
            )
        deny_patterns = tuple(str(value) for value in raw_deny_patterns)
        for pattern in deny_patterns:
            if len(pattern) > 256:
                raise ValueError(
                    "entity-resolution deny pattern exceeds 256 characters"
                )
            bounded_regex.compile(pattern)
        escalation = raw.get("escalation") or {}
        if not isinstance(escalation, Mapping):
            raise ValueError(
                f"entity-resolution escalation for {subtype!r} must be a mapping"
            )
        escalation_class = str(escalation.get("class") or "").strip() or None
        escalation_config = escalation.get("config") or {}
        if not isinstance(escalation_config, Mapping):
            raise ValueError("entity-resolution escalation config must be a mapping")
        return cls(
            name_field=name_field,
            id_prefix=id_prefix,
            identity_fields=identity_fields,
            alias_first=bool(raw.get("alias_first", True)),
            live_exact=bool(raw.get("live_exact", True)),
            escalation_class=escalation_class,
            escalation_config=dict(escalation_config),
            min_length=min_length,
            max_length=max_length,
            deny_exact=deny_exact,
            deny_patterns=deny_patterns,
        )

    def fingerprint(self) -> str:
        payload = {
            "algorithm": IDENTITY_ALGORITHM_VERSION,
            "name_field": self.name_field,
            "id_prefix": self.id_prefix,
            "identity_fields": list(self.identity_fields),
            "alias_first": self.alias_first,
            "live_exact": self.live_exact,
            "escalation_class": self.escalation_class,
            "escalation_config": dict(self.escalation_config),
            "min_length": self.min_length,
            "max_length": self.max_length,
            "deny_exact": sorted(self.deny_exact),
            "deny_patterns": list(self.deny_patterns),
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()


def normalize_entity_name(value: str) -> str:
    """NFKC + casefold identity key, preserving Unicode letters and digits."""
    normalized = unicodedata.normalize("NFKC", str(value)).casefold().strip()
    pieces: list[str] = []
    separator = False
    for char in normalized:
        # Preserve letters, numbers, combining marks, and symbols. In several
        # scripts a combining mark is a semantic vowel/diacritic; dropping it
        # collapses distinct identities before the ASCII escape can protect
        # them. Punctuation, separators, and controls remain word boundaries.
        if unicodedata.category(char)[0] in {"L", "N", "M", "S"}:
            if separator and pieces:
                pieces.append(" ")
            pieces.append(char)
            separator = False
        else:
            separator = True
    return "".join(pieces).strip()


def _ascii_escape_slug(normalized_key: str) -> str:
    pieces: list[str] = []
    separator = False
    for char in normalized_key:
        if "a" <= char <= "z" or "0" <= char <= "9":
            if separator and pieces:
                pieces.append("-")
            pieces.append(char)
            separator = False
        elif char == " ":
            separator = True
        else:
            if separator and pieces:
                pieces.append("-")
            pieces.append(f"u{ord(char):04x}")
            separator = True
    return "".join(pieces).strip("-") or "entity"


def mint_entity_id(id_prefix: str, raw_name: str) -> str:
    """Mint an ASCII node id without dropping meaningful Unicode codepoints."""
    normalized_key = normalize_entity_name(raw_name)
    if not normalized_key:
        raise EntityResolutionRejected("entity name normalizes to an empty identity")
    slug = _ascii_escape_slug(normalized_key)
    digest = hashlib.sha256(
        f"{IDENTITY_ALGORITHM_VERSION}|{id_prefix}|{normalized_key}".encode("utf-8")
    ).hexdigest()[:12]
    return f"{id_prefix}:{slug[:96]}-{digest}"


class EntityResolutionStage:
    """Deployment-scoped resolver used before extracted NodeFacts are emitted."""

    def __init__(self, config: Mapping[str, Any]) -> None:
        self.deployment_name = str(config.get("deployment_name") or "").strip()
        self.branch_name = str(config.get("branch_name") or "").strip()
        if not self.deployment_name or not self.branch_name:
            raise ValueError(
                "enabled entity_resolution requires explicit deployment_name and branch_name"
            )
        if (
            str(config.get("algorithm_version") or IDENTITY_ALGORITHM_VERSION)
            != IDENTITY_ALGORITHM_VERSION
        ):
            raise ValueError(
                f"unsupported entity-resolution algorithm_version; expected {IDENTITY_ALGORITHM_VERSION!r}"
            )
        raw_policies = config.get("policies") or {}
        if not isinstance(raw_policies, Mapping) or not raw_policies:
            raise ValueError("entity_resolution requires a non-empty policies mapping")
        self.policies = {
            str(subtype): EntitySubtypePolicy.from_mapping(str(subtype), raw)
            for subtype, raw in raw_policies.items()
            if isinstance(raw, Mapping)
        }
        if len(self.policies) != len(raw_policies):
            raise ValueError("every entity-resolution subtype policy must be a mapping")
        self._escalators: dict[str, EntityResolutionEscalator] = {}
        self._normalized_aliases_checked = False
        # Per-batch scratch. `begin_batch` clears all three; see its docstring
        # for why batch scope is the correct lifetime.
        self._graph: Any | None = None
        self._catalog_version: int | None = None
        self._live_index: dict[str, dict[str, list[str]]] = {}

    def begin_batch(self) -> None:
        """Drop per-batch caches. Call once per enrichment cycle.

        Three things were being recomputed per entity that are constant for the
        duration of a batch: the Chronos checkout, the active catalog version,
        and -- the expensive one -- the subtype's live normalized-name index,
        which was a full scan of every live node of that subtype normalized in
        Python. For N entities against M live nodes that made the stage read
        N*M rows; it now reads M once.

        Batch, not process, is the right lifetime. A longer-lived cache would
        miss nodes committed by other writers between cycles. Within a cycle a
        concurrent writer can still land a node this index does not have, but
        that cannot silently duplicate an identity. Two fences arbitrate, and
        WHICH of them catches a given race is sub-millisecond ordering, so
        both converge on the same outcome. The primary key on (deployment,
        branch, subtype, normalized_key) makes an identical racing claim
        converge on the winner's canonical. A partial unique index on
        (deployment, branch, subtype, canonical) over minted rows can catch
        the same race first, when the loser's ON CONFLICT arbiter pre-check
        runs before the winner's primary-key tuple lands; `_claim` then
        re-reads this key's claim and converges on it too. That index is not
        purely fail-closed, then: it fails closed only where there is nothing
        to converge on, which is its real job -- two DIFFERENT normalized
        identities minting one canonical, raised as
        EntityResolutionAmbiguous. Either way only a disagreement on
        canonical or policy fingerprint refuses. Identities minted BY this
        batch are covered because `_existing_claim` runs before the index is
        consulted.
        """
        self._graph = None
        self._catalog_version = None
        self._live_index = {}

    def preload_graph(self, conn: psycopg.Connection) -> None:
        """Pin the batch graph before a caller opens its write transaction."""
        self._batch_graph(conn)

    def enabled_for(self, subtype: str) -> bool:
        return subtype in self.policies

    def _require_normalized_aliases(
        self, conn: psycopg.Connection, catalog_version_id: int
    ) -> None:
        """Refuse to resolve against a half-normalized alias table.

        `okg migrate` backfills `normalized_alias` on every pre-#796 row in the
        same transaction that adds the column, so a straggling NULL means the
        deployment's schema snapshot is behind. Matching only the populated
        rows would silently mint a new canonical for a name the operator has
        already curated an alias for -- the duplicate this stage exists to
        prevent. Fail closed instead, once per stage, and let the hot lookup
        below be served by aliases_normalized_lookup_idx.

        Scoped to the catalog version the lookup will actually query: a dead
        NULL row under a retired catalog version can never match
        `_exact_aliases`, and failing every subtype's resolution over one is a
        self-inflicted outage. A genuinely un-migrated table has NULLs under
        the ACTIVE version too, so the upgrade case is still caught.

        Every in-tree alias writer (catalog/aliases.py, catalog/modules_apply.py)
        populates the column, which is what makes caching the result sound; a
        future writer that omits it would be tolerated for the life of an
        already-constructed stage.
        """
        if self._normalized_aliases_checked:
            return
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM okg.aliases "
                    "WHERE catalog_version_id = %s AND normalized_alias IS NULL "
                    "LIMIT 1",
                    (catalog_version_id,),
                )
                straggler = cur.fetchone()
        except psycopg.errors.UndefinedColumn as exc:
            # A truly pre-#796 database has no column to probe. That is the
            # deployment this guard exists for, so it must surface as a typed
            # unavailability -- never as a raw psycopg error escaping into the
            # enricher, which has no handler for one and would poison the
            # publisher's transaction.
            raise EntityResolutionUnavailable(
                "okg.aliases predates normalized_alias",
                fix_hint="run `okg migrate` to re-apply the schema snapshot",
            ) from exc
        if straggler is not None:
            raise EntityResolutionUnavailable(
                "okg.aliases has rows with no normalized_alias",
                fix_hint="run `okg migrate` to backfill normalized_alias",
            )
        self._normalized_aliases_checked = True

    def canonicalize_identity_attrs(
        self,
        *,
        subtype: str,
        attrs: Mapping[str, Any],
        proposed_node_id: str,
        canonical_node_id: str,
    ) -> dict[str, Any]:
        """Rewrite deployment-declared identity attrs to the canonical id."""
        policy = self.policies.get(subtype)
        if policy is None:
            raise EntityResolutionUnavailable(
                f"no entity-resolution policy is enabled for subtype {subtype!r}"
            )
        rewritten = dict(attrs)
        for field in policy.identity_fields:
            existing = rewritten.get(field)
            if existing is not None and str(existing) not in {
                proposed_node_id,
                canonical_node_id,
            }:
                raise EntityResolutionUnavailable(
                    "entity identity attribute disagrees with the proposed node id",
                    data={
                        "subtype": subtype,
                        "field": field,
                        "value": existing,
                        "proposed_node_id": proposed_node_id,
                    },
                )
            rewritten[field] = canonical_node_id
        return rewritten

    def resolve(
        self,
        conn: psycopg.Connection,
        *,
        proposed_node_id: str,
        subtype: str,
        attrs: Mapping[str, Any],
        source_node_ref: str | None = None,
    ) -> ResolutionDecision:
        policy = self.policies.get(subtype)
        if policy is None:
            raise EntityResolutionUnavailable(
                f"no entity-resolution policy is enabled for subtype {subtype!r}"
            )
        raw_name = str(attrs.get(policy.name_field) or "").strip()
        normalized_key = normalize_entity_name(raw_name)
        if not normalized_key:
            self._reject(
                conn,
                subtype,
                raw_name,
                normalized_key,
                policy,
                "empty_normalized_name",
                source_node_ref,
            )

        graph = self._batch_graph(conn)
        # Chronos exposes the checkout's pinned reference, not a global
        # per-write branch revision.  It is diagnostic evidence only; the
        # unique identity claim below is the concurrency boundary.
        graph_token = str(graph.current_ref)
        policy_fingerprint = policy.fingerprint()
        claimed = self._existing_claim(conn, subtype, normalized_key)
        if claimed is not None:
            canonical, prior_fingerprint, method = claimed
            if prior_fingerprint != policy_fingerprint:
                # Configuring an escalation class for a subtype that already
                # has claims lands here: `escalation_class` is a fingerprint
                # input, so every key claimed under the old policy stops
                # matching. That refusal is deliberate -- an identity decided
                # under one policy must not be silently reused under another --
                # but it is a wall without the way through it.
                raise EntityResolutionUnavailable(
                    "entity identity was claimed under a different policy fingerprint",
                    data={
                        "subtype": subtype,
                        "normalized_key": normalized_key,
                        "claimed_policy_fingerprint": prior_fingerprint,
                        "active_policy_fingerprint": policy_fingerprint,
                    },
                    fix_hint=(
                        "the subtype's resolution policy changed after these "
                        "identities were claimed; re-decide them under the new "
                        "policy by deleting the stale rows "
                        "(DELETE FROM okg.entity_identity_claims WHERE "
                        "deployment_name = ... AND branch_name = ... AND "
                        "subtype = ... AND policy_fingerprint = "
                        "'<claimed_policy_fingerprint>'), or merge the "
                        "duplicates they already point at with "
                        "`okg remediate entity-merge`, which rehomes the claim "
                        "rows onto the survivor"
                    ),
                )
            live = self._node_is_live(graph, canonical)
            if not live and method != "mint":
                raise EntityResolutionUnavailable(
                    "claimed canonical is no longer live",
                    data={
                        "subtype": subtype,
                        "normalized_key": normalized_key,
                        "canonical_node_id": canonical,
                        "resolution_method": method,
                    },
                )
            return ResolutionDecision(
                ResolutionStatus.MATCHED,
                canonical,
                f"identity_claim:{method}",
                normalized_key,
                self._active_catalog_version(conn, required=False),
                graph_token,
                materialize=not live,
            )

        catalog_version_id = self._active_catalog_version(
            conn, required=policy.alias_first
        )
        canonical: str | None = None
        method = ""
        if policy.alias_first:
            self._require_normalized_aliases(conn, int(catalog_version_id))
            aliases = self._exact_aliases(
                conn, raw_name, normalized_key, subtype, int(catalog_version_id)
            )
            if len(aliases) > 1:
                raise EntityResolutionAmbiguous(
                    "exact curated aliases disagree",
                    data={"subtype": subtype, "name": raw_name, "canonicals": aliases},
                )
            if aliases:
                canonical = aliases[0]
                self._require_live_subtype(graph, canonical, subtype, tier="alias")
                method = "curated_alias_exact"

        if canonical is None and policy.live_exact:
            live_matches = self._live_normalized_matches(
                graph, subtype, policy.name_field, normalized_key
            )
            if len(live_matches) > 1:
                raise EntityResolutionAmbiguous(
                    "multiple live nodes have the same normalized identity",
                    data={
                        "subtype": subtype,
                        "normalized_key": normalized_key,
                        "node_ids": live_matches,
                    },
                )
            if live_matches:
                canonical = live_matches[0]
                method = "live_normalized_exact"

        escalation_materialize = False
        escalation_decision: ResolutionDecision | None = None
        if canonical is None and policy.escalation_class:
            # A read-only view over a deep copy. The view alone would still let
            # an escalator mutate a nested list or dict in place and silently
            # change the attrs emitted below; the copy closes that. Built
            # OUTSIDE the try on purpose: a failure here is the substrate's, and
            # blaming it on the plugin would send an operator to read the wrong
            # code. Only paid on the escalation path, after the alias and
            # live-name tiers have already declined.
            escalation_attrs = MappingProxyType(copy.deepcopy(dict(attrs)))
            try:
                escalation_decision = self._escalator(subtype, policy).resolve(
                    conn=conn,
                    subtype=subtype,
                    raw_name=raw_name,
                    normalized_key=normalized_key,
                    graph_token=graph_token,
                    attrs=escalation_attrs,
                    graph=graph,
                )
            except EntityResolutionError:
                raise
            except Exception as exc:  # noqa: BLE001 - unavailable must not mint
                raise EntityResolutionUnavailable(
                    "entity-resolution escalation failed",
                    data={
                        "subtype": subtype,
                        "escalation_class": policy.escalation_class,
                        "name": raw_name,
                    },
                ) from exc
            if escalation_decision is None:
                raise EntityResolutionUnavailable(
                    "entity-resolution escalation returned no typed outcome",
                    data={"subtype": subtype, "name": raw_name},
                )
            if escalation_decision.status is ResolutionStatus.MATCHED:
                canonical = escalation_decision.canonical_node_id
                if not canonical:
                    raise EntityResolutionUnavailable(
                        "matched escalation omitted canonical_node_id"
                    )
                self._require_live_subtype(graph, canonical, subtype, tier="escalation")
                method = "escalation"
                # Honour the escalator's own `materialize`. A matched identity
                # otherwise returns early in the caller and emits no node fact,
                # so the canonical's attributes freeze at whichever extraction
                # created it and every later one is dropped without a record.
                # That is harmless when the name IS the identity, and is silent
                # data loss for the fact-shaped subtypes this tier exists to
                # serve, where the attributes are the content. The deployment
                # decides: default False keeps the established behaviour.
                escalation_materialize = bool(escalation_decision.materialize)
            elif escalation_decision.status is not ResolutionStatus.NOVEL:
                error_type = {
                    ResolutionStatus.AMBIGUOUS: EntityResolutionAmbiguous,
                    ResolutionStatus.REJECTED: EntityResolutionRejected,
                    ResolutionStatus.UNAVAILABLE: EntityResolutionUnavailable,
                }.get(escalation_decision.status, EntityResolutionUnavailable)
                raise error_type(
                    f"entity-resolution escalation returned {escalation_decision.status.value}",
                    data={"subtype": subtype, "name": raw_name},
                )

        materialize = escalation_materialize
        status = ResolutionStatus.MATCHED
        if canonical is None:
            self._apply_post_filter(
                conn, subtype, raw_name, normalized_key, policy, source_node_ref
            )
            canonical = mint_entity_id(policy.id_prefix, raw_name)
            collision = self._node_row(graph, canonical)
            if collision is not None:
                collision_attrs = collision.get("attrs") or {}
                collision_name = str(collision_attrs.get(policy.name_field) or "")
                if (
                    str(collision.get("subtype")) != subtype
                    or normalize_entity_name(collision_name) != normalized_key
                ):
                    raise EntityResolutionAmbiguous(
                        "deterministic entity id collides with a different live identity",
                        data={
                            "node_id": canonical,
                            "existing_subtype": collision.get("subtype"),
                            "requested_subtype": subtype,
                            "existing_name": collision_name,
                            "name": raw_name,
                        },
                    )
            method = "mint"
            status = ResolutionStatus.NOVEL
            materialize = True

        claimed_canonical, claimed_method, claimed_fingerprint = self._claim(
            conn,
            subtype=subtype,
            normalized_key=normalized_key,
            raw_name=raw_name,
            canonical_node_id=canonical,
            method=method,
            policy_fingerprint=policy_fingerprint,
            catalog_version_id=catalog_version_id,
            graph_token=graph_token,
        )
        if claimed_canonical != canonical:
            raise EntityResolutionAmbiguous(
                "concurrent identity claim selected a different canonical",
                data={
                    "subtype": subtype,
                    "normalized_key": normalized_key,
                    "planned": canonical,
                    "claimed": claimed_canonical,
                },
            )
        if claimed_fingerprint != policy_fingerprint:
            raise EntityResolutionUnavailable(
                "concurrent identity claim used a different policy fingerprint",
                data={
                    "subtype": subtype,
                    "normalized_key": normalized_key,
                    "planned_policy_fingerprint": policy_fingerprint,
                    "claimed_policy_fingerprint": claimed_fingerprint,
                },
            )
        return ResolutionDecision(
            status,
            canonical,
            claimed_method,
            normalized_key,
            catalog_version_id,
            graph_token,
            materialize=materialize,
        )

    def _existing_claim(
        self, conn: psycopg.Connection, subtype: str, normalized_key: str
    ) -> tuple[str, str, str] | None:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT canonical_node_id, policy_fingerprint, resolution_method "
                "FROM okg.entity_identity_claims WHERE deployment_name = %s "
                "AND branch_name = %s AND subtype = %s AND normalized_key = %s",
                (self.deployment_name, self.branch_name, subtype, normalized_key),
            )
            row = cur.fetchone()
        return (str(row[0]), str(row[1]), str(row[2])) if row else None

    def _claim(
        self,
        conn: psycopg.Connection,
        *,
        subtype: str,
        normalized_key: str,
        raw_name: str,
        canonical_node_id: str,
        method: str,
        policy_fingerprint: str,
        catalog_version_id: int | None,
        graph_token: str,
    ) -> tuple[str, str, str]:
        try:
            # A nested transaction is a savepoint when the publisher already
            # owns a transaction. It lets the partial unique mint fence fail
            # without poisoning the caller's transaction before we raise the
            # typed collision outcome below.
            with conn.transaction(), conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO okg.entity_identity_claims
                      (deployment_name, branch_name, subtype, normalized_key,
                       canonical_node_id, raw_name, resolution_method,
                       policy_fingerprint, catalog_version_id, graph_token)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (deployment_name, branch_name, subtype, normalized_key)
                    DO NOTHING
                    """,
                    (
                        self.deployment_name,
                        self.branch_name,
                        subtype,
                        normalized_key,
                        canonical_node_id,
                        raw_name,
                        method,
                        policy_fingerprint,
                        catalog_version_id,
                        graph_token,
                    ),
                )
                cur.execute(
                    "SELECT canonical_node_id, resolution_method, policy_fingerprint "
                    "FROM okg.entity_identity_claims WHERE deployment_name = %s "
                    "AND branch_name = %s AND subtype = %s AND normalized_key = %s",
                    (
                        self.deployment_name,
                        self.branch_name,
                        subtype,
                        normalized_key,
                    ),
                )
                row = cur.fetchone()
        except psycopg.errors.UniqueViolation as exc:
            # The savepoint has rolled back, so the connection is usable. Two
            # concurrent first writers for one normalized key compute the same
            # canonical, so the loser trips the partial unique mint fence
            # before its own ON CONFLICT arm can see the winner's row. Re-read
            # the claim and let the caller's checks arbitrate: an identical
            # claim converges, a different canonical or fingerprint still
            # fails closed. Only a genuine cross-identity mint collision --
            # a different normalized key holding this canonical -- has no row
            # to converge on, and that keeps raising here.
            existing = self._existing_claim(conn, subtype, normalized_key)
            if existing is not None:
                existing_canonical, existing_fingerprint, existing_method = existing
                return existing_canonical, existing_method, existing_fingerprint
            raise EntityResolutionAmbiguous(
                "deterministic mint collided with another claimed identity",
                data={
                    "subtype": subtype,
                    "normalized_key": normalized_key,
                    "canonical_node_id": canonical_node_id,
                },
            ) from exc
        if row is None:
            raise EntityResolutionUnavailable(
                "identity claim insert was not observable"
            )
        return str(row[0]), str(row[1]), str(row[2])

    def _batch_graph(self, conn: psycopg.Connection) -> Any:
        """One Chronos checkout per batch instead of one per entity."""
        if self._graph is None:
            from okg.substrate.graph_store import ChronosGraphStore

            self._graph = ChronosGraphStore.from_connection(
                conn,
                ensure_schema=False,
                # Resolution queries can run after an extractor reserves an
                # input attempt. They must join the publisher transaction and
                # never invoke Chronos's per-query commit hook.
                autocommit=False,
            ).checkout(self.branch_name)
        return self._graph

    def _active_catalog_version(
        self, conn: psycopg.Connection, *, required: bool
    ) -> int | None:
        # The applied catalog version cannot change mid-batch without a catalog
        # recompose, which does not run concurrently with enrichment, so read it
        # once. `required` still fails closed on a cached miss.
        if self._catalog_version is not None:
            return self._catalog_version
        with conn.cursor() as cur:
            cur.execute(
                "SELECT catalog_version_id FROM okg.catalog_versions "
                "WHERE status = 'applied' "
                "AND (deployment_name = %s OR deployment_name IS NULL) "
                "ORDER BY (deployment_name = %s) DESC NULLS LAST, "
                "catalog_version_id DESC "
                "LIMIT 1",
                (self.deployment_name, self.deployment_name),
            )
            row = cur.fetchone()
        value = int(row[0]) if row and row[0] is not None else None
        if required and value is None:
            raise EntityResolutionUnavailable(
                "curated alias resolution requires an applied catalog version"
            )
        self._catalog_version = value
        return value

    @staticmethod
    def _exact_aliases(
        conn: psycopg.Connection,
        raw_name: str,
        normalized_key: str,
        subtype: str,
        catalog_version_id: int,
    ) -> list[str]:
        # Every row is normalized (enforced by _require_normalized_aliases), so
        # the `normalized_alias IS NULL` compatibility arm is gone -- it matched
        # every un-backfilled row and dragged the deployment's whole alias table
        # into Python on each extracted entity.
        #
        # Written as a UNION of single-index arms rather than one predicate with
        # `scope IS NULL OR scope = %s` and a cross-column
        # `alias = ANY(...) OR normalized_alias = ...`: that shape is not
        # index-addressable and the planner falls back to a full scan of
        # okg.aliases (measured: 200k rows, 73ms and every row filtered, versus
        # 0.14ms for the arms below). The scoped and global arms hit
        # aliases_normalized_lookup_idx; the raw-name arm hits the
        # (alias, catalog_version_id) primary key.
        needles = list(dict.fromkeys([raw_name, normalized_key]))
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT canonical FROM okg.aliases "
                    " WHERE catalog_version_id = %s AND scope = %s "
                    "   AND normalized_alias = %s "
                    "UNION "
                    "SELECT canonical FROM okg.aliases "
                    " WHERE catalog_version_id = %s AND scope IS NULL "
                    "   AND normalized_alias = %s "
                    "UNION "
                    "SELECT canonical FROM okg.aliases "
                    " WHERE alias = ANY(%s) AND catalog_version_id = %s "
                    "   AND (scope IS NULL OR scope = %s)",
                    (
                        catalog_version_id,
                        subtype,
                        normalized_key,
                        catalog_version_id,
                        normalized_key,
                        needles,
                        catalog_version_id,
                        subtype,
                    ),
                )
                canonicals = {str(canonical) for (canonical,) in cur.fetchall()}
                return sorted(canonicals)
        except Exception as exc:  # noqa: BLE001 - unavailability must never mint
            raise EntityResolutionUnavailable("curated alias lookup failed") from exc

    @staticmethod
    def _node_row(graph: Any, node_id: str) -> dict[str, Any] | None:
        rows = graph.query(
            "SELECT node_id, subtype, attrs FROM okg.graph_nodes WHERE node_id = :node_id",
            {"node_id": node_id},
        )
        return dict(rows[0]) if rows else None

    def _node_is_live(self, graph: Any, node_id: str) -> bool:
        return self._node_row(graph, node_id) is not None

    def _require_live_subtype(
        self, graph: Any, node_id: str, subtype: str, *, tier: str
    ) -> None:
        row = self._node_row(graph, node_id)
        if row is None or str(row.get("subtype")) != subtype:
            raise EntityResolutionUnavailable(
                f"{tier} target is missing or has the wrong subtype",
                data={"canonical_node_id": node_id, "expected_subtype": subtype},
            )

    def _live_normalized_matches(
        self, graph: Any, subtype: str, name_field: str, normalized_key: str
    ) -> list[str]:
        """Look the key up in the batch's live index, building it on first use.

        The comparison stays in Python deliberately. `normalize_entity_name` is
        Unicode-aware (NFKC, category-driven punctuation folding); a SQL
        re-implementation that drifted from it by even one codepoint would map
        two spellings to different keys and mint the duplicate this stage
        exists to prevent. So the normalizer stays single-sourced and the scan
        is amortized over the batch instead.
        """
        index = self._live_index.get(subtype)
        if index is None:
            index = {}
            rows = graph.query(
                "SELECT node_id, attrs FROM okg.graph_nodes "
                "WHERE subtype = :subtype ORDER BY node_id",
                {"subtype": subtype},
            )
            for row in rows:
                key = normalize_entity_name(
                    str((row.get("attrs") or {}).get(name_field) or "")
                )
                if key:
                    index.setdefault(key, []).append(str(row["node_id"]))
            self._live_index[subtype] = index
        return list(index.get(normalized_key, ()))

    def _escalator(
        self, subtype: str, policy: EntitySubtypePolicy
    ) -> EntityResolutionEscalator:
        cached = self._escalators.get(subtype)
        if cached is not None:
            return cached
        assert policy.escalation_class is not None
        try:
            module_name, class_name = policy.escalation_class.rsplit(".", 1)
            cls = getattr(importlib.import_module(module_name), class_name)
            escalator = cls(**dict(policy.escalation_config))
        except Exception as exc:  # noqa: BLE001 - plugin configuration is unavailable
            raise EntityResolutionUnavailable(
                f"could not load escalation {policy.escalation_class!r}"
            ) from exc
        self._require_escalator_parameters(escalator, policy.escalation_class, subtype)
        self._escalators[subtype] = escalator
        return escalator

    @staticmethod
    def _require_escalator_parameters(
        escalator: Any, escalation_class: str, subtype: str
    ) -> None:
        """Refuse a plugin whose `resolve` provably cannot take the new keywords.

        `attrs` and `graph` were added to the escalator contract, so a plugin
        written against the older signature raises TypeError once per extracted
        entity, mid-batch, reported as a generic escalation failure. Checking
        when the class is loaded turns that into a typed refusal naming the
        class and the parameters. "Loaded" means first use for that subtype,
        not stage construction: `_escalator` is lazy, so a configured subtype
        that never appears in a batch is never checked.

        Never a compatibility shim: there is no arm here that drops the new
        keywords and calls the old shape. A deployment either accepts them or
        the subtype does not resolve.

        An unintrospectable `resolve` (a C callable, an exotic descriptor) is
        NOT recorded as compatible -- nothing was checked, and treating an
        absent signature as a satisfied one is how a guard comes to report
        "verified" having verified nothing. It proceeds only because the check
        is redundant belt, not braces: the call site wraps the invocation and
        converts any TypeError into `entity_resolution_unavailable`, which
        never mints. That backstop is independent of this function and is
        exercised directly by
        `test_escalator_that_cannot_be_introspected_still_fails_closed`.
        """
        try:
            signature = inspect.signature(escalator.resolve)
        except (TypeError, ValueError):
            return
        parameters = list(signature.parameters.values())
        if any(
            parameter.kind is inspect.Parameter.VAR_KEYWORD
            for parameter in parameters
        ):
            return
        accepted = {
            parameter.name
            for parameter in parameters
            if parameter.kind
            in {
                inspect.Parameter.KEYWORD_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            }
        }
        missing = [
            name for name in ESCALATOR_REQUIRED_PARAMETERS if name not in accepted
        ]
        if missing:
            raise EntityResolutionUnavailable(
                f"escalation {escalation_class!r} cannot accept "
                f"{', '.join(missing)}",
                data={
                    "subtype": subtype,
                    "escalation_class": escalation_class,
                    "missing_parameters": missing,
                },
                fix_hint=(
                    "add keyword-only "
                    f"{' and '.join(ESCALATOR_REQUIRED_PARAMETERS)} parameters to "
                    "the escalator's resolve(); attrs is the proposed node's "
                    "attributes and graph is the pinned Chronos checkout to look "
                    "candidates up in"
                ),
            )

    def _apply_post_filter(
        self,
        conn: psycopg.Connection,
        subtype: str,
        raw_name: str,
        normalized_key: str,
        policy: EntitySubtypePolicy,
        source_node_ref: str | None,
    ) -> None:
        reason = ""
        if len(raw_name) > 4096:
            reason = "raw_name_exceeds_4096"
        elif len(normalized_key) < policy.min_length:
            reason = "below_min_length"
        elif len(normalized_key) > policy.max_length:
            reason = "above_max_length"
        elif normalized_key in policy.deny_exact:
            reason = "deny_exact"
        else:
            deadline = time.monotonic() + DENY_PATTERN_TOTAL_BUDGET_SECONDS
            for pattern in policy.deny_patterns:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise EntityResolutionUnavailable(
                        "entity-resolution deny patterns exceeded their total budget",
                        data={"subtype": subtype},
                    )
                try:
                    if bounded_regex.search(pattern, raw_name, timeout=remaining):
                        reason = f"deny_pattern:{pattern}"
                        break
                except TimeoutError as exc:
                    raise EntityResolutionUnavailable(
                        "entity-resolution deny pattern exceeded its execution budget",
                        data={"subtype": subtype, "pattern": pattern},
                    ) from exc
        if reason:
            self._reject(
                conn, subtype, raw_name, normalized_key, policy, reason, source_node_ref
            )

    def _reject(
        self,
        conn: psycopg.Connection,
        subtype: str,
        raw_name: str,
        normalized_key: str,
        policy: EntitySubtypePolicy,
        reason: str,
        source_node_ref: str | None,
    ) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.entity_resolution_rejections
                  (deployment_name, branch_name, subtype, normalized_key,
                   raw_name, reason, policy_fingerprint, source_node_ref)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (deployment_name, branch_name, subtype,
                             normalized_key, policy_fingerprint, reason)
                DO UPDATE SET last_seen_at = now(), occurrences =
                    okg.entity_resolution_rejections.occurrences + 1
                """,
                (
                    self.deployment_name,
                    self.branch_name,
                    subtype,
                    normalized_key,
                    raw_name,
                    reason,
                    policy.fingerprint(),
                    source_node_ref,
                ),
            )
        raise EntityResolutionRejected(
            f"entity-resolution post-filter rejected {raw_name!r}: {reason}",
            data={
                "subtype": subtype,
                "normalized_key": normalized_key,
                "reason": reason,
            },
        )


__all__ = [
    "ESCALATOR_REQUIRED_PARAMETERS",
    "IDENTITY_ALGORITHM_VERSION",
    "EntityResolutionAmbiguous",
    "EntityResolutionError",
    "EntityResolutionEscalator",
    "EntityResolutionRejected",
    "EntityResolutionStage",
    "EntityResolutionUnavailable",
    "ResolutionDecision",
    "ResolutionStatus",
    "mint_entity_id",
    "normalize_entity_name",
]
