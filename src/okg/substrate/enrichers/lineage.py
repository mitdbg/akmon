"""Lineage declarations for publisher-run enrichers.

This is the slice-5 adapter between the legacy enricher protocol and the
maintained-derivation registry vocabulary. It intentionally stops short of
global ``DerivationRegistry`` admission: judgment governance still needs the
projection-parity oracle and supersession override path from the next task.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from okg.substrate.derived_artifacts.dependency_index import (
    INDEXED_DEPENDENCY_KINDS,
)
from okg.substrate.derived_artifacts.relations import stable_hash
from okg.substrate.derived_artifacts.scheduler import LineageInvalidation


LINEAGE_STATUS_AVAILABLE = "available"
LINEAGE_STATUS_UNAVAILABLE = "unavailable"
MISSING_DEPENDENCY_DECLARATION = "missing_dependency_declaration"
DIRTY_SCOPE_UNAVAILABLE = "dirty_scope_unavailable"
ENRICHER_LINEAGE_SCHEMA = "okg.enricher_output_lineage.v1"

# Issue #1528 (follow-up to #1330): an enricher whose INPUT source run is
# still held off the `default` branch must not run. The publisher already
# excludes a quarantined producer's OWN output from projection (the
# derived-fact loader predicate ``status IN ('rejected','failed',
# 'quarantined')``); the transitive step was missing, so an enricher reading a
# held source kept emitting edges against nodes that were never admitted.
#
# WHAT ROUTES A RUN OFF `default` -- the admission MODE, not the status.
# ``graph_branch_for_admission_mode`` (admission.py:76-80) sends ``review``
# and ``quarantine`` to ``QUARANTINE_BRANCH`` and everything else to
# ``default``.
#
# ``okg.admission_envelopes`` DOES carry a ``target_branch_name`` column
# (schema.sql:1223-1224, AKMON Phase B), but it cannot be read here: ingest's
# ``ensure_envelope`` call for a source sync (``ingest/service.py:2904-2915``)
# never passes it, so every source_sync envelope leaves it NULL and every
# reader that does ``COALESCE(target_branch_name, 'default')`` sees `default`
# -- including a run whose facts only ever existed on ``quarantine:review``.
# That silent mislabel is what makes the mode the only trustworthy signal
# here, and it is also why the publish sweep reaches the envelopes described
# below. Populating the column for source_sync belongs to its own change.
#
# WHEN A HELD RUN IS RELEASED. Only two things prove an enricher may run:
#
#   * The rows were APPLIED onto `default`. ``_apply_quarantined_source_run``
#     (runtime/admission_workflows.py:1408) copies the quarantine-branch facts
#     onto `default`, and the accept path stamps its counts into the envelope
#     verdict (``node_facts_inserted`` / ``edge_facts_inserted``,
#     admission_workflows.py:1070-1092). Three call sites write that stamp,
#     but the other two are fast-track submits (admission_workflows.py:825
#     and exchange/admission.py:412), both reached only under
#     ``admission_mode == "fast_track"``. The mode filter in the query below
#     (``admission_mode = ANY(held_modes)``, ~:303) excludes those rows
#     structurally, so within a held-mode (``review``/``quarantine``)
#     envelope the decide/accept path is the ONLY writer -- and the stamp is
#     the only positive, per-envelope evidence of an apply, so it is what
#     this predicate reads.
#   * The rows are NEVER COMING. ``rejected`` and ``failed`` are terminal:
#     ``_drop_quarantined_source_run`` retracts the run from the quarantine
#     branch. Holding an enricher on a run that will never return would wedge
#     it forever, so those release it.
#
# WHY STATUS ALONE IS NOT PROOF, and why `quarantine` mode is stricter than
# `review` mode. Since issue #1752 the ingest fence
# (``admission_mode_awaits_decision`` in ``_finalize_source_admission``)
# finalizes BOTH held modes ``pending`` to wait for ``okg admission decide``.
# Before that fix only ``review`` was fenced: a ``quarantine`` run fell
# through to ``transition_envelope(status='accepted')`` although its rows
# only ever existed on ``quarantine:review`` and no apply ever ran. Its refs
# were attached before the fence, so that ``quarantine``/``accepted``
# envelope satisfied both publish sweeps (``_mark_generation_published`` and
# ``_mark_generation_blocked`` share the guard ``admission_mode NOT IN
# ('review','quarantine') OR status = 'accepted'``), and one publish later it
# read ``quarantine``/``published`` (or ``blocked``) with its rows still
# stranded off `default` -- the #1330 shape. Envelopes written before the
# fix keep that shape until #2732 recovers them, so for ``quarantine`` mode
# NO status is proof: ``pending``, ``quarantined``, ``accepted``,
# ``published`` and ``blocked`` all hold, and only ``rejected``/``failed``
# or a verdict apply-stamp releases. A post-fix run is accepted only through
# decide, which writes the stamp, so the strict rule costs it nothing.
#
# For ``review`` mode the statuses beyond ``pending``/``quarantined`` ARE
# reachable only through an apply or through the zero-fact path (which
# accepts a run that wrote nothing and attaches no refs), so they release.
# That asymmetry is deliberate and tested.
#
# OPERATOR WEDGE, disclosed rather than hidden (issue #1528 F2). ``okg
# admission decide`` moves only a ``pending`` envelope: the accept path
# short-circuits to ``already_decided`` for ``accepted``, ``published``,
# ``blocked``, ``quarantined`` and ``failed``, and a reject on a
# non-``pending`` envelope short-circuits the same way
# (``runtime/admission_workflows.py``, ``_run_decide``). A ``pending`` hold
# in either held mode is therefore decidable. A ``quarantined`` envelope in
# either mode, and a ``quarantine``-mode envelope finalized ``accepted``
# before #1752, are not: such a hold clears only when a LATER sync of that
# source lands in a non-held mode (fast_track/direct), or through the #2732
# recovery. The console `next_check` and `okg doctor --check
# derived-admission` both say this rather than pointing the operator at a
# command that cannot work.
_HELD_ADMISSION_MODES = ("review", "quarantine")
# Statuses that leave a `review`-mode run undecided and therefore held.
_UNDECIDED_ADMISSION_STATUSES = ("pending", "quarantined")
# Terminal statuses: the run's rows were dropped and are never coming.
_DROPPED_ADMISSION_STATUSES = ("rejected", "failed")
# The admission mode for which no status is proof of an apply (see above).
_NO_STATUS_PROOF_MODE = "quarantine"

# The reason recorded on the withheld run. ``WITHHELD_INPUT_SOURCE_HELD``
# is the operator-facing reason code on the derived-verdict event; the status
# prefix is what the publisher writes to ``okg.enricher_progress.last_status``
# and what ``okg doctor --check derived-admission`` reads back to name the
# state. Both live here so the writer and the reader cannot drift apart.
WITHHELD_INPUT_SOURCE_HELD = "withheld_source_held_for_admission"
WITHHELD_INPUT_SOURCE_HELD_STATUS_PREFIX = "withheld:source_held_for_admission:"

# Issue #1528 F5. A declared source id that matches no registered source
# matches no envelope either, so the gate above would pass VACUOUSLY and the
# enricher would ship ungated with no signal. This is the refusal reason for
# that, raised at declaration-resolution time rather than discovered in
# production.
UNKNOWN_DECLARED_SOURCE = "unknown_declared_source"
UNKNOWN_DECLARED_SOURCE_STATUS_PREFIX = "withheld:unknown_declared_source:"

# Every reason the publisher may record as a WITHHELD run, newest first. The
# publisher writes these, `okg doctor --check derived-admission` and the
# operator console read them back, and the `enricher_progress_last_status_chk`
# constraint admits them all through the single `withheld:%` arm -- so a new
# reason needs no migration, only an entry here.
_WITHHELD_STATUS_REASONS: tuple[tuple[str, str], ...] = (
    (WITHHELD_INPUT_SOURCE_HELD_STATUS_PREFIX, WITHHELD_INPUT_SOURCE_HELD),
    (UNKNOWN_DECLARED_SOURCE_STATUS_PREFIX, UNKNOWN_DECLARED_SOURCE),
)


def unknown_declared_source_status(sources: Sequence[str]) -> str:
    """The ``enricher_progress.last_status`` value for a refused declaration."""
    return (
        UNKNOWN_DECLARED_SOURCE_STATUS_PREFIX + ",".join(sorted(sources))
    )


def withheld_reason_from_status(last_status: str | None) -> tuple[str, str] | None:
    """``(reason_code, detail)`` for a withheld progress status, else ``None``.

    The single reader for `enricher_progress.last_status` withholding, shared
    by `okg doctor` and the operator console so the two cannot disagree about
    what a status means. ``detail`` is the comma-joined source list the
    publisher recorded, or ``""`` when it recorded none.
    """
    status = str(last_status or "")
    for prefix, reason in _WITHHELD_STATUS_REASONS:
        if status.startswith(prefix):
            return reason, status[len(prefix):]
    return None


def is_withheld_status(last_status: str | None) -> bool:
    """Whether this progress status records a withheld (not failed) run."""
    return withheld_reason_from_status(last_status) is not None


def withheld_input_source_status(sources: Sequence[str]) -> str:
    """The ``enricher_progress.last_status`` value for a withheld run.

    Names the sources that caused the withholding so the progress row is
    self-explaining; ``okg doctor`` recovers the reason from the prefix.
    """
    return (
        WITHHELD_INPUT_SOURCE_HELD_STATUS_PREFIX + ",".join(sorted(sources))
    )


def source_alias_index(entries: Any) -> dict[str, str]:
    """Map every accepted alias for a registered source to its ownership id.

    Issue #1528 F5. Ingest writes ``okg.admission_envelopes.source =
    entry.ownership_id`` (``ingest/service.py:2914``), but the registry NAME
    is what an author naturally writes in ``reads_sources`` -- and the two
    differ for real sources: a ``code_repos`` block generates a source named
    ``git-files-<slug>`` whose ownership id is ``<slug>.git-files``
    (``sources/registry.py:806-812, 875-890``). Both are accepted and both
    normalize to the ownership id, which is the only value the envelope query
    can match.

    ``entries`` is any iterable of registry entries exposing ``name`` and
    ``ownership_id`` (``sources.registry.SourceEntry``), or of
    ``(name, ownership_id)`` pairs.
    """
    index: dict[str, str] = {}
    for entry in entries or ():
        if isinstance(entry, (tuple, list)) and len(entry) == 2:
            name, ownership_id = str(entry[0]), str(entry[1])
        else:
            name = str(getattr(entry, "name", "") or "")
            ownership_id = str(getattr(entry, "ownership_id", "") or "")
        if not ownership_id:
            continue
        index[ownership_id] = ownership_id
        if name:
            index[name] = ownership_id
    return index


def resolve_declared_source_ids(
    declaration: EnricherDependencyDeclaration,
    alias_index: Mapping[str, str],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Normalize a declaration's source ids onto registered ownership ids.

    Issue #1528 F5. Returns ``(resolved, unknown)``: the sorted ownership ids
    the admission query should be asked about, and the sorted declared ids
    that resolve to no registered source. A non-empty ``unknown`` is a
    REFUSAL, not a warning -- ``held_input_sources`` would silently match zero
    envelopes for such an id and the enricher would run ungated, which is the
    failure this whole change exists to prevent.

    The caller must have a real registry to pass. An empty ``alias_index``
    means "no registry was loadable", and this function cannot tell that apart
    from "the registry is empty"; callers therefore skip validation entirely
    rather than calling with an empty index, and say so. See
    ``Publisher._resolve_declared_sources``.
    """
    resolved: set[str] = set()
    unknown: set[str] = set()
    for declared in declaration.source_ids:
        ownership_id = alias_index.get(str(declared))
        if ownership_id is None:
            unknown.add(str(declared))
        else:
            resolved.add(ownership_id)
    return tuple(sorted(resolved)), tuple(sorted(unknown))


def held_input_sources(
    conn: Any,
    declaration: EnricherDependencyDeclaration,
    *,
    deployment_name: str,
    source_ids: Sequence[str] | None = None,
) -> tuple[str, ...]:
    """Declared input sources whose LATEST run is still held off `default`.

    Returns the sorted source ids whose most recent ``source_sync`` admission
    envelope for this deployment was routed to the quarantine branch and has
    not been proven applied onto `default`. See the module note above for
    which mode/status combinations that is and why ``quarantine`` mode is
    stricter than ``review`` mode.

    ``source_ids`` overrides the declaration's own ids with the ownership ids
    resolved by ``resolve_declared_source_ids`` (issue #1528 F5). Ingest
    writes the OWNERSHIP id into ``admission_envelopes.source``, so passing
    unresolved registry names here would match nothing. When omitted the
    declaration's ids are used verbatim, which is correct only when they are
    already ownership ids.

    LATEST run, not any run. Envelopes are per-run: ``ingest/service.py``
    folds ``replay_key`` into the idempotency key, so every sync of a source
    inserts a NEW envelope rather than updating one. A ``SELECT DISTINCT ...
    WHERE status = ...`` over all of them would let a single historical held
    run pin the enricher forever, even after many later runs were admitted
    onto `default`. The question the publisher needs answered is about the
    source's CURRENT state -- "is what is on `default` for this source
    complete as of its last run?" -- so only the latest envelope per
    (deployment, source) can answer it.

    ORDERING LIMIT (issue #1528 F8). ``okg.admission_envelopes`` has no
    monotonic row id: ``admission_id`` is a random v4 UUID and there is no
    sequence column, so ``created_at DESC`` is the only real recency key.
    Two envelopes for the same source created in the SAME transaction share
    ``created_at`` (it defaults to ``now()``, the transaction timestamp) and
    their relative order is then arbitrary; ``admission_id DESC`` only makes
    that arbitrary choice STABLE across repeated reads, it does not make it
    correct. In practice one source sync writes one envelope per transaction,
    so the tie does not arise; if a future writer batches several runs of one
    source into a single transaction, this predicate must gain a real
    ordering column first.

    Returns ``()`` when the enricher declared no input sources. That is a real
    limit, not an oversight: gating is only as good as the declaration, so an
    enricher that does not declare ``reads_sources``/``reads_source_ids`` is
    NOT withheld. Declaring inputs is what buys the protection.
    """
    lookup = sorted(
        {str(s) for s in source_ids}
        if source_ids is not None
        else declaration.source_ids
    )
    if not lookup:
        return ()
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH latest AS (
                SELECT DISTINCT ON (source)
                       source, status, admission_mode,
                       (verdict->'node_facts_inserted') IS NOT NULL
                         AS applied_to_default
                  FROM okg.admission_envelopes
                 WHERE deployment_name = %(deployment)s
                   AND intent = 'source_sync'
                   AND source = ANY(%(sources)s)
                 ORDER BY source, created_at DESC, admission_id DESC
            )
            SELECT source
              FROM latest
             WHERE admission_mode = ANY(%(held_modes)s)
               -- Terminal: the rows were dropped and are never coming.
               AND NOT (status = ANY(%(dropped)s))
               -- Positive proof the rows reached `default`.
               AND NOT applied_to_default
               AND (
                     -- `quarantine` mode: no status is proof of an apply,
                     -- because a run finalized before issue #1752 reads
                     -- `accepted`, then `published`/`blocked`, with its
                     -- rows still off `default`.
                     admission_mode = %(no_status_proof_mode)s
                     -- `review` mode: only the undecided statuses hold.
                     OR status = ANY(%(undecided)s)
                   )
            """,
            {
                "deployment": deployment_name,
                "sources": lookup,
                "held_modes": list(_HELD_ADMISSION_MODES),
                "undecided": list(_UNDECIDED_ADMISSION_STATUSES),
                "dropped": list(_DROPPED_ADMISSION_STATUSES),
                "no_status_proof_mode": _NO_STATUS_PROOF_MODE,
            },
        )
        rows = cur.fetchall()
    return tuple(sorted(str(row[0]) for row in rows))


@dataclass(frozen=True)
class EnricherDependencyDeclaration:
    """Selector-level dependencies for one enricher.

    ``kinds`` names the reverse-index channels. The selector fields keep those
    channels narrow enough for planning: a graph-node invalidation matches an
    enricher by subtype, and a graph-edge invalidation by edge type.
    """

    kinds: frozenset[str]
    node_subtypes: frozenset[str] = field(default_factory=frozenset)
    edge_types: frozenset[str] = field(default_factory=frozenset)
    source_ids: frozenset[str] = field(default_factory=frozenset)
    component_id: str | None = None
    component_hash: str | None = None
    status: str = LINEAGE_STATUS_AVAILABLE
    fallback_reason: str | None = None

    @property
    def available(self) -> bool:
        return self.status == LINEAGE_STATUS_AVAILABLE

    @property
    def force_full(self) -> bool:
        return self.fallback_reason is not None

    def to_dict(self) -> dict[str, Any]:
        return {
            "kinds": sorted(self.kinds),
            "node_subtypes": sorted(self.node_subtypes),
            "edge_types": sorted(self.edge_types),
            "source_ids": sorted(self.source_ids),
            "component_id": self.component_id,
            "component_hash": self.component_hash,
            "status": self.status,
            "fallback_reason": self.fallback_reason,
            "force_full": self.force_full,
        }


@dataclass(frozen=True)
class EnricherLineageInvalidation:
    dependency_kind: str
    dependency_ref: str
    node_subtype: str | None = None
    edge_type: str | None = None
    source_id: str | None = None
    dirty_reason: str = "changed"

    def __post_init__(self) -> None:
        if self.dependency_kind not in INDEXED_DEPENDENCY_KINDS:
            raise ValueError(
                "dependency_kind must be one of "
                f"{sorted(INDEXED_DEPENDENCY_KINDS)}; "
                f"got {self.dependency_kind!r}"
            )
        if not self.dependency_ref:
            raise ValueError("dependency_ref is required")

    def to_key(self) -> dict[str, str]:
        return {
            "dependency_kind": self.dependency_kind,
            "dependency_ref": self.dependency_ref,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            **self.to_key(),
            "node_subtype": self.node_subtype,
            "edge_type": self.edge_type,
            "source_id": self.source_id,
            "dirty_reason": self.dirty_reason,
        }


@dataclass(frozen=True)
class EnricherLineagePlan:
    declaration: EnricherDependencyDeclaration
    dirty: bool
    matched_invalidations: tuple[EnricherLineageInvalidation, ...] = ()

    @property
    def lineage_status(self) -> str:
        return self.declaration.status

    @property
    def fallback_reason(self) -> str | None:
        return self.declaration.fallback_reason

    @property
    def force_full(self) -> bool:
        return self.declaration.force_full

    @property
    def dependency_keys(self) -> tuple[dict[str, str], ...]:
        return tuple(item.to_key() for item in self.matched_invalidations)


@dataclass(frozen=True)
class EnricherRunLineageEnvelope:
    """Persistable adapter payload for an enricher edge-fact run."""

    enricher_name: str
    derivation_id: str
    generation_id: int
    catalog_version_id: int | None
    component_id: str
    component_hash: str
    declaration: EnricherDependencyDeclaration
    plan: EnricherLineagePlan
    output_id: str
    n_edges_emitted: int = 0
    n_edges_skipped: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": ENRICHER_LINEAGE_SCHEMA,
            "output_kind": "edge_fact_stream",
            "output_id": self.output_id,
            "materialization": {
                "kind": "edge_facts",
                "projection": "project_new_edges",
            },
            "enricher_name": self.enricher_name,
            "derivation_id": self.derivation_id,
            "generation_id": self.generation_id,
            "catalog_version_id": self.catalog_version_id,
            "component": {
                "component_id": self.component_id,
                "component_hash": self.component_hash,
                "component_kind": "enricher",
            },
            "lineage_status": self.plan.lineage_status,
            "fallback_reason": self.plan.fallback_reason,
            "force_full": self.plan.force_full,
            "dirty": self.plan.dirty,
            "dependency_declaration": self.declaration.to_dict(),
            "dependency_keys": list(self.plan.dependency_keys),
            "matched_invalidations": [
                item.to_dict() for item in self.plan.matched_invalidations
            ],
            "edge_counts": {
                "emitted": self.n_edges_emitted,
                "skipped": self.n_edges_skipped,
            },
        }


def enricher_derivation_id(enricher: Any) -> str:
    return f"_enricher:{_enricher_name(enricher)}"


def declare_enricher_dependencies(
    enricher: Any,
) -> EnricherDependencyDeclaration:
    explicit = getattr(enricher, "lineage_dependencies", None)
    if explicit is not None:
        if isinstance(explicit, EnricherDependencyDeclaration):
            return explicit
        if not isinstance(explicit, Mapping):
            return _missing_declaration(enricher)
        return _declaration_from_mapping(enricher, explicit)

    node_subtypes = _string_set(getattr(enricher, "reads_subtypes", ()))
    edge_types = _string_set(getattr(enricher, "reads_edge_types", ()))
    source_ids = _string_set(
        getattr(enricher, "reads_sources", ())
        or getattr(enricher, "reads_source_ids", ())
    )
    kinds: set[str] = set()
    if node_subtypes:
        kinds.add("graph_node")
    if edge_types:
        kinds.add("graph_edge")
    if source_ids:
        kinds.update({"source_record", "source_content_hash"})
    if not kinds:
        return _missing_declaration(enricher)
    component_id = enricher_derivation_id(enricher)
    component_hash = _component_hash(enricher, node_subtypes, edge_types, source_ids)
    return EnricherDependencyDeclaration(
        kinds=frozenset(kinds | {"component_hash"}),
        node_subtypes=frozenset(node_subtypes),
        edge_types=frozenset(edge_types),
        source_ids=frozenset(source_ids),
        component_id=component_id,
        component_hash=component_hash,
    )


def plan_enricher_lineage(
    declaration: EnricherDependencyDeclaration,
    invalidations: Sequence[EnricherLineageInvalidation] | None,
) -> EnricherLineagePlan:
    if not declaration.available:
        return EnricherLineagePlan(
            declaration=declaration,
            dirty=True,
            matched_invalidations=(),
        )
    if invalidations is None:
        unavailable = EnricherDependencyDeclaration(
            kinds=declaration.kinds,
            node_subtypes=declaration.node_subtypes,
            edge_types=declaration.edge_types,
            source_ids=declaration.source_ids,
            component_id=declaration.component_id,
            component_hash=declaration.component_hash,
            status=LINEAGE_STATUS_UNAVAILABLE,
            fallback_reason=DIRTY_SCOPE_UNAVAILABLE,
        )
        return EnricherLineagePlan(
            declaration=unavailable,
            dirty=True,
            matched_invalidations=(),
        )
    matched = tuple(
        invalidation
        for invalidation in invalidations
        if _matches(declaration, invalidation)
    )
    return EnricherLineagePlan(
        declaration=declaration,
        dirty=bool(matched),
        matched_invalidations=matched,
    )


def invalidations_from_chronos_dirty_scope(
    scope: Any,
) -> tuple[EnricherLineageInvalidation, ...] | None:
    if scope is None:
        return None
    nodes_by_subtype = scope[0] if len(scope) > 0 else {}
    edges_by_type = scope[1] if len(scope) > 1 else {}
    edge_table_available = bool(scope[2]) if len(scope) > 2 else False
    invalidations: list[EnricherLineageInvalidation] = []
    for subtype, node_ids in nodes_by_subtype.items():
        for node_id in node_ids:
            invalidations.append(
                EnricherLineageInvalidation(
                    dependency_kind="graph_node",
                    dependency_ref=str(node_id),
                    node_subtype=str(subtype),
                    dirty_reason="graph_node_changed",
                )
            )
    if edge_table_available:
        for edge_type, edge_ids in edges_by_type.items():
            for edge_id in edge_ids:
                invalidations.append(
                    EnricherLineageInvalidation(
                        dependency_kind="graph_edge",
                        dependency_ref=str(edge_id),
                        edge_type=str(edge_type),
                        dirty_reason="graph_edge_changed",
                    )
                )
    return tuple(invalidations)


def invalidations_from_change_frontier(
    invalidations: Sequence[LineageInvalidation],
) -> tuple[EnricherLineageInvalidation, ...]:
    """Adapt derived-artifact frontier keys to enricher lineage keys."""

    adapted: list[EnricherLineageInvalidation] = []
    for invalidation in invalidations:
        source_id = None
        if invalidation.dependency_kind in {
            "source_record",
            "source_content_hash",
        }:
            source_id = invalidation.dependency_ref.split(":", 1)[0] or None
        adapted.append(
            EnricherLineageInvalidation(
                dependency_kind=invalidation.dependency_kind,
                dependency_ref=invalidation.dependency_ref,
                source_id=source_id,
                dirty_reason=invalidation.dirty_reason,
            )
        )
    return tuple(adapted)


def invalidations_from_source_delta_batches(
    batches: Sequence[Any],
) -> tuple[EnricherLineageInvalidation, ...] | None:
    """Build enricher invalidations from source-run delta evidence."""

    from okg.substrate.derived_artifacts.change_frontier import (
        build_change_frontier,
    )

    frontier = build_change_frontier(source_delta_batches=batches)
    if frontier.skipped_source_ids:
        return None
    return invalidations_from_change_frontier(frontier.invalidations)


def build_enricher_run_lineage_envelope(
    enricher: Any,
    *,
    generation_id: int,
    catalog_version_id: int | None,
    plan: EnricherLineagePlan,
    n_edges_emitted: int = 0,
    n_edges_skipped: int = 0,
) -> EnricherRunLineageEnvelope:
    declaration = plan.declaration
    component_id = declaration.component_id or enricher_derivation_id(enricher)
    component_hash = declaration.component_hash or _component_hash(
        enricher,
        declaration.node_subtypes,
        declaration.edge_types,
        declaration.source_ids,
    )
    name = _enricher_name(enricher)
    return EnricherRunLineageEnvelope(
        enricher_name=name,
        derivation_id=enricher_derivation_id(enricher),
        generation_id=generation_id,
        catalog_version_id=catalog_version_id,
        component_id=component_id,
        component_hash=component_hash,
        declaration=declaration,
        plan=plan,
        output_id=f"_enricher:{name}:generation:{generation_id}:edge_facts",
        n_edges_emitted=int(n_edges_emitted),
        n_edges_skipped=int(n_edges_skipped),
    )


def _matches(
    declaration: EnricherDependencyDeclaration,
    invalidation: EnricherLineageInvalidation,
) -> bool:
    kind = invalidation.dependency_kind
    if kind not in declaration.kinds:
        return False
    if kind == "graph_node":
        return (
            bool(declaration.node_subtypes)
            and invalidation.node_subtype in declaration.node_subtypes
        )
    if kind == "graph_edge":
        return (
            bool(declaration.edge_types)
            and invalidation.edge_type in declaration.edge_types
        )
    if kind in {"source_record", "source_content_hash"}:
        return (
            bool(declaration.source_ids)
            and (
                invalidation.source_id in declaration.source_ids
            or any(
                invalidation.dependency_ref.startswith(f"{source_id}:")
                for source_id in declaration.source_ids
            )
            )
        )
    if kind == "component_hash":
        return invalidation.dependency_ref == declaration.component_hash
    return True


def _declaration_from_mapping(
    enricher: Any,
    raw: Mapping[str, Any],
) -> EnricherDependencyDeclaration:
    kinds = _string_set(raw.get("kinds", ()))
    node_subtypes = _string_set(raw.get("node_subtypes", ()))
    edge_types = _string_set(raw.get("edge_types", ()))
    source_ids = _string_set(raw.get("source_ids", ()))
    kinds.update(
        kind
        for condition, kind in (
            (node_subtypes, "graph_node"),
            (edge_types, "graph_edge"),
            (source_ids, "source_record"),
        )
        if condition
    )
    kinds = {kind for kind in kinds if kind in INDEXED_DEPENDENCY_KINDS}
    if not kinds:
        return _missing_declaration(enricher)
    if "graph_node" in kinds and not node_subtypes:
        return _missing_declaration(enricher)
    if "graph_edge" in kinds and not edge_types:
        return _missing_declaration(enricher)
    if (
        {"source_record", "source_content_hash"} & kinds
        and not source_ids
    ):
        return _missing_declaration(enricher)
    if source_ids:
        kinds.add("source_content_hash")
    component_id = str(raw.get("component_id") or enricher_derivation_id(enricher))
    component_hash = str(
        raw.get("component_hash")
        or _component_hash(enricher, node_subtypes, edge_types, source_ids)
    )
    if not component_hash.startswith(("sha256:", "dep:")):
        component_hash = f"sha256:{component_hash}"
    return EnricherDependencyDeclaration(
        kinds=frozenset(kinds | {"component_hash"}),
        node_subtypes=frozenset(node_subtypes),
        edge_types=frozenset(edge_types),
        source_ids=frozenset(source_ids),
        component_id=component_id,
        component_hash=component_hash,
    )


def _missing_declaration(enricher: Any) -> EnricherDependencyDeclaration:
    return EnricherDependencyDeclaration(
        kinds=frozenset(),
        component_id=enricher_derivation_id(enricher),
        component_hash=_component_hash(enricher, set(), set(), set()),
        status=LINEAGE_STATUS_UNAVAILABLE,
        fallback_reason=MISSING_DEPENDENCY_DECLARATION,
    )


def _component_hash(
    enricher: Any,
    node_subtypes: Sequence[str] | frozenset[str] | set[str],
    edge_types: Sequence[str] | frozenset[str] | set[str],
    source_ids: Sequence[str] | frozenset[str] | set[str],
) -> str:
    payload = {
        "class": f"{enricher.__class__.__module__}.{enricher.__class__.__qualname__}",
        "name": _enricher_name(enricher),
        "node_subtypes": sorted(str(item) for item in node_subtypes),
        "edge_types": sorted(str(item) for item in edge_types),
        "source_ids": sorted(str(item) for item in source_ids),
        "requires_narrowings": [
            [str(src), str(edge), str(dst)]
            for src, edge, dst in (
                getattr(enricher, "requires_narrowings", ()) or ()
            )
        ],
    }
    return "sha256:" + stable_hash(payload)


def _enricher_name(enricher: Any) -> str:
    return str(getattr(enricher, "name", enricher.__class__.__name__))


def _string_set(value: Any) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, str):
        return {value} if value else set()
    try:
        return {str(item) for item in value if str(item)}
    except TypeError:
        return set()


__all__ = [
    "DIRTY_SCOPE_UNAVAILABLE",
    "ENRICHER_LINEAGE_SCHEMA",
    "EnricherDependencyDeclaration",
    "EnricherLineageInvalidation",
    "EnricherLineagePlan",
    "EnricherRunLineageEnvelope",
    "LINEAGE_STATUS_AVAILABLE",
    "LINEAGE_STATUS_UNAVAILABLE",
    "MISSING_DEPENDENCY_DECLARATION",
    "build_enricher_run_lineage_envelope",
    "declare_enricher_dependencies",
    "enricher_derivation_id",
    "invalidations_from_change_frontier",
    "invalidations_from_chronos_dirty_scope",
    "invalidations_from_source_delta_batches",
    "plan_enricher_lineage",
]
