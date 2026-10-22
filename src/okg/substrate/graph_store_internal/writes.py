"""Fact-write boundary for the Chronos graph store."""
from __future__ import annotations

from contextlib import nullcontext
from dataclasses import dataclass, field as dataclass_field
from datetime import datetime
import json
import logging
import time
from typing import Any, Iterable, Mapping

from okg.substrate.errors import SubstrateError
from okg.substrate.fact_equivalence import (
    MISSING,
    canonical,
    compare_attrs,
    edge_columns_differing,
)
from okg.substrate.graph_store_internal.common import (
    DEFAULT_BRANCH,
    EDGE_TABLE,
    NODE_TABLE,
    GraphEditResult,
    dedupe_rows_last,
    edge_id,
    md5_text,
)
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.searchable_text import (
    stamp_searchable_text,
    unstamp_without_declaration,
)
from okg.substrate.nomos.read_class import (
    KnownReadClasses,
    known_read_classes,
    stamp_rows,
)
from okg.substrate.nomos.source_lineage import (
    LINEAGE_ATTR,
    attrs_with_lineage,
    combine_source_lineages,
    lineage_for_row,
    source_ids_from_lineage,
    stamp_attrs_with_source_lineage,
)

_LOCATION_ONLY_NODE_ATTRS = frozenset({
    "start_line",
    "end_line",
    "start_col",
    "end_col",
    "definition_range",
    "selection_range",
})

_METADATA_ONLY_NODE_ATTRS = frozenset({
    LINEAGE_ATTR,
})

_PAIRED_WORKSPACE_KIND = "okg_paired_workspace"
_PAIRED_WORKSPACE_WRITABLE_STATUSES = frozenset({
    "staged",
    "active",
    "checkpointed",
})
_PAIRED_WORKSPACE_TERMINAL_STATUSES = frozenset({
    "handed_off",
    "retired",
    "cleaning",
    "closed",
    "abandoned",
})
_PAIRED_WORKSPACE_LIFECYCLE_STATUSES = (
    _PAIRED_WORKSPACE_WRITABLE_STATUSES
    | _PAIRED_WORKSPACE_TERMINAL_STATUSES
)

# What a node retraction does to a shared node (issue #1901); see
# ChronosGraphWriteMixin.apply_facts.
NODE_RETRACTION_MODES = frozenset({"handoff", "purge"})



#: Advisory-lock key of the edge ranking (item 92a): ranked writes take it
#: shared before their branch fence, a ranking change takes it exclusively.
_EDGE_RANKING_LOCK = ("okg.edge_precedence", "ranking")


def _add_ms(timings_ms: dict[str, float], label: str, started: float) -> None:
    """Add the milliseconds since ``started`` to ``timings_ms[label]``."""
    elapsed = round((time.perf_counter() - started) * 1000, 3)
    timings_ms[label] = round(timings_ms.get(label, 0.0) + elapsed, 3)


def _first_value(row: Any) -> Any:
    """The first column of a fetched row (a mapping or a sequence)."""
    if isinstance(row, Mapping):
        return next(iter(row.values()))
    return row[0]


def _is_lock_timeout(exc: BaseException) -> bool:
    """Whether ``exc`` (or what it wraps) is PostgreSQL's lock_timeout."""
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if getattr(current, "sqlstate", None) == "55P03":
            return True
        if type(current).__name__ == "LockNotAvailable":
            return True
        current = current.__cause__ or current.__context__
    return False


@dataclass
class _RankedEdgeWrite:
    """What ``_apply_edge_declarers`` decided for one write's ranked edges."""

    outcome: Any
    # The stored rows of every touched ranked edge, read after the fence.
    current_edges: dict[int, Mapping[str, Any]] = dataclass_field(default_factory=dict)
    # Ranked edges incident to nodes this write deletes or retracts.
    incident_rows: list[dict[str, Any]] = dataclass_field(default_factory=list)
    # Ranked edges purged with a removed endpoint node.
    purged_edges: int = 0


@dataclass(frozen=True)
class _ApplyOptions:
    """The options an apply was prepared under; a reuse must match them."""

    branch: str
    source: str | None
    policy_fingerprint: str | None
    run_id: str | None
    observed_at: datetime | None
    strict_endpoints: bool
    delete_incident_edges: bool
    derived_reapplication: bool
    node_retraction: str
    row_provenance: Mapping[str, Mapping[str, Any]] | None = None


#: Keys of one ``row_provenance`` entry. ``lineage`` may be absent, which
#: leaves the row with no lineage attribute.
ROW_PROVENANCE_KEYS = frozenset({
    "source", "run_id", "observed_at", "expected_source", "lineage",
})


def _validate_row_provenance(
    facts: list[NodeFact | EdgeFact],
    row_provenance: Mapping[str, Mapping[str, Any]],
    *,
    source: str | None,
    derived_reapplication: bool,
) -> None:
    """Refuse a provenance-preserving rewrite that is not one (issue #3318).

    ``row_provenance`` rewrites stored nodes in place: every fact is a node
    insert of a key that names its own entry, and the batch carries no
    source of its own. A retraction, an edge, a substitute declaration or a
    batch source would each let the rewrite move or drop ownership, which
    is exactly what it exists to prevent.
    """
    if source is not None or derived_reapplication:
        raise ValueError(
            "row_provenance writes carry no batch source and are not a "
            "derived re-application"
        )
    for fact in facts:
        if not isinstance(fact, NodeFact):
            raise ValueError("row_provenance writes rewrite nodes only")
        if fact.op.upper() != "I" or fact.ownership_mode != "authoritative":
            raise ValueError(
                "row_provenance writes are authoritative node inserts only"
            )
        entry = row_provenance.get(str(fact.node_id))
        if entry is None:
            raise ValueError(
                f"row_provenance has no entry for node {fact.node_id!r}"
            )
        unknown = set(entry) - ROW_PROVENANCE_KEYS
        missing = {"source", "run_id", "observed_at", "expected_source"} - set(entry)
        if unknown or missing:
            raise ValueError(
                f"row_provenance entry for {fact.node_id!r} is malformed: "
                f"unknown {sorted(unknown)}, missing {sorted(missing)}"
            )


def _apply_options(**values: Any) -> _ApplyOptions:
    return _ApplyOptions(**values)


@dataclass
class PreparedFactApply:
    """The read-and-compute half of one ``apply_facts`` call.

    Made by ``prepare_apply_facts``; consumed once by the write half. With a
    ``key_fingerprint`` it can be prepared before the publish lock and reused
    under it (issue 2941 fix 1): the fingerprint covers the physical rows of
    every key the preparation read, so an unchanged fingerprint under the
    branch write lock proves the reads would return the same rows now.
    """

    facts: tuple[NodeFact | EdgeFact, ...]
    options: _ApplyOptions
    key_fingerprint: tuple[Any, ...] | None
    session: Any
    timings_ms: dict[str, float]
    total_started: float
    node_upserts: list[dict[str, Any]]
    node_deletes: list[dict[str, Any]]
    edge_upserts: list[dict[str, Any]]
    edge_deletes: list[dict[str, Any]]
    touched_nodes_by_subtype: dict[str, list[str]]
    touched_edges_by_type: dict[str, list[int]]
    touched_edge_endpoints_by_type: dict[str, list[dict[str, Any]]]
    searchable_fields_cache: dict[str, list[str]]
    batch_node_upsert_ids: set[str]
    prior_nodes_by_id: dict[str, Any]
    prior_edges_by_id: dict[int, Any]
    delete_node_ids: set[str]
    delete_edge_ids: set[int]
    original_node_upsert_count: int
    substitute_node_upserts: list[dict[str, Any]]
    scoped_node_deletes_for_apply: list[dict[str, Any]]
    scoped_edge_deletes_for_apply: list[dict[str, Any]]
    node_deletes_for_incident_edges: list[dict[str, Any]]
    incident_edge_deletes: list[dict[str, Any]]
    coalesced_node_delete_ids: set[str]
    coalesced_edge_delete_ids: set[int]
    node_attr_changes_by_subtype: Any
    edge_attr_changes_by_type: Any
    delta_counts: dict[str, int]
    # Ranked edges (item 92a), decided while preparing; the write half takes
    # the ranking lock and refuses if the ranking changed since.
    edge_ranks: dict[str, dict[str, int]]
    ranking_relevant: bool
    ranked_types_for_source: set[str]
    ranked_edge_upserts: list[dict[str, Any]]
    ranked_edge_delete_ids: list[int]
    unranked_writes_of_ranked_types: list[int]
    used: bool = dataclass_field(default=False)
    # Issue #2941: the Nomos read classes of the rows to write, computed
    # while preparing ahead of the lock. The write half reuses a class only
    # for a row whose class inputs are unchanged.
    read_classes: KnownReadClasses = dataclass_field(default_factory=dict)
    # Issue #2941: the ranked-edge decision made while preparing ahead of
    # the lock, from the declarations and stored rows the key fingerprint
    # covers. The write half reuses it only for this verified state.
    ranked_decision: "_RankedEdgeWrite | None" = None


def _has_derived_node_lifecycle(row: Mapping[str, Any] | None) -> bool:
    if not row:
        return False
    source = str(row.get("source") or "")
    record = row.get("source_record_id")
    return source.startswith("_enricher:") or (
        isinstance(record, Mapping) and record.get("derived_support") is not None
    )


class ChronosGraphWriteMixin:
    """Graph fact application, lineage, endpoint validation, and write locks."""

    context: Any

    def apply_facts(
        self,
        facts: Iterable[NodeFact | EdgeFact],
        *,
        branch: str,
        source: str | None = None,
        policy_fingerprint: str | None = None,
        run_id: str | None = None,
        observed_at: datetime | None = None,
        strict_endpoints: bool = True,
        session: Any | None = None,
        delete_incident_edges: bool = True,
        derived_reapplication: bool = False,
        node_retraction: str = "handoff",
        prepared: PreparedFactApply | None = None,
        row_provenance: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> GraphEditResult:
        """Apply graph facts under one source attribution.

        The edge ranking (item 92a) is read before the write transaction to
        plan the write, then again under the ranking lock and the branch
        write fence. If a ranking change committed in between, the write is
        refused with :class:`EdgeRankingChangedDuringWrite` before it writes
        anything; it is not retried here, because the refusal rolls back a
        caller-owned transaction and only the caller can rerun its work.

        ``node_retraction`` decides what a node retraction does to a shared
        node (issue #1901). ``"handoff"``: the source withdraws its own
        declaration, and a row it owns passes to a surviving declarer or is
        deleted when none remains. ``"purge"``: the node itself is being
        removed (entity merge retiring the losing node), so every declaration
        of it is deleted and a row this source owns is deleted.

        ``derived_reapplication`` says these facts were already admitted
        and attributed by their emitting producer and are only being
        re-applied to the graph now (the publisher writing accepted
        derived fact-log rows into a checkpoint). It keeps the lineage
        each row already carries and runs the derived-provenance stamps.
        Without it the re-application would overwrite a derived node's
        inherited root sources with the emitting producer's own id, and
        the trust lattice would then read that producer's declared label
        instead of the root's (#1534).

        ``prepared`` (issue 2941 fix 1) is this call's read-and-compute
        half, made earlier by :meth:`prepare_apply_facts` for the same
        facts and options. It is used only after the write transaction has
        taken the branch write lock and found every row it read unchanged;
        otherwise that half is computed again under the lock, as without
        it. Either way the written rows and the result are the same.

        ``row_provenance`` (issue #3318) rewrites existing nodes in place
        without taking them from their owner: it maps each node id to the
        ``source``, ``run_id``, ``observed_at`` and ``lineage`` its row keeps,
        and the ``expected_source`` the stored row must still have under the
        branch write lock. Each fact supplies the attrs and its own record id
        and revision; the writer re-stamps only the searchable text and hash.
        ``source`` must be None: the batch owns nothing. The caller reads the
        rows it rewrites, so it must hold the branch write lock from that
        read to this write (the catalog restamp and its ownership repair
        both do); the ``expected_source`` check under the lock is only a
        backstop and compares the source alone. Used by the catalog
        searchable-text restamp and its ownership repair.
        """
        if node_retraction not in NODE_RETRACTION_MODES:
            raise ValueError(
                f"unsupported node_retraction: {node_retraction!r}; "
                f"expected one of {sorted(NODE_RETRACTION_MODES)}"
            )
        options = _apply_options(
            branch=branch, source=source,
            policy_fingerprint=policy_fingerprint, run_id=run_id,
            observed_at=observed_at, strict_endpoints=strict_endpoints,
            delete_incident_edges=delete_incident_edges,
            derived_reapplication=derived_reapplication,
            node_retraction=node_retraction,
            row_provenance=row_provenance,
        )
        if prepared is not None:
            return self._apply_verified_prepared(
                list(facts), prepared, options=options, session=session,
            )
        state = self.prepare_apply_facts(
            facts,
            branch=branch,
            source=source,
            policy_fingerprint=policy_fingerprint,
            run_id=run_id,
            observed_at=observed_at,
            strict_endpoints=strict_endpoints,
            session=session,
            delete_incident_edges=delete_incident_edges,
            derived_reapplication=derived_reapplication,
            node_retraction=node_retraction,
            row_provenance=row_provenance,
        )
        return self._write_prepared_apply(state)

    def prepare_apply_facts(
        self,
        facts: Iterable[NodeFact | EdgeFact],
        *,
        branch: str,
        source: str | None = None,
        policy_fingerprint: str | None = None,
        run_id: str | None = None,
        observed_at: datetime | None = None,
        strict_endpoints: bool = True,
        session: Any | None = None,
        delete_incident_edges: bool = True,
        derived_reapplication: bool = False,
        node_retraction: str = "handoff",
        key_fingerprint: bool = False,
        row_provenance: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> PreparedFactApply:
        """The read-and-compute half of :meth:`apply_facts`; writes nothing.

        It reads the prior rows of every key the facts touch and the
        lineage of edge endpoints, then computes the rows to write, the
        change classes and the delta counts. :meth:`apply_facts` passes the
        result to its write half. ``key_fingerprint`` also fingerprints the
        physical rows of every key read, before and after the reads, so a
        later write half can prove they did not change (issue 2941 fix 1);
        a write during the reads raises ``prepared_apply_inputs_moved``.
        """
        if node_retraction not in NODE_RETRACTION_MODES:
            raise ValueError(
                f"unsupported node_retraction: {node_retraction!r}; "
                f"expected one of {sorted(NODE_RETRACTION_MODES)}"
            )
        facts = list(facts)
        if row_provenance is not None:
            _validate_row_provenance(
                facts,
                row_provenance,
                source=source,
                derived_reapplication=derived_reapplication,
            )
        timings_ms: dict[str, float] = {}
        total_started = time.perf_counter()

        def add_timing(label: str, started: float) -> None:
            elapsed = round((time.perf_counter() - started) * 1000, 3)
            timings_ms[label] = round(timings_ms.get(label, 0.0) + elapsed, 3)

        def add_chronos_operation_timings(prefix: str) -> None:
            backend = getattr(getattr(session, "_context", None), "_backend", None)
            pop_timings = getattr(backend, "pop_last_operation_timings_ms", None)
            if not callable(pop_timings):
                return
            for label, elapsed in pop_timings().items():
                if isinstance(elapsed, (int, float)):
                    timings_ms[f"{prefix}.{label}"] = round(float(elapsed), 3)

        def add_chronos_transaction_timings(prefix: str) -> None:
            backend = getattr(getattr(session, "_context", None), "_backend", None)
            pop_timings = getattr(backend, "pop_last_transaction_timings_ms", None)
            if not callable(pop_timings):
                return
            for label, elapsed in pop_timings().items():
                if isinstance(elapsed, (int, float)):
                    timings_ms[f"{prefix}.finalize.{label}"] = round(
                        float(elapsed),
                        3,
                    )

        fingerprint_before = None
        if key_fingerprint:
            fingerprint_before = self._prepared_key_fingerprint(facts)
        step_started = time.perf_counter()
        node_upserts: list[dict[str, Any]] = []
        node_deletes: list[dict[str, Any]] = []
        edge_upserts: list[dict[str, Any]] = []
        edge_deletes: list[dict[str, Any]] = []
        touched_nodes_by_subtype: dict[str, list[str]] = {}
        touched_edges_by_type: dict[str, list[int]] = {}
        touched_edge_endpoints_by_type: dict[str, list[dict[str, Any]]] = {}
        edge_type_by_id: dict[int, str] = {}
        searchable_fields_cache: dict[str, list[str]] = {}

        for fact in facts:
            if isinstance(fact, NodeFact):
                touched_nodes_by_subtype.setdefault(
                    str(fact.subtype),
                    [],
                ).append(str(fact.node_id))
                op = fact.op.upper()
                if op == "R":
                    node_deletes.append({"node_id": fact.node_id})
                elif op == "I":
                    attrs = self._stamp_searchable_text(
                        fact.subtype,
                        dict(fact.attrs),
                        cache=searchable_fields_cache,
                    )
                    row_source, row_run_id, row_observed_at = (
                        source, run_id, observed_at,
                    )
                    if row_provenance is not None:
                        # The row keeps the provenance it already has; only
                        # its searchable text is stamped again (#3318).
                        kept = row_provenance[str(fact.node_id)]
                        attrs.pop(LINEAGE_ATTR, None)
                        if kept.get("lineage") is not None:
                            attrs[LINEAGE_ATTR] = kept["lineage"]
                        row_source = kept["source"]
                        row_run_id = kept["run_id"]
                        row_observed_at = kept["observed_at"]
                    else:
                        attrs = stamp_attrs_with_source_lineage(
                            attrs,
                            source_id=source,
                            source_record_id=fact.source_record_id,
                            source_revision=fact.source_revision,
                            policy_fingerprint=policy_fingerprint,
                            preserve_existing_lineage=derived_reapplication,
                        )
                    text = attrs.get("text")
                    node_upserts.append({
                        "node_id": fact.node_id,
                        "subtype": fact.subtype,
                        "attrs": attrs,
                        "text_hash": None if text is None else md5_text(str(text)),
                        "embedding": None,
                        "embedding_model": None,
                        "source": row_source,
                        "source_record_id": dict(fact.source_record_id),
                        "source_revision": dict(fact.source_revision),
                        "run_id": row_run_id,
                        "observed_at": row_observed_at,
                        "_ownership_mode": fact.ownership_mode,
                    })
                else:
                    raise ValueError(f"unsupported node op: {fact.op!r}")
            elif isinstance(fact, EdgeFact):
                computed_edge_id = edge_id(fact.src, fact.edge_type, fact.dst)
                edge_type_by_id[int(computed_edge_id)] = str(fact.edge_type)
                touched_edges_by_type.setdefault(
                    str(fact.edge_type),
                    [],
                ).append(int(computed_edge_id))
                touched_edge_endpoints_by_type.setdefault(
                    str(fact.edge_type),
                    [],
                ).append({
                    "edge_id": int(computed_edge_id),
                    "src": str(fact.src),
                    "dst": str(fact.dst),
                })
                op = fact.op.upper()
                if op == "R":
                    edge_deletes.append({"edge_id": computed_edge_id})
                elif op == "I":
                    edge_upserts.append({
                        "edge_id": computed_edge_id,
                        "src": fact.src,
                        "dst": fact.dst,
                        "edge_type": fact.edge_type,
                        "attrs": stamp_attrs_with_source_lineage(
                            fact.attrs,
                            source_id=source,
                            source_record_id=fact.source_record_id,
                            source_revision=fact.source_revision,
                            policy_fingerprint=policy_fingerprint,
                            preserve_existing_lineage=derived_reapplication,
                        ),
                        "provenance": fact.provenance,
                        "confidence": float(fact.confidence),
                        "source": source,
                        "source_record_id": dict(fact.source_record_id),
                        "source_revision": dict(fact.source_revision),
                        "run_id": run_id,
                        "observed_at": observed_at,
                    })
                else:
                    raise ValueError(f"unsupported edge op: {fact.op!r}")
            else:
                raise TypeError(f"unsupported graph edit type: {type(fact).__name__}")
        add_timing("prepare_rows", step_started)

        step_started = time.perf_counter()
        node_upserts = dedupe_rows_last(node_upserts, "node_id")
        node_deletes = dedupe_rows_last(node_deletes, "node_id")
        edge_upserts = dedupe_rows_last(edge_upserts, "edge_id")
        edge_deletes = dedupe_rows_last(edge_deletes, "edge_id")
        add_timing("dedupe_rows", step_started)
        # Nodes this write both retracts and upserts are still declared by the
        # source; the shared-node declarer step leaves them to the same-flush
        # coalescing below (issue #1901).
        batch_node_upsert_ids = {str(row["node_id"]) for row in node_upserts}

        # Every new automatically managed derived node must carry the exact
        # support declaration before any Chronos graph write occurs. The
        # emitter is the friendly first line of defense; this boundary is the
        # authoritative guard for alternative/admitted fact producers.
        from okg.substrate.derived_node_liveness import validate_derived_node_row

        for row in node_upserts:
            try:
                validate_derived_node_row(row)
            except ValueError as exc:
                raise SubstrateError(
                    "derived node assertion has no admitted support declaration",
                    data={
                        "node_id": str(row.get("node_id") or ""),
                        "source": str(row.get("source") or ""),
                        "reason": str(exc),
                    },
                    fix_hint=(
                        "emit the exact okg.derived_support.v1 object and matching "
                        "canonical derived_from list"
                    ),
                ) from exc

        if session is None:
            step_started = time.perf_counter()
            session = self.checkout(branch)
            add_timing("checkout", step_started)
        elif str(getattr(session, "branch_id", branch)) != str(branch):
            raise SubstrateError(
                "Chronos graph session branch does not match apply target",
                data={
                    "session_branch": str(getattr(session, "branch_id", "")),
                    "target_branch": str(branch),
                },
                fix_hint=(
                    "checkout a session for the same branch passed to "
                    "apply_facts"
                ),
            )
        node_ids_for_prior = list(dict.fromkeys(
            [str(row["node_id"]) for row in node_upserts]
            + [str(row["node_id"]) for row in node_deletes]
        ))
        step_started = time.perf_counter()
        prior_nodes_by_id = self._prior_nodes_by_id(session, node_ids_for_prior)
        add_timing("load_prior_nodes", step_started)
        edge_ids_for_prior = list(dict.fromkeys(
            [int(row["edge_id"]) for row in edge_upserts]
            + [int(row["edge_id"]) for row in edge_deletes]
        ))
        step_started = time.perf_counter()
        prior_edges_by_id = self._prior_edges_by_id(session, edge_ids_for_prior)
        add_timing("load_prior_edges", step_started)
        if derived_reapplication or source == "_publisher_derived":
            step_started = time.perf_counter()
            # Nodes first: the edge stamp below inherits from endpoint node
            # lineage, so a node's declared provenance must be resolved
            # before an edge reads it, or the edge inherits the empty
            # envelope this method exists to fill.
            for label, value in self._stamp_publisher_derived_node_lineage(
                session,
                node_upserts=node_upserts,
                policy_fingerprint=policy_fingerprint,
            ).items():
                timings_ms[f"publisher_lineage_stamp.{label}"] = value
            self._stamp_publisher_derived_edge_lineage(
                session,
                edge_upserts=edge_upserts,
                node_upserts=node_upserts,
                policy_fingerprint=policy_fingerprint,
            )
            add_timing("publisher_lineage_stamp", step_started)
        # Ranked edge types (item 92a): every producer's version is kept in
        # okg.graph_edge_declarers and the graph stores the highest-ranked
        # one. Their rows leave the last-writer-wins path below and are
        # decided inside the write transaction (_apply_edge_declarers).
        edge_ranks: dict[str, dict[str, int]] = {}
        if self._tracks_node_declarers() and (
            edge_upserts or edge_deletes or node_deletes
        ):
            step_started = time.perf_counter()
            edge_ranks = self._edge_precedence_ranks()
            add_timing("edge_declarers.load_ranking", step_started)
        # Only a write that can touch a declaration holds the ranking lock: an
        # edge write of a ranked type, or a delete (an edge delete, or a node
        # delete with its incident edges). Edge writes of unranked types keep
        # today's path and do not wait for a ranking change.
        ranking_relevant = bool(edge_ranks) and bool(
            node_deletes
            or edge_deletes
            or any(str(row["edge_type"]) in edge_ranks for row in edge_upserts)
        )
        # Only the producers a type's ranking names take the declarer path;
        # another producer of a ranked type keeps today's path, and writing an
        # edge a ranked producer declares is refused inside the transaction.
        # An unattributed write (source=None) overrides every declarer.
        ranked_types_for_source = {
            edge_type
            for edge_type, ranking in edge_ranks.items()
            if source is None or source in ranking
        }
        ranked_edge_upserts = [
            row for row in edge_upserts
            if str(row["edge_type"]) in ranked_types_for_source
        ]
        ranked_edge_delete_ids = [
            int(row["edge_id"])
            for row in edge_deletes
            if edge_type_by_id.get(int(row["edge_id"])) in ranked_types_for_source
        ]
        unranked_writes_of_ranked_types = [
            int(row["edge_id"])
            for row in edge_upserts
            if str(row["edge_type"]) in edge_ranks
            and str(row["edge_type"]) not in ranked_types_for_source
        ]
        if ranked_edge_upserts or ranked_edge_delete_ids:
            edge_upserts = [
                row for row in edge_upserts
                if str(row["edge_type"]) not in ranked_types_for_source
            ]
            ranked_delete_set = set(ranked_edge_delete_ids)
            edge_deletes = [
                row for row in edge_deletes
                if int(row["edge_id"]) not in ranked_delete_set
            ]
        step_started = time.perf_counter()
        delete_node_ids = {str(row["node_id"]) for row in node_deletes}
        delete_edge_ids = {int(row["edge_id"]) for row in edge_deletes}
        original_node_upsert_count = len(node_upserts)
        original_edge_upsert_count = len(edge_upserts)
        substitute_node_upserts = [
            row
            for row in node_upserts
            if self._is_substitute_node(row)
        ]
        substitute_preserved_count = sum(
            1
            for row in node_upserts
            if self._substitute_would_clobber(
                row,
                prior_nodes_by_id.get(str(row["node_id"])),
            )
        )
        node_upserts = [
            row for row in node_upserts
            if not self._substitute_would_clobber(
                row,
                prior_nodes_by_id.get(str(row["node_id"])),
            )
            and (
                str(row["node_id"]) in delete_node_ids
                or not self._node_upsert_semantic_noop(
                    row,
                    prior_nodes_by_id.get(str(row["node_id"])),
                )
            )
        ]
        edge_upserts = [
            row for row in edge_upserts
            if int(row["edge_id"]) in delete_edge_ids
            or not self._edge_upsert_semantic_noop(
                row,
                prior_edges_by_id.get(int(row["edge_id"])),
            )
        ]
        add_timing("noop_filter", step_started)
        timings_ms["noop_filter.nodes_dropped"] = float(
            original_node_upsert_count - len(node_upserts)
        )
        timings_ms["noop_filter.substitute_owners_preserved"] = float(
            substitute_preserved_count
        )
        timings_ms["noop_filter.edges_dropped"] = float(
            original_edge_upsert_count - len(edge_upserts)
        )
        scoped_node_deletes = self._scoped_delete_keys_from_prior(
            node_deletes,
            key_field="node_id",
            prior_by_id=prior_nodes_by_id,
            source=source,
        )
        scoped_edge_deletes = self._scoped_delete_keys_from_prior(
            edge_deletes,
            key_field="edge_id",
            prior_by_id=prior_edges_by_id,
            source=source,
        )
        node_upsert_ids_for_apply = {str(row["node_id"]) for row in node_upserts}
        edge_upsert_ids_for_apply = {int(row["edge_id"]) for row in edge_upserts}
        coalesced_node_delete_ids = {
            str(row["node_id"])
            for row in scoped_node_deletes
            if str(row["node_id"]) in node_upsert_ids_for_apply
        }
        coalesced_edge_delete_ids = {
            int(row["edge_id"])
            for row in scoped_edge_deletes
            if int(row["edge_id"]) in edge_upsert_ids_for_apply
        }
        scoped_node_deletes_for_apply = [
            row for row in scoped_node_deletes
            if str(row["node_id"]) not in coalesced_node_delete_ids
        ]
        scoped_edge_deletes_for_apply = [
            row for row in scoped_edge_deletes
            if int(row["edge_id"]) not in coalesced_edge_delete_ids
        ]
        node_deletes_for_incident_edges = [
            row for row in node_deletes
            if str(row["node_id"]) not in coalesced_node_delete_ids
        ]
        step_started = time.perf_counter()
        incident_edge_rows = self._incident_edges_for_node_deletes(
            session,
            node_deletes_for_incident_edges,
            source=source,
            exclude_edge_ids={
                int(row["edge_id"]) for row in scoped_edge_deletes_for_apply
            },
            exclude_edge_types=ranked_types_for_source,
        ) if delete_incident_edges else []
        add_timing("load_incident_edges", step_started)
        for row in incident_edge_rows:
            prior_edges_by_id.setdefault(int(row["edge_id"]), row)
            edge_type = str(row["edge_type"])
            touched_edges_by_type.setdefault(edge_type, []).append(int(row["edge_id"]))
            touched_edge_endpoints_by_type.setdefault(edge_type, []).append({
                "edge_id": int(row["edge_id"]),
                "src": str(row["src"]),
                "dst": str(row["dst"]),
            })
        incident_edge_deletes = [
            {"edge_id": int(row["edge_id"])}
            for row in incident_edge_rows
        ]
        if coalesced_node_delete_ids:
            timings_ms["transaction.delete_nodes.skipped_same_flush_replacements"] = (
                float(len(coalesced_node_delete_ids))
            )
        if coalesced_edge_delete_ids:
            timings_ms["transaction.delete_edges.skipped_same_flush_replacements"] = (
                float(len(coalesced_edge_delete_ids))
            )
        step_started = time.perf_counter()
        node_attr_changes_by_subtype = self._node_attr_changes_by_subtype(
            session,
            node_upserts=node_upserts,
            node_deletes=scoped_node_deletes_for_apply,
            prior_by_id=prior_nodes_by_id,
        )
        self._add_node_change_class_timings(
            timings_ms,
            node_attr_changes_by_subtype,
        )
        add_timing("node_attr_changes", step_started)
        step_started = time.perf_counter()
        edge_attr_changes_by_type = self._edge_attr_changes_by_type(
            edge_upserts=edge_upserts,
            edge_deletes=[
                *scoped_edge_deletes_for_apply,
                *incident_edge_deletes,
            ],
            prior_by_id=prior_edges_by_id,
        )
        add_timing("edge_attr_changes", step_started)
        step_started = time.perf_counter()
        delta_counts = self._delta_counts_from_prior_rows(
            node_upserts=node_upserts,
            node_deletes=scoped_node_deletes_for_apply,
            edge_upserts=edge_upserts,
            edge_deletes=[
                *scoped_edge_deletes_for_apply,
                *incident_edge_deletes,
            ],
            prior_nodes_by_id=prior_nodes_by_id,
            prior_edges_by_id=prior_edges_by_id,
            source=source,
        )
        add_timing("delta_counts", step_started)
        step_started = time.perf_counter()
        missing_endpoints = (
            self._missing_edge_endpoints(
                session,
                edge_upserts=[*edge_upserts, *ranked_edge_upserts],
                node_upserts=node_upserts,
                node_deletes=node_deletes,
            )
            if strict_endpoints
            else set()
        )
        add_timing("missing_edge_endpoints", step_started)
        if missing_endpoints:
            raise SubstrateError(
                "Chronos graph edge write references missing endpoint nodes",
                data={
                    "missing_node_ids": sorted(missing_endpoints),
                    "edge_count": len(edge_upserts),
                },
                fix_hint=(
                    "emit endpoint NodeFact rows before or in the same "
                    "ingest chunk as the EdgeFact"
                ),
            )
        key_fingerprint_value = None
        read_classes: KnownReadClasses = {}
        ranked_decision: _RankedEdgeWrite | None = None
        if key_fingerprint:
            # Issue #2941: decide the ranked edges here, from the stored rows
            # and declarations the key fingerprint covers, so the write half
            # under the lock only writes the decision. The reads sit between
            # the two fingerprint readings, so a write during them refuses
            # the preparation like any other. Only when the fingerprint
            # covers the declarations: reuse is then tied to their rows.
            if (
                edge_ranks
                and (ranked_edge_upserts or ranked_edge_delete_ids)
                and not node_deletes
                and any(
                    part[0] == "okg.graph_edge_declarers"
                    for part in fingerprint_before or ()
                )
                and self._edge_declarers_table_registered()
            ):
                step_started = time.perf_counter()
                ranked_decision = self._decide_edge_declarers(
                    session,
                    source=source,
                    ranks=edge_ranks,
                    ranked_types_for_source=ranked_types_for_source,
                    upserts=ranked_edge_upserts,
                    withdrawn_edge_ids=ranked_edge_delete_ids,
                    retracted_node_ids=[],
                    removed_node_ids=[],
                    timings_ms=timings_ms,
                    label="prepare.edge_declarers",
                )
                add_timing("prepare.edge_declarers", step_started)
            key_fingerprint_value = self._prepared_key_fingerprint(facts)
            if key_fingerprint_value != fingerprint_before:
                raise SubstrateError(
                    "graph rows changed while the apply was being prepared",
                    data={"reason": "prepared_apply_inputs_moved"},
                )
            # Prepared ahead of the lock (issue #2941): compute the rows'
            # read classes now, so the write half under the lock only
            # checks that each row's class inputs did not change.
            step_started = time.perf_counter()
            known_read_classes(node_upserts, "node", into=read_classes)
            known_read_classes(edge_upserts, "edge", into=read_classes)
            # A ranked type's upserts wait for the declarer step, which
            # stores a winning upsert's own values, so their classes are
            # known here too. Every derived `calls`/`references` edge is
            # ranked.
            known_read_classes(ranked_edge_upserts, "edge", into=read_classes)
            add_timing("prepare.read_classes", step_started)
        add_timing("prepare_total", total_started)
        return PreparedFactApply(
            facts=tuple(facts),
            options=_apply_options(
                branch=branch, source=source,
                policy_fingerprint=policy_fingerprint, run_id=run_id,
                observed_at=observed_at, strict_endpoints=strict_endpoints,
                delete_incident_edges=delete_incident_edges,
                derived_reapplication=derived_reapplication,
                node_retraction=node_retraction,
                row_provenance=row_provenance,
            ),
            key_fingerprint=key_fingerprint_value,
            session=session,
            timings_ms=timings_ms,
            total_started=total_started,
            node_upserts=node_upserts,
            node_deletes=node_deletes,
            edge_upserts=edge_upserts,
            edge_deletes=edge_deletes,
            touched_nodes_by_subtype=touched_nodes_by_subtype,
            touched_edges_by_type=touched_edges_by_type,
            touched_edge_endpoints_by_type=touched_edge_endpoints_by_type,
            searchable_fields_cache=searchable_fields_cache,
            batch_node_upsert_ids=batch_node_upsert_ids,
            prior_nodes_by_id=prior_nodes_by_id,
            prior_edges_by_id=prior_edges_by_id,
            delete_node_ids=delete_node_ids,
            delete_edge_ids=delete_edge_ids,
            original_node_upsert_count=original_node_upsert_count,
            substitute_node_upserts=substitute_node_upserts,
            scoped_node_deletes_for_apply=scoped_node_deletes_for_apply,
            scoped_edge_deletes_for_apply=scoped_edge_deletes_for_apply,
            node_deletes_for_incident_edges=node_deletes_for_incident_edges,
            incident_edge_deletes=incident_edge_deletes,
            coalesced_node_delete_ids=coalesced_node_delete_ids,
            coalesced_edge_delete_ids=coalesced_edge_delete_ids,
            node_attr_changes_by_subtype=node_attr_changes_by_subtype,
            edge_attr_changes_by_type=edge_attr_changes_by_type,
            delta_counts=delta_counts,
            edge_ranks=edge_ranks,
            ranking_relevant=ranking_relevant,
            ranked_types_for_source=ranked_types_for_source,
            ranked_edge_upserts=ranked_edge_upserts,
            ranked_edge_delete_ids=ranked_edge_delete_ids,
            unranked_writes_of_ranked_types=unranked_writes_of_ranked_types,
            read_classes=read_classes,
            ranked_decision=ranked_decision,
        )

    def _write_prepared_apply(
        self,
        state: PreparedFactApply,
        *,
        in_transaction: bool = False,
    ) -> GraphEditResult:
        """The write half of :meth:`apply_facts`.

        ``in_transaction``: the caller already opened the session's write
        transaction and took the branch write lock (the verified path).
        """
        session = state.session
        timings_ms = state.timings_ms
        total_started = state.total_started
        branch = state.options.branch
        source = state.options.source
        delete_incident_edges = state.options.delete_incident_edges
        node_retraction = state.options.node_retraction
        node_upserts = state.node_upserts
        node_deletes = state.node_deletes
        edge_upserts = state.edge_upserts
        touched_edges_by_type = state.touched_edges_by_type
        touched_edge_endpoints_by_type = state.touched_edge_endpoints_by_type
        searchable_fields_cache = state.searchable_fields_cache
        batch_node_upsert_ids = state.batch_node_upsert_ids
        prior_nodes_by_id = state.prior_nodes_by_id
        prior_edges_by_id = state.prior_edges_by_id
        delete_node_ids = state.delete_node_ids
        original_node_upsert_count = state.original_node_upsert_count
        substitute_node_upserts = state.substitute_node_upserts
        scoped_node_deletes_for_apply = state.scoped_node_deletes_for_apply
        scoped_edge_deletes_for_apply = state.scoped_edge_deletes_for_apply
        node_deletes_for_incident_edges = state.node_deletes_for_incident_edges
        incident_edge_deletes = state.incident_edge_deletes
        coalesced_node_delete_ids = state.coalesced_node_delete_ids
        coalesced_edge_delete_ids = state.coalesced_edge_delete_ids
        node_attr_changes_by_subtype = state.node_attr_changes_by_subtype
        edge_attr_changes_by_type = state.edge_attr_changes_by_type
        delta_counts = state.delta_counts
        edge_ranks = state.edge_ranks
        ranking_relevant = state.ranking_relevant
        ranked_types_for_source = state.ranked_types_for_source
        ranked_edge_upserts = state.ranked_edge_upserts
        ranked_edge_delete_ids = state.ranked_edge_delete_ids
        unranked_writes_of_ranked_types = state.unranked_writes_of_ranked_types

        def add_timing(label: str, started: float) -> None:
            elapsed = round((time.perf_counter() - started) * 1000, 3)
            timings_ms[label] = round(timings_ms.get(label, 0.0) + elapsed, 3)

        def add_chronos_operation_timings(prefix: str) -> None:
            backend = getattr(getattr(session, "_context", None), "_backend", None)
            pop_timings = getattr(backend, "pop_last_operation_timings_ms", None)
            if not callable(pop_timings):
                return
            for label, elapsed in pop_timings().items():
                if isinstance(elapsed, (int, float)):
                    timings_ms[f"{prefix}.{label}"] = round(float(elapsed), 3)

        def add_chronos_transaction_timings(prefix: str) -> None:
            backend = getattr(getattr(session, "_context", None), "_backend", None)
            pop_timings = getattr(backend, "pop_last_transaction_timings_ms", None)
            if not callable(pop_timings):
                return
            for label, elapsed in pop_timings().items():
                if isinstance(elapsed, (int, float)):
                    timings_ms[f"{prefix}.finalize.{label}"] = round(
                        float(elapsed),
                        3,
                    )

        transaction_started = time.perf_counter()
        # A same-key retract+reinsert in one flush is physically coalesced into
        # the upsert (the reinserted row version overwrites the retracted one, so
        # no standalone delete is issued), but the retraction is still a real
        # logical operation. Seed the delete counts with the coalesced retractions
        # so nodes_deleted / edges_deleted reflect every requested retraction, not
        # only the ones applied as standalone deletes.
        deleted_edges = len(coalesced_edge_delete_ids)
        incident_edges_deleted = 0
        deleted_nodes = len(coalesced_node_delete_ids)
        nodes_upserted = len(node_upserts)
        edges_upserted = len(edge_upserts)
        # Shared-node declarers (issue #1901). A retraction may pass the node
        # to another declarer, and a retraction of a row this source does not
        # own still withdraws its declaration, so any node retraction or
        # substitute declaration needs the write transaction.
        track_declarers = self._tracks_node_declarers() and bool(
            node_deletes or substitute_node_upserts
        )
        if track_declarers:
            step_started = time.perf_counter()
            self._require_node_declarers_table()
            add_timing("node_declarers.ensure_table", step_started)
        # Ranked edges: this write's own declarations and withdrawals, and the
        # ranked edges incident to nodes it deletes or retracts.
        track_edge_declarers = bool(edge_ranks) and bool(
            ranked_edge_upserts
            or ranked_edge_delete_ids
            or unranked_writes_of_ranked_types
            or (node_deletes and delete_incident_edges)
        )
        if track_edge_declarers:
            step_started = time.perf_counter()
            self._require_edge_declarers_table()
            add_timing("edge_declarers.ensure_table", step_started)
        if (
            node_upserts
            or substitute_node_upserts
            or scoped_node_deletes_for_apply
            or track_declarers
            or edge_upserts
            or scoped_edge_deletes_for_apply
            or track_edge_declarers
        ):
            with (
                session.transaction() if not in_transaction else nullcontext()
            ):
                if not in_transaction:
                    step_started = time.perf_counter()
                    if ranking_relevant:
                        # Shared with every other ranked write; a ranking change
                        # (okg migrate, worker start) takes it exclusively, so no
                        # write planned with the old ranking commits after it.
                        self._lock_edge_ranking_shared()
                    self._lock_branch_write(branch)
                    add_timing("transaction.lock_branch", step_started)
                # On the verified path the caller took the ranking lock (when
                # relevant) before the branch lock, in the same order.
                if ranking_relevant:
                    self._refuse_if_edge_ranking_changed(edge_ranks, branch=branch)
                # Re-read ownership only after acquiring the same branch fence
                # used by graph writers. A derived node id is one authoritative
                # row, not an implicit merge point: the same producer may update
                # it, but a different producer/source cannot replace it.
                if node_upserts:
                    current_priors = self._prior_nodes_by_id(
                        session,
                        [str(row["node_id"]) for row in node_upserts],
                    )
                    if state.options.row_provenance is not None:
                        self._refuse_moved_provenance_rows(
                            node_upserts,
                            current_priors,
                            state.options.row_provenance,
                        )
                    collisions = []
                    for row in node_upserts:
                        node_id = str(row["node_id"])
                        prior = current_priors.get(node_id)
                        if prior is None:
                            continue
                        if not (
                            _has_derived_node_lifecycle(row)
                            or _has_derived_node_lifecycle(prior)
                        ):
                            continue
                        incoming_source = str(row.get("source") or "")
                        prior_source = str(prior.get("source") or "")
                        if incoming_source != prior_source:
                            collisions.append({
                                "node_id": node_id,
                                "existing_source": prior_source,
                                "incoming_source": incoming_source,
                            })
                    if collisions:
                        raise SubstrateError(
                            "derived node id is already owned by a different producer",
                            data={"collisions": collisions},
                            fix_hint=(
                                "mint a producer-owned node id or use an explicit "
                                "merge contract; cross-producer replacement is refused"
                            ),
                        )
                if substitute_node_upserts:
                    step_started = time.perf_counter()
                    substitute_ids = [
                        str(row["node_id"])
                        for row in substitute_node_upserts
                    ]
                    current_substitute_priors = self._prior_nodes_by_id(
                        session,
                        substitute_ids,
                    )
                    for node_id in substitute_ids:
                        prior_nodes_by_id.pop(node_id, None)
                    prior_nodes_by_id.update(current_substitute_priors)
                    node_upserts = [
                        row
                        for row in node_upserts
                        if not self._is_substitute_node(row)
                    ]
                    node_upserts.extend(
                        row
                        for row in substitute_node_upserts
                        if not self._substitute_would_clobber(
                            row,
                            current_substitute_priors.get(str(row["node_id"])),
                        )
                        and (
                            str(row["node_id"]) in delete_node_ids
                            or not self._node_upsert_semantic_noop(
                                row,
                                current_substitute_priors.get(str(row["node_id"])),
                            )
                        )
                    )
                    substitute_preserved_count = sum(
                        1
                        for row in substitute_node_upserts
                        if self._substitute_would_clobber(
                            row,
                            current_substitute_priors.get(str(row["node_id"])),
                        )
                    )
                    timings_ms[
                        "noop_filter.substitute_owners_preserved"
                    ] = float(substitute_preserved_count)
                    timings_ms["noop_filter.nodes_dropped"] = float(
                        original_node_upsert_count - len(node_upserts)
                    )
                    nodes_upserted = len(node_upserts)
                    node_attr_changes_by_subtype = self._node_attr_changes_by_subtype(
                        session,
                        node_upserts=node_upserts,
                        node_deletes=scoped_node_deletes_for_apply,
                        prior_by_id=prior_nodes_by_id,
                    )
                    self._add_node_change_class_timings(
                        timings_ms,
                        node_attr_changes_by_subtype,
                    )
                    delta_counts = self._delta_counts_from_prior_rows(
                        node_upserts=node_upserts,
                        node_deletes=scoped_node_deletes_for_apply,
                        edge_upserts=edge_upserts,
                        edge_deletes=[
                            *scoped_edge_deletes_for_apply,
                            *incident_edge_deletes,
                        ],
                        prior_nodes_by_id=prior_nodes_by_id,
                        prior_edges_by_id=prior_edges_by_id,
                        source=source,
                    )
                    add_timing("transaction.revalidate_substitutes", step_started)
                if track_declarers:
                    step_started = time.perf_counter()
                    scoped_node_deletes_for_apply = self._apply_node_declarers(
                        session,
                        source=source,
                        node_deletes=node_deletes,
                        batch_node_upsert_ids=batch_node_upsert_ids,
                        substitute_node_upserts=substitute_node_upserts,
                        node_upserts=node_upserts,
                        prior_nodes_by_id=prior_nodes_by_id,
                        timings_ms=timings_ms,
                        purge=node_retraction == "purge",
                        searchable_fields_cache=searchable_fields_cache,
                    )
                    nodes_upserted = len(node_upserts)
                    node_attr_changes_by_subtype = self._node_attr_changes_by_subtype(
                        session,
                        node_upserts=node_upserts,
                        node_deletes=scoped_node_deletes_for_apply,
                        prior_by_id=prior_nodes_by_id,
                    )
                    self._add_node_change_class_timings(
                        timings_ms,
                        node_attr_changes_by_subtype,
                    )
                    delta_counts = self._delta_counts_from_prior_rows(
                        node_upserts=node_upserts,
                        node_deletes=scoped_node_deletes_for_apply,
                        edge_upserts=edge_upserts,
                        edge_deletes=[
                            *scoped_edge_deletes_for_apply,
                            *incident_edge_deletes,
                        ],
                        prior_nodes_by_id=prior_nodes_by_id,
                        prior_edges_by_id=prior_edges_by_id,
                        source=source,
                    )
                    add_timing("transaction.node_declarers", step_started)
                if track_edge_declarers:
                    step_started = time.perf_counter()
                    ranked = self._apply_edge_declarers(
                        session,
                        source=source,
                        ranks=edge_ranks,
                        ranked_types_for_source=ranked_types_for_source,
                        unranked_writes=unranked_writes_of_ranked_types,
                        upserts=ranked_edge_upserts,
                        withdrawn_edge_ids=ranked_edge_delete_ids,
                        retracted_node_ids=(
                            [
                                str(row["node_id"])
                                for row in node_deletes_for_incident_edges
                            ]
                            if delete_incident_edges else []
                        ),
                        removed_node_ids=[
                            str(row["node_id"])
                            for row in scoped_node_deletes_for_apply
                        ],
                        timings_ms=timings_ms,
                        prepared=state.ranked_decision,
                    )
                    prior_edges_by_id.update(ranked.current_edges)
                    for row in ranked.incident_rows:
                        edge_type = str(row["edge_type"])
                        touched_edges_by_type.setdefault(edge_type, []).append(
                            int(row["edge_id"])
                        )
                        touched_edge_endpoints_by_type.setdefault(
                            edge_type, [],
                        ).append({
                            "edge_id": int(row["edge_id"]),
                            "src": str(row["src"]),
                            "dst": str(row["dst"]),
                        })
                    for edge_type, changes in self._edge_attr_changes_by_type(
                        edge_upserts=ranked.outcome.edge_upserts,
                        edge_deletes=ranked.outcome.edge_deletes,
                        prior_by_id=ranked.current_edges,
                    ).items():
                        edge_attr_changes_by_type.setdefault(
                            edge_type, {},
                        ).update(changes)
                    for label, value in self._delta_counts_from_prior_rows(
                        node_upserts=[],
                        node_deletes=[],
                        edge_upserts=ranked.outcome.edge_upserts,
                        edge_deletes=ranked.outcome.edge_deletes,
                        prior_nodes_by_id={},
                        prior_edges_by_id=ranked.current_edges,
                        source=None,
                    ).items():
                        delta_counts[label] = delta_counts.get(label, 0) + value
                    edge_upserts = [*edge_upserts, *ranked.outcome.edge_upserts]
                    scoped_edge_deletes_for_apply = [
                        *scoped_edge_deletes_for_apply,
                        *ranked.outcome.edge_deletes,
                    ]
                    edges_upserted = len(edge_upserts)
                    add_timing("transaction.edge_declarers", step_started)
                for row in node_upserts:
                    row.pop("_ownership_mode", None)
                # PACT lane-h-nomos-read-class-stored: every row version this
                # write stores carries the Nomos read class of its own final
                # content (lineage stamped, declarer handoffs included). A
                # table registered without the columns ignores the two keys.
                # Each new class's catalog row is written first, in this
                # transaction (PACT lane-h-nomos-read-class-sql), so every
                # stamped class has its catalog row when the rows commit.
                step_started = time.perf_counter()
                stamp_rows(
                    node_upserts, "node", catalog=self.context.db,
                    known=state.read_classes,
                )
                stamp_rows(
                    edge_upserts, "edge", catalog=self.context.db,
                    known=state.read_classes,
                )
                add_timing("transaction.stamp_read_class", step_started)
                apply_table_edits = getattr(session, "apply_table_edits", None)
                can_apply_mixed = callable(apply_table_edits)
                edge_deletes_coalesced_with_upserts = (
                    bool(scoped_edge_deletes_for_apply)
                    and bool(edge_upserts)
                    and can_apply_mixed
                )
                if scoped_edge_deletes_for_apply and not edge_deletes_coalesced_with_upserts:
                    step_started = time.perf_counter()
                    if can_apply_mixed:
                        apply_table_edits(
                            EDGE_TABLE,
                            deletes=scoped_edge_deletes_for_apply,
                            upserts=[],
                        )
                    else:
                        session.delete_keys(EDGE_TABLE, scoped_edge_deletes_for_apply)
                    deleted_edges += len(scoped_edge_deletes_for_apply)
                    add_chronos_operation_timings("transaction.delete_edges")
                    add_timing("transaction.delete_edges", step_started)
                if node_deletes_for_incident_edges and delete_incident_edges:
                    step_started = time.perf_counter()
                    (
                        incident_edges_deleted,
                        incident_edges_by_type,
                    ) = self._delete_incident_edges(
                        session,
                        incident_edge_deletes,
                        source=source,
                    )
                    for edge_type, edge_ids in incident_edges_by_type.items():
                        touched_edges_by_type.setdefault(edge_type, []).extend(edge_ids)
                    add_timing("transaction.delete_incident_edges", step_started)
                elif node_deletes_for_incident_edges:
                    timings_ms[
                        "transaction.delete_incident_edges.skipped_verified_record_set"
                    ] = float(len(node_deletes_for_incident_edges))
                if scoped_node_deletes_for_apply:
                    step_started = time.perf_counter()
                    if can_apply_mixed:
                        apply_table_edits(
                            NODE_TABLE,
                            deletes=scoped_node_deletes_for_apply,
                            upserts=[],
                        )
                    else:
                        session.delete_keys(NODE_TABLE, scoped_node_deletes_for_apply)
                    deleted_nodes += len(scoped_node_deletes_for_apply)
                    add_timing("transaction.delete_nodes", step_started)
                if node_upserts:
                    step_started = time.perf_counter()
                    if can_apply_mixed:
                        apply_table_edits(
                            NODE_TABLE,
                            deletes=[],
                            upserts=node_upserts,
                        )
                        add_chronos_operation_timings("transaction.edit_nodes")
                        add_timing("transaction.edit_nodes", step_started)
                    else:
                        session.upsert_rows(NODE_TABLE, node_upserts)
                        add_chronos_operation_timings("transaction.upsert_nodes")
                        add_timing("transaction.upsert_nodes", step_started)
                if edge_upserts:
                    step_started = time.perf_counter()
                    if can_apply_mixed:
                        apply_table_edits(
                            EDGE_TABLE,
                            deletes=(
                                scoped_edge_deletes_for_apply
                                if edge_deletes_coalesced_with_upserts else []
                            ),
                            upserts=edge_upserts,
                        )
                        if edge_deletes_coalesced_with_upserts:
                            deleted_edges += len(scoped_edge_deletes_for_apply)
                        add_chronos_operation_timings("transaction.edit_edges")
                        add_timing("transaction.edit_edges", step_started)
                    else:
                        session.upsert_rows(EDGE_TABLE, edge_upserts)
                        add_chronos_operation_timings("transaction.upsert_edges")
                        add_timing("transaction.upsert_edges", step_started)
                if node_upserts:
                    # PACT openrouter-embeddings-hnsw: link each stored row's
                    # content to its document for every active vector set,
                    # from the rows in memory. Last in the transaction, so the
                    # set counter's row lock (taken here) is held only until
                    # the commit that follows.
                    step_started = time.perf_counter()
                    from okg.substrate.embeddings.store import (
                        write_node_documents,
                    )

                    write_node_documents(
                        self.context.db,
                        node_upserts,
                        catalog_version_id=getattr(self, "_catalog_version_id", None),
                        timings=timings_ms,
                    )
                    add_timing("transaction.node_documents", step_started)
        else:
            timings_ms["transaction.skipped_noop"] = 1.0
        add_chronos_transaction_timings("transaction")
        add_timing("transaction.total", transaction_started)
        self._grant_chronos_public_tables_to_mcp_if_present()
        dirty_nodes_by_subtype: dict[str, tuple[str, ...]] = {
            subtype: tuple(dict.fromkeys(node_changes.keys()))
            for subtype, node_changes in node_attr_changes_by_subtype.items()
            if node_changes
        }
        dirty_edge_ids_by_type = {
            edge_type: tuple(dict.fromkeys(edge_changes.keys()))
            for edge_type, edge_changes in edge_attr_changes_by_type.items()
            if edge_changes
        }
        dirty_edge_id_set = {
            int(edge_id)
            for edge_ids in dirty_edge_ids_by_type.values()
            for edge_id in edge_ids
        }

        step_started = time.perf_counter()
        result = GraphEditResult(
            nodes_upserted=nodes_upserted,
            nodes_deleted=deleted_nodes,
            edges_upserted=edges_upserted,
            edges_deleted=deleted_edges + incident_edges_deleted,
            nodes_by_subtype=dirty_nodes_by_subtype,
            node_attr_changes_by_subtype=node_attr_changes_by_subtype,
            edges_by_type=dirty_edge_ids_by_type,
            edge_attr_changes_by_type=edge_attr_changes_by_type,
            edge_endpoints_by_type={
                edge_type: tuple(
                    dict(endpoint)
                    for endpoint in endpoints
                    if int(endpoint.get("edge_id", -1)) in dirty_edge_id_set
                )
                for edge_type, endpoints in touched_edge_endpoints_by_type.items()
                if any(
                    int(endpoint.get("edge_id", -1)) in dirty_edge_id_set
                    for endpoint in endpoints
                )
            },
            delta_counts=delta_counts,
        )
        add_timing("build_result", step_started)
        add_timing("total", total_started)
        result.timings_ms.update(timings_ms)
        return result

    def _prepared_key_fingerprint(
        self,
        facts: Iterable[NodeFact | EdgeFact],
    ) -> tuple[Any, ...]:
        """Fingerprint the physical rows of every key an edge-only apply reads.

        Count and hash of ``(ctid, xmin)`` of every row version, any branch,
        of the edges the facts name and of their endpoint nodes: the rows
        ``prepare_apply_facts`` reads (prior edges, endpoint lineage, endpoint
        existence), and, when the table is registered, the recorded ranked
        declarations of those edges, which the prepared ranked decision reads
        (issue #2941). Any write to one of those keys adds or rewrites a row
        version and so changes it; a table rewrite changes every ``ctid`` and
        reads as a change. Only edge facts qualify: a node fact's preparation
        also reads keys the facts do not name (declared inputs, incident
        edges), so it raises ``prepared_apply_node_facts``.
        """
        edge_ids: set[int] = set()
        node_ids: set[str] = set()
        for fact in facts:
            if not isinstance(fact, EdgeFact):
                raise SubstrateError(
                    "only an edge-only apply can be prepared ahead",
                    data={"reason": "prepared_apply_node_facts"},
                )
            edge_ids.add(int(edge_id(fact.src, fact.edge_type, fact.dst)))
            node_ids.add(str(fact.src))
            node_ids.add(str(fact.dst))
        db = self.context.db
        if (
            getattr(db, "dialect", None) != "postgres"
            or getattr(self.context, "backend_name", None) != "interval"
        ):
            raise SubstrateError(
                "a prepared apply needs the PostgreSQL interval backend",
                data={"reason": "prepared_apply_backend_unsupported"},
            )
        rows = db.execute(
            "SELECT table_name, physical_table FROM _chronos_branch_tables "
            "WHERE table_name IN ('okg.graph_nodes', 'okg.graph_edges', "
            "'okg.graph_edge_declarers') AND backend = 'interval'",
            (),
        ).fetchall()
        physical = {
            str(row["table_name"]): str(row["physical_table"]) for row in rows
        }
        if not {"okg.graph_nodes", "okg.graph_edges"} <= set(physical):
            raise SubstrateError(
                "a prepared apply needs the interval graph tables",
                data={"reason": "prepared_apply_tables_missing"},
            )
        keyed: list[tuple[str, str, list[Any], str]] = [
            ("okg.graph_edges", "edge_id", sorted(edge_ids), "bigint[]"),
            ("okg.graph_nodes", "node_id", sorted(node_ids), "text[]"),
        ]
        if "okg.graph_edge_declarers" in physical:
            keyed.append(
                (
                    "okg.graph_edge_declarers",
                    "edge_id",
                    sorted(edge_ids),
                    "bigint[]",
                )
            )
        out: list[Any] = []
        for table, key, values, cast in keyed:
            name = physical[table]
            if not name.replace("_", "").isalnum():
                raise SubstrateError(
                    "unexpected Chronos physical table name",
                    data={"table": table, "physical_table": name},
                )
            row = db.execute(
                "SELECT count(*) AS n, COALESCE(sum(hashtextextended("
                "ctid::text || ':' || xmin::text, 0)), 0) AS h "
                f'FROM "{name}" WHERE {key} = ANY(?::{cast})',
                (values,),
            ).fetchone()
            out.append((table, len(values), int(row["n"]), int(row["h"])))
        return tuple(out)

    def _apply_verified_prepared(
        self,
        facts: list[NodeFact | EdgeFact],
        prepared: PreparedFactApply,
        *,
        options: _ApplyOptions,
        session: Any | None,
    ) -> GraphEditResult:
        """Write a prepared apply after proving its reads still hold.

        One write transaction: take the branch write lock (the fence every
        graph writer takes), then compare the facts, the options and the key
        fingerprint with the prepared ones. All equal: write the prepared
        rows. Anything different: prepare again inside the same transaction,
        under the lock, and write that. Writers wait at most this one
        transaction. The result's timings say which happened
        (``prepared.reused`` or ``prepared.recomputed`` with its reason) and
        how long the lock was held (``prepared.lock_hold``).
        """
        # A prepared half is consumed once; offered again (a retried chunk),
        # it is recomputed like any other mismatch.
        already_used = prepared.used
        prepared.used = True
        if session is None:
            session = self.checkout(options.branch)
        elif str(getattr(session, "branch_id", options.branch)) != str(
            options.branch
        ):
            raise SubstrateError(
                "Chronos graph session branch does not match apply target",
                data={
                    "session_branch": str(getattr(session, "branch_id", "")),
                    "target_branch": str(options.branch),
                },
            )
        verify_started = time.perf_counter()
        with session.transaction():
            lock_started = time.perf_counter()
            # Ranked edges (item 92a): the ranking lock comes before the branch
            # lock, as in apply_facts. Taken whenever declarers are tracked,
            # because a recomputation below may find ranked work the
            # preparation did not.
            tracks_declarers = self._tracks_node_declarers()
            if tracks_declarers:
                self._lock_edge_ranking_shared()
            self._lock_branch_write(options.branch)
            lock_wait_ms = round((time.perf_counter() - lock_started) * 1000, 3)
            reason: str | None = None
            if already_used:
                reason = "prepared_already_used"
            elif prepared.options != options:
                reason = "options_changed"
            elif tuple(facts) != prepared.facts:
                reason = "facts_changed"
            elif (
                tracks_declarers
                and prepared.facts
                and prepared.edge_ranks != self._edge_precedence_ranks()
            ):
                # Always re-read: an edge write that matched its stored row
                # was dropped while preparing, but a ranking added since
                # makes it a declaration to record (item 92a backfill).
                reason = "edge_ranking_changed"
            elif prepared.key_fingerprint is None:
                reason = "no_key_fingerprint"
            else:
                try:
                    now = self._prepared_key_fingerprint(facts)
                except SubstrateError as exc:
                    reason = str(
                        (getattr(exc, "data", None) or {}).get("reason")
                        or "key_fingerprint_unavailable"
                    )
                else:
                    if now != prepared.key_fingerprint:
                        reason = "key_rows_changed"
            check_ms = round((time.perf_counter() - verify_started) * 1000, 3)
            if reason is None:
                state = prepared
                state.session = session
                state.timings_ms = {
                    f"prelock.{label}": value
                    for label, value in prepared.timings_ms.items()
                }
            else:
                state = self.prepare_apply_facts(
                    facts,
                    branch=options.branch,
                    source=options.source,
                    policy_fingerprint=options.policy_fingerprint,
                    run_id=options.run_id,
                    observed_at=options.observed_at,
                    strict_endpoints=options.strict_endpoints,
                    session=session,
                    delete_incident_edges=options.delete_incident_edges,
                    derived_reapplication=options.derived_reapplication,
                    node_retraction=options.node_retraction,
                    row_provenance=options.row_provenance,
                )
            state.total_started = verify_started
            state.timings_ms["prepared.lock_wait"] = lock_wait_ms
            state.timings_ms["prepared.check"] = check_ms
            if reason is None:
                state.timings_ms["prepared.reused"] = 1.0
            else:
                state.timings_ms["prepared.recomputed"] = 1.0
                state.timings_ms[f"prepared.recomputed.{reason}"] = 1.0
            result = self._write_prepared_apply(state, in_transaction=True)
        result.timings_ms["prepared.lock_hold"] = round(
            (time.perf_counter() - lock_started) * 1000, 3
        )
        return result

    @staticmethod
    def _stamp_publisher_derived_node_lineage(
        session: Any,
        *,
        node_upserts: list[dict[str, Any]],
        policy_fingerprint: str | None,
    ) -> dict[str, float]:
        """Give a derived NODE the lineage of the rows it was derived from.

        PACT ``bind-derived-node-provenance``. An edge can inherit from its
        two structural endpoints (see the sibling method); a node has none,
        so the emitting enricher declares its inputs via
        ``emit_node_fact(derived_from=...)``, which lands in
        ``source_record_id["derived_from"]``. Here — where a Chronos session
        exists to read them — those ids are resolved to their lineage and
        combined through the same meet the edge path uses.

        A node that declares nothing keeps its empty envelope and stays
        fail-closed. Inputs are NEVER inferred from graph adjacency: a node
        next to a trusted node need not be derived from it, so such a rule
        would launder trust while appearing principled.

        LIMIT OF THIS MECHANISM — the declaration is SELF-ASSERTED and is
        verified nowhere. The combine is a meet, so a TRUTHFUL declaration
        cannot raise trust above its inputs. But there is no taint tracking
        that checks a declaration against what the producer actually read,
        so a producer deriving from {trusted_A, untrusted_B} that declares
        only ``[trusted_A]`` inherits a trusted envelope. Note the incentive
        this creates: SELECTIVE omission is strictly better for a dishonest
        producer than total omission, which merely fails closed. So this is
        defense against accident and drift, not against a hostile producer,
        and it is NOT a no-laundering proof. Closing it needs the producer's
        output admitted against its actual inputs — PACT
        ``admit-derived-node-emission``, which is why derived node emission
        being ungoverned is tracked as its own hole.

        The combined envelope names only source ids the inputs already
        carry. It deliberately does not introduce the emitting producer as
        a lineage source id: readiness unions graph lineage source ids into
        ``policy_required_sources``, so a new id no policy covers would turn
        readiness red on every governed deployment.
        """
        declaring = [
            row for row in node_upserts
            if (row.get("source_record_id") or {}).get("derived_from")
        ]
        # A derived node that declares nothing is not an error here — it just
        # stays fail-closed. But it is invisible to Nomos-enforced reads and
        # counts toward the readiness quarantine, so the operator needs the
        # number rather than having to infer it from a red readiness check.
        counts: dict[str, float] = {
            "nodes_declaring_provenance": float(len(declaring)),
            "nodes_undeclared_provenance": float(
                len(node_upserts) - len(declaring)
            ),
        }
        if not declaring:
            return counts
        input_ids = sorted({
            str(node_id)
            for row in declaring
            for node_id in (row["source_record_id"]["derived_from"] or ())
            if node_id
        })
        if not input_ids:
            return counts
        declaring_ids = {str(row["node_id"]) for row in declaring}
        # Seed from nodes that are NOT themselves waiting on a declaration.
        # A declaring node's attrs envelope is still empty at this point, so
        # seeding it here would hand its own dependents that emptiness — the
        # publisher applies every enricher's facts in ONE batch, so a chain
        # built within a single cycle lands here together.
        lineage_by_node: dict[str, dict[str, Any]] = {
            str(row["node_id"]): lineage_for_row(
                row.get("attrs") or {},
                source_id=row.get("source"),
                source_record_id=row.get("source_record_id"),
                source_revision=row.get("source_revision"),
            )
            for row in node_upserts
            if str(row["node_id"]) not in declaring_ids
        }
        missing_ids = [
            node_id for node_id in input_ids
            if node_id not in lineage_by_node
        ]
        # Stored lineage for declared inputs that are ALSO being written in
        # this batch is kept aside rather than merged above, so the in-batch
        # fixpoint always prefers the fresh value over the stale row. It is
        # used only as the fallback below, which is what lets a node declare
        # a PRIOR VERSION OF ITSELF — re-emitting a node with new attrs while
        # keeping the provenance it already had. Without it, such a node would
        # resolve to nothing and silently lose lineage it legitimately held.
        stored_lineage: dict[str, dict[str, Any]] = {}
        if missing_ids:
            rows = session.query(
                """
                SELECT node_id, attrs, source, source_record_id, source_revision
                  FROM okg.graph_nodes
                 WHERE node_id = ANY(:node_ids)
                """,
                {"node_ids": missing_ids},
            )
            for row in rows:
                node_id = str(row["node_id"])
                lineage = lineage_for_row(
                    row.get("attrs") or {},
                    source_id=row.get("source"),
                    source_record_id=row.get("source_record_id"),
                    source_revision=row.get("source_revision"),
                )
                if node_id in declaring_ids:
                    stored_lineage[node_id] = lineage
                else:
                    lineage_by_node[node_id] = lineage

        def _declared_inputs(row: dict[str, Any]) -> list[str]:
            return [
                str(node_id)
                for node_id in (row["source_record_id"]["derived_from"] or ())
                if node_id
            ]

        # Resolve in dependency order by repeated passes. A declaring node
        # becomes resolvable once every input it names is resolved, so a
        # same-batch chain converges regardless of list order. Bounded by the
        # number of declaring nodes: each pass either resolves at least one or
        # stops. Whatever is left is cyclic (A derives from B, B from A) or
        # depends on something unresolvable, and stays fail-closed below.
        pending = {str(row["node_id"]): row for row in declaring}
        while pending:
            ready = [
                node_id for node_id, row in pending.items()
                if all(
                    dep in lineage_by_node for dep in _declared_inputs(row)
                )
            ]
            if not ready:
                break
            for node_id in ready:
                row = pending.pop(node_id)
                combined = combine_source_lineages(
                    [
                        lineage_by_node[dep]
                        for dep in _declared_inputs(row)
                    ],
                    policy_fingerprint=policy_fingerprint,
                )
                row["attrs"] = attrs_with_lineage(
                    dict(row.get("attrs") or {}),
                    combined,
                )
                lineage_by_node[node_id] = combined

        # Unresolved: at least one declared input could not be resolved. It
        # contributes None, which combine_source_lineages marks malformed, so
        # the whole envelope fails closed rather than silently narrowing to
        # the inputs that did resolve.
        for node_id, row in pending.items():
            combined = combine_source_lineages(
                [
                    # The stored fallback is SELF-DECLARATION ONLY: a node
                    # re-emitted as a new version of itself keeps the
                    # provenance it already had. It deliberately does not
                    # apply to other unresolved deps — falling back to
                    # another node's PRIOR envelope would resurrect stale,
                    # possibly more-trusted lineage for a dep that is
                    # unresolvable right now (e.g. its own input was
                    # retracted), making fail-closed depend on whether the
                    # row happened to be published before. Anything else
                    # contributes None, which combine_source_lineages marks
                    # malformed.
                    lineage_by_node.get(dep)
                    or (stored_lineage.get(dep) if dep == node_id else None)
                    for dep in _declared_inputs(row)
                ],
                policy_fingerprint=policy_fingerprint,
            )
            row["attrs"] = attrs_with_lineage(
                dict(row.get("attrs") or {}),
                combined,
            )
            # Deliberately NOT written back into lineage_by_node. Every node
            # left here is already unresolvable, so publishing its envelope
            # would only change whether a sibling sees None or this malformed
            # envelope — identical status either way, but a different stored
            # `sources` list, which makes persisted bytes depend on emission
            # order.
        counts["nodes_unresolved_provenance"] = float(len(pending))
        return counts

    @staticmethod
    def _stamp_publisher_derived_edge_lineage(
        session: Any,
        *,
        edge_upserts: list[dict[str, Any]],
        node_upserts: list[dict[str, Any]],
        policy_fingerprint: str | None,
    ) -> None:
        if not edge_upserts:
            return
        endpoint_ids = sorted({
            str(node_id)
            for row in edge_upserts
            for node_id in (row["src"], row["dst"])
            if node_id
        })
        if not endpoint_ids:
            return
        lineage_by_node: dict[str, dict[str, Any]] = {}
        for row in node_upserts:
            node_id = str(row["node_id"])
            lineage_by_node[node_id] = lineage_for_row(
                row.get("attrs") or {},
                source_id=row.get("source"),
                source_record_id=row.get("source_record_id"),
                source_revision=row.get("source_revision"),
            )
        missing_ids = [
            node_id for node_id in endpoint_ids if node_id not in lineage_by_node
        ]
        if missing_ids:
            rows = session.query(
                """
                SELECT node_id, attrs, source, source_record_id, source_revision
                  FROM okg.graph_nodes
                 WHERE node_id = ANY(:node_ids)
                """,
                {"node_ids": missing_ids},
            )
            for row in rows:
                lineage_by_node[str(row["node_id"])] = lineage_for_row(
                    row.get("attrs") or {},
                    source_id=row.get("source"),
                    source_record_id=row.get("source_record_id"),
                    source_revision=row.get("source_revision"),
                )
        for row in edge_upserts:
            attrs = dict(row.get("attrs") or {})
            existing = attrs.get(LINEAGE_ATTR)
            source_ids = source_ids_from_lineage(
                existing if isinstance(existing, Mapping) else None
            )
            if source_ids and "_publisher_derived" not in source_ids:
                continue
            row["attrs"] = attrs_with_lineage(
                attrs,
                combine_source_lineages(
                    [
                        lineage_by_node.get(str(row["src"])),
                        lineage_by_node.get(str(row["dst"])),
                    ],
                    policy_fingerprint=policy_fingerprint,
                ),
            )

    @staticmethod
    def _add_node_change_class_timings(
        timings_ms: dict[str, float],
        node_attr_changes_by_subtype: Mapping[
            str,
            Mapping[str, Iterable[str]],
        ],
    ) -> None:
        """Expose row churn shape in graph-write timings.

        The replay bottleneck needs to distinguish semantic churn from line/span
        churn without doing another graph read. The changed-field map is already
        computed for dirty-scope propagation, so these counters are just an
        in-memory aggregation.
        """
        inserted = 0
        deleted = 0
        location_only = 0
        metadata_only = 0
        semantic_or_identity = 0
        total = 0
        field_counts: dict[str, int] = {}

        for subtype, changes_by_node in node_attr_changes_by_subtype.items():
            subtype_total = 0
            subtype_inserted = 0
            subtype_deleted = 0
            subtype_location_only = 0
            subtype_metadata_only = 0
            subtype_semantic_or_identity = 0
            for changed_fields in changes_by_node.values():
                fields = {str(field) for field in changed_fields}
                if not fields:
                    continue
                total += 1
                subtype_total += 1
                for field in fields:
                    field_counts[field] = field_counts.get(field, 0) + 1
                if "__new__" in fields:
                    inserted += 1
                    subtype_inserted += 1
                elif "__deleted__" in fields:
                    deleted += 1
                    subtype_deleted += 1
                else:
                    material_fields = fields - _METADATA_ONLY_NODE_ATTRS
                    if (
                        material_fields
                        and material_fields <= _LOCATION_ONLY_NODE_ATTRS
                    ):
                        location_only += 1
                        subtype_location_only += 1
                    elif not material_fields:
                        metadata_only += 1
                        subtype_metadata_only += 1
                    else:
                        semantic_or_identity += 1
                        subtype_semantic_or_identity += 1

            if subtype_total:
                prefix = f"node_change_class.by_subtype.{subtype}"
                timings_ms[f"{prefix}.total"] = float(subtype_total)
                if subtype_inserted:
                    timings_ms[f"{prefix}.inserted"] = float(subtype_inserted)
                if subtype_deleted:
                    timings_ms[f"{prefix}.deleted"] = float(subtype_deleted)
                if subtype_location_only:
                    timings_ms[f"{prefix}.location_only"] = float(
                        subtype_location_only
                    )
                if subtype_metadata_only:
                    timings_ms[f"{prefix}.metadata_only"] = float(
                        subtype_metadata_only
                    )
                if subtype_semantic_or_identity:
                    timings_ms[f"{prefix}.semantic_or_identity"] = float(
                        subtype_semantic_or_identity
                    )

        if not total:
            return
        timings_ms["node_change_class.total"] = float(total)
        timings_ms["node_change_class.inserted"] = float(inserted)
        timings_ms["node_change_class.deleted"] = float(deleted)
        timings_ms["node_change_class.location_only"] = float(location_only)
        timings_ms["node_change_class.metadata_only"] = float(metadata_only)
        timings_ms["node_change_class.semantic_or_identity"] = float(
            semantic_or_identity
        )
        for field, count in sorted(
            field_counts.items(),
            key=lambda item: (-item[1], item[0]),
        )[:20]:
            timings_ms[f"node_change_fields.{field}"] = float(count)

    @staticmethod
    def _node_attr_changes_by_subtype(
        session,
        *,
        node_upserts: list[dict[str, Any]],
        node_deletes: list[dict[str, Any]],
        prior_by_id: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> dict[str, dict[str, tuple[str, ...]]]:
        node_ids = list(dict.fromkeys(
            [
                str(row["node_id"])
                for row in node_upserts
            ]
            + [
                str(row["node_id"])
                for row in node_deletes
            ]
        ))
        if not node_ids:
            return {}
        if prior_by_id is None:
            prior_by_id = ChronosGraphWriteMixin._prior_nodes_by_id(
                session,
                node_ids,
            )
        out: dict[str, dict[str, tuple[str, ...]]] = {}
        for row in node_upserts:
            node_id = str(row["node_id"])
            subtype = str(row["subtype"])
            attrs = dict(row.get("attrs") or {})
            prior = prior_by_id.get(node_id)
            if prior is None:
                changed = {"__new__", *attrs.keys()}
            else:
                prior_subtype = str(prior.get("subtype") or "")
                prior_attrs = dict(prior.get("attrs") or {})
                keys = set(prior_attrs) | set(attrs)
                changed = {
                    key for key in keys
                    if canonical(prior_attrs.get(key, MISSING))
                    != canonical(attrs.get(key, MISSING))
                }
                if prior_subtype != subtype:
                    changed.add("__subtype__")
                # A SOURCE reattribution (the node's dedicated `source` column
                # changed on an upsert) moves its by-source count bucket while the
                # node id is unchanged. The console count rollup (console-count-
                # rollup / #444 slice 2) forces a full recount on this marker; it
                # is stamped on the UPSERT path only -- a DELETE's source -1 is
                # fold-owned, not force-owned, so the delete loop below must NOT
                # stamp it (stamping both would double-count the move).
                prior_source = str(prior.get("source") or "")
                new_source = str(row.get("source") or "")
                if prior_source != new_source:
                    changed.add("__source__")
            out.setdefault(subtype, {})[node_id] = tuple(sorted(changed))
        for row in node_deletes:
            node_id = str(row["node_id"])
            prior = prior_by_id.get(node_id)
            subtype = (
                str(prior.get("subtype"))
                if prior is not None else "__unknown__"
            )
            prior_attrs = dict(prior.get("attrs") or {}) if prior is not None else {}
            out.setdefault(subtype, {})[node_id] = tuple(
                sorted({"__deleted__", *prior_attrs.keys()})
            )
        return out

    @staticmethod
    def _edge_attr_changes_by_type(
        *,
        edge_upserts: list[dict[str, Any]],
        edge_deletes: list[dict[str, Any]],
        prior_by_id: Mapping[int, Mapping[str, Any]],
    ) -> dict[str, dict[int, tuple[str, ...]]]:
        out: dict[str, dict[int, tuple[str, ...]]] = {}
        for row in edge_upserts:
            edge_key = int(row["edge_id"])
            edge_type = str(row["edge_type"])
            attrs = dict(row.get("attrs") or {})
            prior = prior_by_id.get(edge_key)
            if prior is None:
                changed = {"__new__", *attrs.keys()}
            else:
                prior_type = str(prior.get("edge_type") or "")
                prior_attrs = dict(prior.get("attrs") or {})
                keys = set(prior_attrs) | set(attrs)
                changed = {
                    key for key in keys
                    if canonical(prior_attrs.get(key, MISSING))
                    != canonical(attrs.get(key, MISSING))
                }
                if prior_type != edge_type:
                    changed.add("__edge_type__")
                # confidence/provenance live in dedicated columns, not attrs, so
                # a change to either is invisible to the attr diff above; mark it
                # explicitly so the edge lands in the dirty scope (an edge whose
                # only change is column-only confidence/provenance is a real
                # change, per the no-op filter).
                if str(prior.get("provenance") or "") != str(
                    row.get("provenance") or ""
                ):
                    changed.add("__provenance__")
                if float(prior.get("confidence") or 0.0) != float(
                    row.get("confidence") or 0.0
                ):
                    changed.add("__confidence__")
            out.setdefault(edge_type, {})[edge_key] = tuple(sorted(changed))
        for row in edge_deletes:
            edge_key = int(row["edge_id"])
            prior = prior_by_id.get(edge_key)
            edge_type = (
                str(prior.get("edge_type"))
                if prior is not None else "__unknown__"
            )
            prior_attrs = dict(prior.get("attrs") or {}) if prior is not None else {}
            out.setdefault(edge_type, {})[edge_key] = tuple(
                sorted({"__deleted__", *prior_attrs.keys()})
            )
        return out

    @staticmethod
    def _prior_nodes_by_id(
        session: Any,
        node_ids: list[str] | tuple[str, ...],
    ) -> dict[str, Mapping[str, Any]]:
        if not node_ids:
            return {}
        rows = ChronosGraphWriteMixin._visible_rows_for_keys_fast_path(
            session,
            NODE_TABLE,
            [{"node_id": str(node_id)} for node_id in node_ids],
        )
        if rows is not None:
            return {str(row["node_id"]): row for row in rows}
        rows = session.query(
            """
            SELECT node_id, subtype, attrs, source,
                   source_record_id, source_revision
              FROM okg.graph_nodes
             WHERE node_id = ANY(:node_ids)
            """,
            {"node_ids": list(node_ids)},
        )
        return {str(row["node_id"]): row for row in rows}

    @staticmethod
    def _prior_edges_by_id(
        session: Any,
        edge_ids: list[int] | tuple[int, ...],
    ) -> dict[int, Mapping[str, Any]]:
        if not edge_ids:
            return {}
        rows = ChronosGraphWriteMixin._visible_rows_for_keys_fast_path(
            session,
            EDGE_TABLE,
            [{"edge_id": int(edge_id)} for edge_id in edge_ids],
        )
        if rows is not None:
            return {int(row["edge_id"]): row for row in rows}
        rows = session.query(
            """
            SELECT edge_id, src, dst, edge_type, attrs,
                   provenance, confidence, source,
                   source_record_id, source_revision
              FROM okg.graph_edges
             WHERE edge_id = ANY(:edge_ids)
            """,
            {"edge_ids": list(edge_ids)},
        )
        return {int(row["edge_id"]): row for row in rows}

    @staticmethod
    def _visible_rows_for_keys_fast_path(
        session: Any,
        table: str,
        keys: list[dict[str, Any]],
    ) -> list[dict[str, Any]] | None:
        context = getattr(session, "_context", None)
        backend = getattr(context, "_backend", None)
        ref = getattr(session, "_ref", None)
        ensure_fresh = getattr(session, "_ensure_fresh", None)
        visible_rows_for_keys = getattr(backend, "_visible_rows_for_keys", None)
        prepared_segment = getattr(backend, "_prepared_segment", None)
        meta_for_ref = getattr(backend, "_meta_for_ref", None)
        if (
            not callable(visible_rows_for_keys)
            or not callable(prepared_segment)
            or not callable(meta_for_ref)
            or ref is None
        ):
            return None
        if callable(ensure_fresh):
            ensure_fresh()
            ref = getattr(session, "_ref", ref)
        segment = prepared_segment(ref)
        meta = meta_for_ref(ref, table)
        return [
            dict(row)
            for row in visible_rows_for_keys(table, keys, segment, meta=meta)
        ]

    @staticmethod
    def _node_upsert_semantic_noop(
        row: Mapping[str, Any],
        prior: Mapping[str, Any] | None,
    ) -> bool:
        """Whether an upsert would store nothing new (issue #2335).

        Decided by the one rule the source runner's persisted dedupe uses
        (`okg.substrate.fact_equivalence`): attrs compared as JSON, with the
        source revision inside the lineage envelope ignored. So is the
        ``source_revision`` column, which only mirrors that revision; the
        owning ``source`` and ``source_record_id`` still count. Without this,
        a writer outside the runner stored a new version of unchanged
        content every time an input's revision moved.
        """
        if prior is None:
            return False
        return (
            str(row.get("subtype") or "") == str(prior.get("subtype") or "")
            and compare_attrs(
                dict(row.get("attrs") or {}), dict(prior.get("attrs") or {}),
            ).unchanged
            and row.get("source") == prior.get("source")
            and dict(row.get("source_record_id") or {})
            == dict(prior.get("source_record_id") or {})
        )

    @staticmethod
    def _is_substitute_node(row: Mapping[str, Any]) -> bool:
        return row.get("_ownership_mode") == "substitute"

    @staticmethod
    def _substitute_would_clobber(
        row: Mapping[str, Any],
        prior: Mapping[str, Any] | None,
    ) -> bool:
        """Let a substitute fill absence without taking another source's node.

        ``ownership_mode=substitute`` is an explicit source contract. It may
        create an absent identity or update the same source's existing row,
        but it cannot reattribute a row currently owned by another source.
        The decision is revalidated after the branch lock to make the contract
        hold under concurrent authoritative and substitute writers.

        KNOWN GAP (see PR discussion): ``source`` is stamped once per substrate
        source, so for a COMPOSITE source such as ``ExternalRepoFactory`` --
        which runs git_files, git_history, and code_structure as sub-adapters of
        one source -- every fact carries an identical value and this comparison
        never fires. Tightening it to fill-absence-only does NOT work: it breaks
        the legitimate authoritative -> substitute transition when a file leaves
        HEAD (git_files retracts and git_history substitutes in the same batch,
        which then collapses to a bare delete and cascades incident edges). A
        real fix belongs on the emission side -- git-history should not supply an
        endpoint for a path git_files authoritatively owns -- or requires
        persisting the ownership mode so prior rows can be compared by mode.
        """
        if prior is None:
            return False
        return (
            ChronosGraphWriteMixin._is_substitute_node(row)
            and prior.get("source") != row.get("source")
        )

    @staticmethod
    def _refuse_moved_provenance_rows(
        node_upserts: list[dict[str, Any]],
        current_priors: Mapping[str, Mapping[str, Any]],
        row_provenance: Mapping[str, Mapping[str, Any]],
    ) -> None:
        """Refuse a provenance-preserving rewrite whose rows moved (#3318).

        Read under the branch write lock. A caller that read its rows
        without holding that lock could find a row another writer retracted
        or gave a new source since; writing the caller's copy would put back
        a row or an owner that is no longer true. Nothing is written then.
        This compares the source only: a same-source update in that window
        is not detected here, which is why callers hold the lock across
        their read (review S1 of #3318).
        """
        moved = []
        for row in node_upserts:
            node_id = str(row["node_id"])
            expected = row_provenance[node_id]["expected_source"]
            prior = current_priors.get(node_id)
            if prior is None or prior.get("source") != expected:
                moved.append({
                    "node_id": node_id,
                    "expected_source": expected,
                    "current_source": (
                        None if prior is None else prior.get("source")
                    ),
                    "present": prior is not None,
                })
        if moved:
            raise SubstrateError(
                "a node changed while its provenance was being preserved",
                data={
                    "reason": "row_provenance_rows_moved",
                    "moved": moved[:20],
                    "moved_count": len(moved),
                },
                fix_hint=(
                    "another writer changed these nodes after they were "
                    "read; rerun the command, which reads them again"
                ),
            )

    def _tracks_node_declarers(self) -> bool:
        """Shared-node declarers are kept on PostgreSQL, like the write fence.

        ``_lock_branch_write`` fences only PostgreSQL. Without the fence the
        declarer read and the delete could interleave with another writer, so
        other dialects keep the earlier delete-on-retraction behavior.
        """
        return getattr(self.context.db, "dialect", None) == "postgres"

    def _require_node_declarers_table(self) -> None:
        """Make sure ``okg.graph_node_declarers`` is a registered table.

        ``okg migrate`` and a fresh provision register it. A deployment that
        runs this code before migrating gets it here, the same self-heal the
        rollup tables use, unless the caller owns an open transaction on the
        connection: registering would commit the caller's work, so the write
        is refused instead, naming the fix.
        """
        from okg.substrate.graph_store_internal.node_declarers import (
            NODE_DECLARERS_TABLE,
        )

        if getattr(self, "_node_declarers_registered", False):
            return
        backend = self.context._backend
        if NODE_DECLARERS_TABLE not in backend.tables:
            backend.refresh_registries()
        if NODE_DECLARERS_TABLE not in backend.tables:
            # An autocommit context owns its connection and commits any open
            # work itself before each write transaction; a caller-owned
            # context (autocommit=False) with an open transaction does not.
            if not self.context.autocommit and self.context.db.in_transaction:
                raise SubstrateError(
                    "shared-node declarer table is not registered",
                    data={
                        "table": NODE_DECLARERS_TABLE,
                        "caller_transaction_open": True,
                    },
                    fix_hint=(
                        "run `okg migrate` for this deployment; the graph "
                        "writer cannot register the table inside the "
                        "caller's open transaction"
                    ),
                )
            self.ensure_node_declarers_table()
            backend.refresh_registries()
            if NODE_DECLARERS_TABLE not in backend.tables:
                raise SubstrateError(
                    "shared-node declarer table did not register",
                    data={"table": NODE_DECLARERS_TABLE},
                    fix_hint="run `okg migrate` and inspect its output",
                )
        self._node_declarers_registered = True

    def _apply_node_declarers(
        self,
        session: Any,
        *,
        source: str | None,
        node_deletes: list[dict[str, Any]],
        batch_node_upsert_ids: set[str],
        substitute_node_upserts: list[dict[str, Any]],
        node_upserts: list[dict[str, Any]],
        prior_nodes_by_id: dict[str, Any],
        timings_ms: dict[str, float],
        purge: bool = False,
        searchable_fields_cache: dict[str, list[str]] | None = None,
    ) -> list[dict[str, Any]]:
        """Record declarers and pass retracted shared nodes on (issue #1901).

        Runs inside the graph write transaction after ``_lock_branch_write``.
        It re-reads the retracted rows and every declaration of them, so the
        ownership read before the fence cannot decide a delete. Hand-off rows
        are appended to ``node_upserts``; the returned list is the node deletes
        to apply, replacing the ones computed before the fence.
        """
        from okg.substrate.graph_store_internal.node_declarers import (
            NODE_DECLARERS_TABLE,
            load_declarations,
            reconcile_node_declarers,
        )

        retracted_ids = sorted({
            str(row["node_id"])
            for row in node_deletes
            if str(row["node_id"]) not in batch_node_upsert_ids
        })
        current_nodes = (
            self._prior_nodes_by_id(session, retracted_ids)
            if retracted_ids else {}
        )
        for node_id in retracted_ids:
            prior_nodes_by_id.pop(node_id, None)
        prior_nodes_by_id.update(current_nodes)
        declarations = load_declarations(
            session,
            [
                *retracted_ids,
                *(str(row["node_id"]) for row in substitute_node_upserts),
            ],
        )
        outcome = reconcile_node_declarers(
            source=source,
            retracted_node_ids=retracted_ids,
            still_declared_node_ids=batch_node_upsert_ids,
            substitute_rows=substitute_node_upserts,
            current_nodes=current_nodes,
            declarations=declarations,
            purge=purge,
        )
        handoffs, refused = self._prepare_handoff_rows(
            outcome.handoffs,
            current_nodes=current_nodes,
            searchable_fields_cache=searchable_fields_cache,
        )
        node_deletes_to_apply = [
            *outcome.node_deletes,
            *({"node_id": node_id} for node_id in refused),
        ]
        refused = [*outcome.refused_subtype, *refused]
        if outcome.declaration_deletes or outcome.declaration_upserts:
            apply_table_edits = getattr(session, "apply_table_edits", None)
            if callable(apply_table_edits):
                apply_table_edits(
                    NODE_DECLARERS_TABLE,
                    deletes=outcome.declaration_deletes,
                    upserts=outcome.declaration_upserts,
                )
            else:
                if outcome.declaration_deletes:
                    session.delete_keys(
                        NODE_DECLARERS_TABLE, outcome.declaration_deletes,
                    )
                if outcome.declaration_upserts:
                    session.upsert_rows(
                        NODE_DECLARERS_TABLE, outcome.declaration_upserts,
                    )
        node_upserts.extend(handoffs)
        timings_ms["transaction.node_declarers.recorded"] = float(
            len(outcome.declaration_upserts)
        )
        timings_ms["transaction.node_declarers.withdrawn"] = float(
            len(outcome.declaration_deletes)
        )
        timings_ms["transaction.node_declarers.handoffs"] = float(
            len(handoffs)
        )
        timings_ms["transaction.node_declarers.handoffs_refused_subtype"] = float(
            len(refused)
        )
        return node_deletes_to_apply

    def _prepare_handoff_rows(
        self,
        handoffs: list[dict[str, Any]],
        *,
        current_nodes: Mapping[str, Mapping[str, Any]],
        searchable_fields_cache: dict[str, list[str]] | None,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        """Prepare hand-off rows the way a written node row is prepared.

        The stored declaration already carries the declarer's own lineage
        and policy fingerprint, stamped when that declarer wrote it; those are
        kept, because they are the survivor's attribution, not this caller's.
        What is redone here: the searchable text and text hash under today's
        searchable-field rules, and the derived-node boundary check.

        ``reconcile_node_declarers`` already chooses the survivor only among
        copies with the live row's subtype; this re-check is a backstop. A
        declaration whose subtype differs from the live row's is not
        written: the live row is the one the catalog last admitted, and a
        stored copy under another subtype may name one the catalog no longer
        allows. That node is deleted instead, as before this change, and
        counted. The publish consistency gate still checks every live node's
        subtype against the active catalog.
        """
        from okg.substrate.derived_node_liveness import validate_derived_node_row

        prepared: list[dict[str, Any]] = []
        refused: list[str] = []
        cache = searchable_fields_cache if searchable_fields_cache is not None else {}
        for row in handoffs:
            node_id = str(row["node_id"])
            current = current_nodes.get(node_id) or {}
            if str(row["subtype"]) != str(current.get("subtype") or ""):
                refused.append(node_id)
                continue
            # A stored declaration carries this stamper's output. Turn it back
            # into producer attrs first (issue #2991), so a body kept under
            # the reserved copy survives even if the declaration it was
            # stamped under has changed since, and a plain old rollup is not
            # read as a body.
            attrs = self._stamp_searchable_text(
                str(row["subtype"]),
                self._unstamp_stored_attrs(
                    str(row["subtype"]), dict(row.get("attrs") or {}), cache=cache,
                ),
                cache=cache,
            )
            text = attrs.get("text")
            prepared_row = {
                **row,
                "attrs": attrs,
                "text_hash": None if text is None else md5_text(str(text)),
            }
            try:
                validate_derived_node_row(prepared_row)
            except ValueError as exc:
                raise SubstrateError(
                    "shared-node hand-off row has no admitted support declaration",
                    data={
                        "node_id": node_id,
                        "source": str(row.get("source") or ""),
                        "reason": str(exc),
                    },
                    fix_hint=(
                        "a derived node cannot be handed off between sources; "
                        "inspect okg.graph_node_declarers for this node"
                    ),
                ) from exc
            prepared.append(prepared_row)
        return prepared, refused

    def withdraw_node_declarations(
        self,
        *,
        branch: str,
        sources: Iterable[str],
        run_id: str | None = None,
        session: Any | None = None,
    ) -> int:
        """Delete recorded shared-node declarations of ``sources`` (issue #1901).

        For paths that remove a source's writes without retracting each node
        through this source: source retirement (every declaration of the
        retired source) and compensation or quarantine rejection of one run
        (``run_id``: only the declarations that run recorded). Runs in its own
        graph write transaction after the branch write fence, nested in the
        caller's transaction when there is one. Returns the rows deleted.

        A database whose declarer table is not registered has no declarations
        to withdraw, so this returns 0 without registering it.
        """
        from okg.substrate.graph_store_internal.node_declarers import (
            NODE_DECLARERS_TABLE,
        )

        wanted = sorted({str(source) for source in sources if source})
        if not wanted or not self._tracks_node_declarers():
            return 0
        backend = self.context._backend
        if NODE_DECLARERS_TABLE not in backend.tables:
            backend.refresh_registries()
        if NODE_DECLARERS_TABLE not in backend.tables:
            return 0
        if session is None:
            session = self.checkout(branch)
        sql = (
            f"SELECT node_id, source FROM {NODE_DECLARERS_TABLE} "
            "WHERE source = ANY(:sources)"
        )
        params: dict[str, Any] = {"sources": wanted}
        if run_id is not None:
            sql += " AND run_id = :run_id"
            params["run_id"] = str(run_id)
        with session.transaction():
            self._lock_branch_write(branch)
            keys = [
                {"node_id": str(row["node_id"]), "source": str(row["source"])}
                for row in session.query(sql, params)
            ]
            if keys:
                apply_table_edits = getattr(session, "apply_table_edits", None)
                if callable(apply_table_edits):
                    apply_table_edits(NODE_DECLARERS_TABLE, deletes=keys, upserts=[])
                else:
                    session.delete_keys(NODE_DECLARERS_TABLE, keys)
        return len(keys)

    def _edge_precedence_ranks(self) -> dict[str, dict[str, int]]:
        """The deployment's edge ranking as ``okg migrate`` / the worker
        copied it into ``okg.edge_precedence`` (item 92a)."""
        from okg.substrate.graph_store_internal.edge_declarers import (
            load_edge_precedence,
        )

        return load_edge_precedence(self.context.db)

    def _lock_edge_ranking_shared(self) -> None:
        """Hold the edge ranking steady for this transaction (item 92a).

        Every path that writes ranked edges or edge declarations takes it
        shared; ``apply_deployment_edge_precedence`` takes the same key
        exclusively before it changes the ranking and purges, so a ranking
        change waits for in-flight ranked writes and they wait for it.

        The lock order is NOT the same everywhere: the store's own writes
        take it before their branch fence, but merge-back, source
        retirement, the publisher, entity merge, remediation, the transform
        gate and the undo to a run base hold a branch lock first and reach
        this lock later. The rule that keeps that deadlock-free is that the
        ranking change always yields: it takes each branch lock under a
        lock timeout shorter than ``deadlock_timeout`` and refuses with
        :class:`EdgeRankingChangeBusy` instead of waiting
        (:meth:`_lock_branch_write_yielding`).
        """
        db = self.context.db
        if getattr(db, "dialect", None) != "postgres":
            return
        db.execute(
            "SELECT pg_advisory_xact_lock_shared(hashtext(?), hashtext(?))",
            list(_EDGE_RANKING_LOCK),
        )

    def _refuse_if_edge_ranking_changed(
        self, planned: dict[str, dict[str, int]], *, branch: str,
    ) -> None:
        """Refuse a write planned with a ranking that has since changed.

        Called under the shared ranking lock and the branch fence, before
        anything is written. The write is not replanned: inside a
        caller-owned transaction the refusal rolls the whole transaction
        back, so retrying here would silently drop the caller's earlier
        work.
        """
        from okg.substrate.graph_store_internal.edge_declarers import (
            EdgeRankingChangedDuringWrite,
        )

        current = self._edge_precedence_ranks()
        if current == planned:
            return
        raise EdgeRankingChangedDuringWrite(
            "the edge ranking changed during this write; nothing was written",
            data={
                "branch": branch,
                "planned_edge_types": sorted(planned),
                "current_edge_types": sorted(current),
            },
            fix_hint=(
                "the edge ranking changed during this write; rerun it (inside "
                "a caller-owned transaction the whole transaction was rolled "
                "back, so rerun the caller's work)"
            ),
        )

    def _lock_branch_write_yielding(
        self, branch: str, *, deployment_name: str | None,
    ) -> None:
        """Take a branch write fence for a ranking change, or give way.

        The ranking change holds the ranking lock exclusively. A writer that
        holds this branch lock and then waits for the ranking lock would
        deadlock with it, so the wait here is bounded by a lock timeout of a
        quarter of ``deadlock_timeout``: on timeout the ranking change is
        refused (:class:`EdgeRankingChangeBusy`), its transaction rolls
        back, and the branch holder's work goes on.
        """
        from okg.substrate.graph_store_internal.edge_declarers import (
            EdgeRankingChangeBusy,
        )

        db = self.context.db
        if getattr(db, "dialect", None) != "postgres":
            self._lock_branch_write(branch)
            return
        previous = _first_value(
            db.execute("SELECT current_setting('lock_timeout')").fetchone()
        )
        db.execute(
            "SELECT set_config('lock_timeout', GREATEST(1, (EXTRACT(EPOCH FROM "
            "current_setting('deadlock_timeout')::interval) * 250)::int)::text "
            "|| 'ms', true)"
        )
        try:
            db.execute(
                "SELECT pg_advisory_xact_lock(hashtext(?), hashtext(?))",
                ["okg.chronos_graph", branch],
            )
        except Exception as exc:
            if not _is_lock_timeout(exc):
                raise
            deployment = deployment_name or "<deployment>"
            raise EdgeRankingChangeBusy(
                f"graph writers are busy on branch {branch!r}; the edge "
                "ranking change was not applied",
                data={"branch": branch, "deployment": deployment_name},
                fix_hint=(
                    f"graph writers are busy on branch {branch}; retry the "
                    "ranking change (rerun `okg migrate --deployment "
                    f"{deployment} --apply`, or restart the worker). Nothing "
                    "was changed"
                ),
            ) from exc
        db.execute("SELECT set_config('lock_timeout', ?, true)", [str(previous)])
        self._assert_paired_workspace_writable(branch)

    def _edge_declarers_table_registered(self) -> bool:
        """Whether ``okg.graph_edge_declarers`` is registered; reads only."""
        from okg.substrate.graph_store_internal.edge_declarers import (
            EDGE_DECLARERS_TABLE,
        )

        backend = self.context._backend
        if EDGE_DECLARERS_TABLE not in backend.tables:
            backend.refresh_registries()
        return EDGE_DECLARERS_TABLE in backend.tables

    def _require_edge_declarers_table(self) -> None:
        """Make sure ``okg.graph_edge_declarers`` is a registered table.

        The same self-heal as :meth:`_require_node_declarers_table`: ``okg
        migrate`` and a fresh provision register it; a writer that reaches a
        ranked edge first registers it, unless the caller owns an open
        transaction, in which case the write is refused naming the fix.
        """
        from okg.substrate.graph_store_internal.edge_declarers import (
            EDGE_DECLARERS_TABLE,
        )

        if getattr(self, "_edge_declarers_registered", False):
            return
        backend = self.context._backend
        if EDGE_DECLARERS_TABLE not in backend.tables:
            backend.refresh_registries()
        if EDGE_DECLARERS_TABLE not in backend.tables:
            if not self.context.autocommit and self.context.db.in_transaction:
                raise SubstrateError(
                    "shared-edge declarer table is not registered",
                    data={
                        "table": EDGE_DECLARERS_TABLE,
                        "caller_transaction_open": True,
                    },
                    fix_hint=(
                        "run `okg migrate` for this deployment; the graph "
                        "writer cannot register the table inside the "
                        "caller's open transaction"
                    ),
                )
            self.ensure_edge_declarers_table()
            backend.refresh_registries()
            if EDGE_DECLARERS_TABLE not in backend.tables:
                raise SubstrateError(
                    "shared-edge declarer table did not register",
                    data={"table": EDGE_DECLARERS_TABLE},
                    fix_hint="run `okg migrate` and inspect its output",
                )
        self._edge_declarers_registered = True

    def _apply_edge_declarers(
        self,
        session: Any,
        *,
        source: str | None,
        ranks: Mapping[str, Mapping[str, int]],
        ranked_types_for_source: set[str],
        unranked_writes: list[int],
        upserts: list[dict[str, Any]],
        withdrawn_edge_ids: list[int],
        retracted_node_ids: list[str],
        removed_node_ids: list[str],
        timings_ms: dict[str, float],
        prepared: "_RankedEdgeWrite | None" = None,
    ) -> "_RankedEdgeWrite":
        """Record declarations of ranked edges and store each edge's winner.

        Runs inside the graph write transaction after ``_lock_branch_write``,
        so every row it decides from is read after the fence. Writes the
        declarer-table edits itself and returns the graph edge rows to write
        and delete, with the current rows they replace.

        ``unranked_writes``: edges of a ranked type this source writes on
        today's path because the ranking does not name it. Writing one that a
        ranked producer declares is refused: the ranking cannot order them.

        ``prepared``: the decision ``_decide_edge_declarers`` made while the
        write was prepared ahead of the lock (issue #2941). The verified
        prepared path passes it only when the key fingerprint, which covers
        the touched edges' stored rows and declarations, and the ranking are
        unchanged; the reads and the reconcile are then not repeated.
        """
        from okg.substrate.graph_store_internal.edge_declarers import (
            EDGE_DECLARERS_TABLE,
            EdgePrecedenceUnranked,
            load_declarations,
        )

        if unranked_writes:
            declared = load_declarations(session, unranked_writes)
            if declared:
                edge_id_value, declarations_of = sorted(declared.items())[0]
                edge_type = str(declarations_of[0].get("edge_type") or "")
                raise EdgePrecedenceUnranked(
                    "an edge is declared by several producers and the ranking "
                    "cannot order them",
                    data={
                        "edge_id": int(edge_id_value),
                        "edge_type": edge_type,
                        "declaring_sources": sorted(
                            {str(d["source"]) for d in declarations_of}
                            | {str(source)}
                        ),
                        "unranked_sources": [str(source)],
                        "colliding_edges": len(declared),
                    },
                    fix_hint=(
                        f"add {source} to edge_precedence.{edge_type} in "
                        "deployment.yaml, run okg migrate --deployment <name> "
                        "--apply, and restart the worker"
                    ),
                )
        if prepared is not None and not (retracted_node_ids or removed_node_ids):
            ranked = prepared
            timings_ms["transaction.edge_declarers.prepared_reused"] = 1.0
        else:
            ranked = self._decide_edge_declarers(
                session,
                source=source,
                ranks=ranks,
                ranked_types_for_source=ranked_types_for_source,
                upserts=upserts,
                withdrawn_edge_ids=withdrawn_edge_ids,
                retracted_node_ids=retracted_node_ids,
                removed_node_ids=removed_node_ids,
                timings_ms=timings_ms,
                label="transaction.edge_declarers",
            )
        outcome = ranked.outcome
        step_started = time.perf_counter()
        if outcome.declaration_deletes or outcome.declaration_upserts:
            apply_table_edits = getattr(session, "apply_table_edits", None)
            if callable(apply_table_edits):
                apply_table_edits(
                    EDGE_DECLARERS_TABLE,
                    deletes=outcome.declaration_deletes,
                    upserts=outcome.declaration_upserts,
                )
            else:
                if outcome.declaration_deletes:
                    session.delete_keys(
                        EDGE_DECLARERS_TABLE, outcome.declaration_deletes,
                    )
                if outcome.declaration_upserts:
                    session.upsert_rows(
                        EDGE_DECLARERS_TABLE, outcome.declaration_upserts,
                    )
        _add_ms(timings_ms, "transaction.edge_declarers.write_declarations", step_started)
        timings_ms["transaction.edge_declarers.recorded"] = float(
            len(outcome.declaration_upserts)
        )
        timings_ms["transaction.edge_declarers.withdrawn"] = float(
            len(outcome.declaration_deletes)
        )
        timings_ms["transaction.edge_declarers.adopted"] = float(
            len(outcome.adopted)
        )
        timings_ms["transaction.edge_declarers.handoffs"] = float(
            len(outcome.handoffs)
        )
        timings_ms["transaction.edge_declarers.purged_edges"] = float(
            ranked.purged_edges
        )
        return ranked

    def _decide_edge_declarers(
        self,
        session: Any,
        *,
        source: str | None,
        ranks: Mapping[str, Mapping[str, int]],
        ranked_types_for_source: set[str],
        upserts: list[dict[str, Any]],
        withdrawn_edge_ids: list[int],
        retracted_node_ids: list[str],
        removed_node_ids: list[str],
        timings_ms: dict[str, float],
        label: str,
    ) -> "_RankedEdgeWrite":
        """Read the touched ranked edges' stored rows and declarations and
        decide the declarer edits and the rows to store; writes nothing.

        Under the branch write fence for a direct write, or ahead of the lock
        for a prepared one whose key fingerprint covers every row read here
        (issue #2941).
        """
        from okg.substrate.graph_store_internal.edge_declarers import (
            load_declarations,
            reconcile_edge_declarers,
        )

        retracted = sorted(set(retracted_node_ids) | set(removed_node_ids))
        removed = set(removed_node_ids)
        incident_rows: list[dict[str, Any]] = []
        if retracted:
            incident_rows = [
                dict(row)
                for row in session.query(
                    """
                    SELECT edge_id, src, dst, edge_type, source
                      FROM okg.graph_edges
                     WHERE (src = ANY(:node_ids) OR dst = ANY(:node_ids))
                       AND edge_type = ANY(:edge_types)
                    """,
                    {"node_ids": retracted, "edge_types": sorted(ranks)},
                )
            ]
        incident_declarations = load_declarations(
            session, [int(row["edge_id"]) for row in incident_rows],
        )
        # A ranked edge here is one with declarations, or a row a ranked
        # producer wrote before declarations were recorded. A row of an
        # unranked producer with no declaration stays on today's path.
        incident_rows = [
            row for row in incident_rows
            if int(row["edge_id"]) in incident_declarations
            or str(row.get("source") or "")
            in ranks.get(str(row["edge_type"]), {})
        ]
        purged = {
            int(row["edge_id"])
            for row in incident_rows
            if str(row["src"]) in removed or str(row["dst"]) in removed
        }
        withdrawn = set(int(edge_id) for edge_id in withdrawn_edge_ids)
        withdrawn |= {
            int(row["edge_id"])
            for row in incident_rows
            if int(row["edge_id"]) not in purged
            and str(row["edge_type"]) in ranked_types_for_source
        }
        touched = sorted(
            {int(row["edge_id"]) for row in upserts} | withdrawn | purged
        )
        step_started = time.perf_counter()
        current_edges = self._prior_edges_by_id(session, touched)
        _add_ms(timings_ms, f"{label}.read_current", step_started)
        step_started = time.perf_counter()
        declarations = load_declarations(session, touched)
        _add_ms(timings_ms, f"{label}.read_declarations", step_started)
        step_started = time.perf_counter()
        outcome = reconcile_edge_declarers(
            source=source,
            ranks=ranks,
            upserts=upserts,
            withdrawn_edge_ids=withdrawn,
            purged_edge_ids=purged,
            current_edges=current_edges,
            declarations=declarations,
        )
        _add_ms(timings_ms, f"{label}.reconcile", step_started)
        return _RankedEdgeWrite(
            outcome=outcome,
            current_edges=current_edges,
            incident_rows=incident_rows,
            purged_edges=len(purged),
        )

    def withdraw_edge_declarations(
        self,
        *,
        branch: str,
        sources: Iterable[str],
        run_id: str | None = None,
        session: Any | None = None,
    ) -> dict[str, int]:
        """Withdraw recorded edge declarations of ``sources`` (item 92a).

        For paths that remove a source's writes without retracting each edge
        through this source: source retirement (every declaration of the
        retired source) and compensation of one run with no run base
        (``run_id``: only the declarations that run recorded). Each affected
        edge's stored row then passes to the next-ranked declarer, or is
        deleted when none remains, in the same transaction. Runs after the
        branch write fence, nested in the caller's transaction when there is
        one. A database without the table has nothing to withdraw.
        """
        from okg.substrate.graph_store_internal.edge_declarers import (
            EDGE_DECLARERS_TABLE,
            load_declarations,
            withdraw_declarations,
        )

        wanted = sorted({str(source) for source in sources if source})
        counts = {
            "declarations_withdrawn": 0,
            "edges_rewritten": 0,
            "edges_deleted": 0,
        }
        if not wanted or not self._tracks_node_declarers():
            return counts
        backend = self.context._backend
        if EDGE_DECLARERS_TABLE not in backend.tables:
            backend.refresh_registries()
        if EDGE_DECLARERS_TABLE not in backend.tables:
            return counts
        if session is None:
            session = self.checkout(branch)
        sql = (
            f"SELECT edge_id, source FROM {EDGE_DECLARERS_TABLE} "
            "WHERE source = ANY(:sources)"
        )
        params: dict[str, Any] = {"sources": wanted}
        if run_id is not None:
            sql += " AND run_id = :run_id"
            params["run_id"] = str(run_id)
        with session.transaction():
            self._lock_edge_ranking_shared()
            self._lock_branch_write(branch)
            ranks = self._edge_precedence_ranks()
            keys = [
                {"edge_id": int(row["edge_id"]), "source": str(row["source"])}
                for row in session.query(sql, params)
            ]
            if not keys:
                return counts
            edge_ids = sorted({key["edge_id"] for key in keys})
            outcome = withdraw_declarations(
                sources=wanted,
                ranks=ranks,
                current_edges=self._prior_edges_by_id(session, edge_ids),
                declarations=load_declarations(session, edge_ids),
                withdrawn=[(key["edge_id"], key["source"]) for key in keys],
            )
            # PACT lane-h-nomos-read-class-stored: the next-ranked winner's
            # row carries the read class of its own content.
            stamp_rows(outcome.edge_upserts, "edge")
            apply_table_edits = getattr(session, "apply_table_edits", None)
            if callable(apply_table_edits):
                apply_table_edits(
                    EDGE_DECLARERS_TABLE,
                    deletes=outcome.declaration_deletes,
                    upserts=outcome.declaration_upserts,
                )
                if outcome.edge_upserts or outcome.edge_deletes:
                    apply_table_edits(
                        EDGE_TABLE,
                        deletes=outcome.edge_deletes,
                        upserts=outcome.edge_upserts,
                    )
            else:
                session.delete_keys(
                    EDGE_DECLARERS_TABLE, outcome.declaration_deletes,
                )
                if outcome.declaration_upserts:
                    session.upsert_rows(
                        EDGE_DECLARERS_TABLE, outcome.declaration_upserts,
                    )
                if outcome.edge_deletes:
                    session.delete_keys(EDGE_TABLE, outcome.edge_deletes)
                if outcome.edge_upserts:
                    session.upsert_rows(EDGE_TABLE, outcome.edge_upserts)
        counts["declarations_withdrawn"] = len(keys)
        counts["edges_rewritten"] = len(outcome.edge_upserts)
        counts["edges_deleted"] = len(outcome.edge_deletes)
        return counts

    def purge_declarations_outside_ranking(
        self, *, deployment_name: str | None = None,
    ) -> dict[str, int]:
        """Delete declarations the current ranking no longer covers.

        Run after ``okg.edge_precedence`` changes (``okg migrate`` and worker
        start), in the caller's transaction with the ranking change. A
        declaration of a producer the ranking dropped, or of an edge type it
        no longer ranks, would otherwise act later: an unranked stale
        declaration makes every write of its edge refuse, and one kept across
        an unrank/re-rank comes back as the winner although its producer
        retracted the edge meanwhile.

        Only branches that hold such rows are written: their rows are deleted
        under the branch write fence and the winner of each edge that still
        has a ranked declaration is stored again. An edge with no ranked
        declaration left keeps its stored row as an ordinary
        last-writer-wins row (a later ranked write replaces it; it is not
        adopted as a declaration). A frozen paired workspace accepts no
        ordinary write: one without stale rows is skipped with a warning, one
        with stale rows is refused, naming the branch.
        """
        from okg.substrate.graph_store_internal.edge_declarers import (
            EDGE_DECLARERS_TABLE,
            load_declarations,
        )

        counts = {
            "branches": 0,
            "declarations_purged": 0,
            "edges_rewritten": 0,
            "frozen_branches_skipped": 0,
        }
        if not self._tracks_node_declarers():
            return counts
        backend = self.context._backend
        if EDGE_DECLARERS_TABLE not in backend.tables:
            backend.refresh_registries()
        if EDGE_DECLARERS_TABLE not in backend.tables:
            return counts
        ranks = self._edge_precedence_ranks()
        ranked_keys = sorted(
            f"{edge_type}|{source}"
            for edge_type, sources in ranks.items()
            for source in sources
        )
        stale_sql = (
            f"SELECT edge_id, source FROM {EDGE_DECLARERS_TABLE} "
            "WHERE NOT ((edge_type || '|' || source) = ANY(:ranked))"
        )
        params = {"ranked": ranked_keys}
        frozen_skipped: list[str] = []
        for info in self.context.list_branches():
            branch = str(info.branch_id)
            session = self.checkout(branch)
            frozen = self._paired_workspace_frozen(branch)
            if not session.query(stale_sql + " LIMIT 1", params):
                if frozen is not None:
                    frozen_skipped.append(branch)
                continue
            if frozen is not None:
                deployment = deployment_name or "<deployment>"
                children = self._branch_descendants(branch)
                if children:
                    delete = (
                        f"delete the frozen branch and its child branches "
                        f"({', '.join(children)}) with `okg branch delete "
                        f"{branch} --deployment {deployment} --confirm "
                        "--cascade` (this deletes the branch and every child "
                        "branch under it)"
                    )
                else:
                    delete = (
                        "delete the frozen branch with `okg branch delete "
                        f"{branch} --deployment {deployment} --confirm`"
                    )
                raise SubstrateError(
                    f"edge ranking change refused: frozen paired workspace "
                    f"{branch!r} holds declarations the new ranking no longer "
                    "covers, and a frozen branch accepts no write",
                    data={
                        "branch": branch,
                        "lifecycle_status": frozen,
                        "deployment": deployment_name,
                        "child_branches": children,
                    },
                    fix_hint=(
                        "nothing was applied (the ranking is unchanged). "
                        "Either restore the previous edge_precedence in "
                        f"deployment.yaml, or {delete}, then rerun `okg "
                        f"migrate --deployment {deployment} --apply`"
                    ),
                )
            with session.transaction():
                self._lock_branch_write_yielding(
                    branch, deployment_name=deployment_name,
                )
                stale = [
                    {"edge_id": int(row["edge_id"]), "source": str(row["source"])}
                    for row in session.query(stale_sql, params)
                ]
                if not stale:
                    continue
                apply_table_edits = getattr(session, "apply_table_edits", None)
                if callable(apply_table_edits):
                    apply_table_edits(EDGE_DECLARERS_TABLE, deletes=stale, upserts=[])
                else:
                    session.delete_keys(EDGE_DECLARERS_TABLE, stale)
                still_declared = set(
                    load_declarations(session, [key["edge_id"] for key in stale])
                )
                settled = self._settle_ranked_edges(
                    session, still_declared, ranks=ranks,
                )
                counts["branches"] += 1
                counts["declarations_purged"] += len(stale)
                counts["edges_rewritten"] += settled["edges_rewritten"]
        if frozen_skipped:
            logging.getLogger(__name__).warning(
                "edge ranking change: skipped frozen paired-workspace branches "
                "with no stale declarations: %s",
                ", ".join(frozen_skipped),
            )
        counts["frozen_branches_skipped"] = len(frozen_skipped)
        return counts

    def _branch_descendants(self, branch: str) -> list[str]:
        """Child branches ``okg branch delete --cascade`` would also delete."""
        if getattr(self.context, "backend_name", None) != "interval":
            return []
        rows = self.context.db.execute(
            """
            WITH RECURSIVE subtree(branch_id) AS (
              SELECT branch_id
                FROM _chronos_branch_interval_branches
               WHERE branch_id = ?
              UNION ALL
              SELECT child.branch_id
                FROM _chronos_branch_interval_branches AS child
                JOIN subtree ON child.parent_branch_id = subtree.branch_id
               WHERE child.branch_id <> 'main'
            )
            SELECT branch_id FROM subtree ORDER BY branch_id
            """,
            (branch,),
        ).fetchall()
        return [
            str(_first_value(row))
            for row in rows
            if str(_first_value(row)) != branch
        ]

    def _paired_workspace_frozen(self, branch: str) -> str | None:
        """The lifecycle status of a frozen paired workspace, else ``None``."""
        try:
            self._assert_paired_workspace_writable(branch)
        except SubstrateError as exc:
            status = (exc.data or {}).get("lifecycle_status")
            if status in _PAIRED_WORKSPACE_TERMINAL_STATUSES:
                return str(status)
            raise
        return None

    def _settle_ranked_edges(
        self,
        session: Any,
        edge_ids: Iterable[int],
        *,
        ranks: Mapping[str, Mapping[str, int]] | None = None,
    ) -> dict[str, int]:
        """Store each edge's winner after its declarer rows changed in bulk.

        ``edge_ids`` are edges whose declarations a merge, a rollback or a
        ranking change just rewrote. The caller holds the branch write fence
        inside its transaction; the graph edits join that transaction.
        """
        from okg.substrate.graph_store_internal.edge_declarers import (
            load_declarations,
            settle_declared_edges,
        )

        ids = sorted({int(edge_id) for edge_id in edge_ids})
        if not ids:
            return {"edges_rewritten": 0, "edges_deleted": 0}
        outcome = settle_declared_edges(
            ranks=ranks if ranks is not None else self._edge_precedence_ranks(),
            edge_ids=ids,
            current_edges=self._prior_edges_by_id(session, ids),
            declarations=load_declarations(session, ids),
        )
        # PACT lane-h-nomos-read-class-stored: a settled winner's row carries
        # the read class of its own content.
        stamp_rows(outcome.edge_upserts, "edge")
        if outcome.edge_upserts or outcome.edge_deletes:
            apply_table_edits = getattr(session, "apply_table_edits", None)
            if callable(apply_table_edits):
                apply_table_edits(
                    EDGE_TABLE,
                    deletes=outcome.edge_deletes,
                    upserts=outcome.edge_upserts,
                )
            else:
                if outcome.edge_deletes:
                    session.delete_keys(EDGE_TABLE, outcome.edge_deletes)
                if outcome.edge_upserts:
                    session.upsert_rows(EDGE_TABLE, outcome.edge_upserts)
        return {
            "edges_rewritten": len(outcome.edge_upserts),
            "edges_deleted": len(outcome.edge_deletes),
        }

    def _purge_edge_declarations(self, session: Any, edge_ids: Iterable[int]) -> int:
        """Delete every declaration of edges whose stored row is being removed
        for a reason no producer controls (a dangling endpoint). Caller holds
        the branch write fence inside its transaction."""
        from okg.substrate.graph_store_internal.edge_declarers import (
            EDGE_DECLARERS_TABLE,
        )

        ids = sorted({int(edge_id) for edge_id in edge_ids})
        if not ids or not self._tracks_node_declarers():
            return 0
        backend = self.context._backend
        if EDGE_DECLARERS_TABLE not in backend.tables:
            backend.refresh_registries()
        if EDGE_DECLARERS_TABLE not in backend.tables:
            return 0
        keys = [
            {"edge_id": int(row["edge_id"]), "source": str(row["source"])}
            for row in session.query(
                f"SELECT edge_id, source FROM {EDGE_DECLARERS_TABLE} "
                "WHERE edge_id = ANY(:edge_ids)",
                {"edge_ids": ids},
            )
        ]
        if keys:
            apply_table_edits = getattr(session, "apply_table_edits", None)
            if callable(apply_table_edits):
                apply_table_edits(EDGE_DECLARERS_TABLE, deletes=keys, upserts=[])
            else:
                session.delete_keys(EDGE_DECLARERS_TABLE, keys)
        return len(keys)

    @staticmethod
    def _edge_upsert_semantic_noop(
        row: Mapping[str, Any],
        prior: Mapping[str, Any] | None,
    ) -> bool:
        """Whether an edge upsert would store nothing new; see
        :meth:`_node_upsert_semantic_noop` (issue #2335)."""
        if prior is None:
            return False
        return (
            str(row.get("src") or "") == str(prior.get("src") or "")
            and str(row.get("dst") or "") == str(prior.get("dst") or "")
            and str(row.get("edge_type") or "")
            == str(prior.get("edge_type") or "")
            and compare_attrs(
                dict(row.get("attrs") or {}), dict(prior.get("attrs") or {}),
            ).unchanged
            and row.get("source") == prior.get("source")
            and dict(row.get("source_record_id") or {})
            == dict(prior.get("source_record_id") or {})
            # confidence/provenance are dedicated edge columns (not in attrs);
            # a change to either is a real change, not a no-op -- it must be
            # written, not dropped (and marked dirty in
            # _edge_attr_changes_by_type). Confidence is compared at the
            # precision the REAL column stores.
            and not edge_columns_differing(
                new_provenance=row.get("provenance"),
                new_confidence=row.get("confidence"),
                stored_provenance=prior.get("provenance"),
                stored_confidence=prior.get("confidence"),
            )
        )

    @staticmethod
    def _scoped_delete_keys_from_prior(
        keys: list[dict[str, Any]],
        *,
        key_field: str,
        prior_by_id: Mapping[Any, Mapping[str, Any]],
        source: str | None,
    ) -> list[dict[str, Any]]:
        if source is None:
            return list(keys)
        out: list[dict[str, Any]] = []
        for key in keys:
            key_value = key[key_field]
            prior = prior_by_id.get(key_value)
            if prior is not None and prior.get("source") == source:
                out.append({key_field: key_value})
        return out

    @staticmethod
    def _delta_counts_from_prior_rows(
        *,
        node_upserts: list[dict[str, Any]],
        node_deletes: list[dict[str, Any]],
        edge_upserts: list[dict[str, Any]],
        edge_deletes: list[dict[str, Any]],
        prior_nodes_by_id: Mapping[str, Mapping[str, Any]],
        prior_edges_by_id: Mapping[int, Mapping[str, Any]],
        source: str | None,
    ) -> dict[str, int]:
        counts = {
            "nodes_added": 0,
            "nodes_retracted": 0,
            "nodes_updated": 0,
            "edges_added": 0,
            "edges_retracted": 0,
            "edges_updated": 0,
        }
        node_upsert_ids = {str(row["node_id"]) for row in node_upserts}
        edge_upsert_ids = {int(row["edge_id"]) for row in edge_upserts}
        for row in node_upserts:
            node_id = str(row["node_id"])
            prior = prior_nodes_by_id.get(node_id)
            if prior is None or (
                source is not None and prior.get("source") != source
            ):
                counts["nodes_added"] += 1
            elif not ChronosGraphWriteMixin._node_upsert_semantic_noop(row, prior):
                counts["nodes_updated"] += 1
        for row in node_deletes:
            node_id = str(row["node_id"])
            if node_id in node_upsert_ids:
                continue
            prior = prior_nodes_by_id.get(node_id)
            if prior is not None and (
                source is None or prior.get("source") == source
            ):
                counts["nodes_retracted"] += 1
        for row in edge_upserts:
            edge_key = int(row["edge_id"])
            prior = prior_edges_by_id.get(edge_key)
            if prior is None or (
                source is not None and prior.get("source") != source
            ):
                counts["edges_added"] += 1
            elif not ChronosGraphWriteMixin._edge_upsert_semantic_noop(row, prior):
                counts["edges_updated"] += 1
        for row in edge_deletes:
            edge_key = int(row["edge_id"])
            if edge_key in edge_upsert_ids:
                continue
            prior = prior_edges_by_id.get(edge_key)
            if prior is not None and (
                source is None or prior.get("source") == source
            ):
                counts["edges_retracted"] += 1
        return counts

    def searchable_text_for_comparison(
        self,
        subtype: str,
        attrs: Mapping[str, Any],
        *,
        cache: dict[str, list[str]],
    ) -> dict[str, Any]:
        """Return a copy of node ``attrs`` with ``text`` stamped as a write would.

        The source runner's persisted dedupe compares a candidate node with
        the stored row. The stored row's ``text`` went through
        ``_stamp_searchable_text`` on its write, so the candidate must go
        through the same rule before the two are compared; otherwise every
        node of a subtype with declared searchable fields reads as changed.
        Nodes only: the writer never stamps edge attrs.
        """
        return self._stamp_searchable_text(str(subtype), dict(attrs), cache=cache)

    def _unstamp_stored_attrs(
        self,
        subtype: str,
        attrs: dict[str, Any],
        *,
        cache: dict[str, list[str]],
    ) -> dict[str, Any]:
        # A declaration row does not record the searchable declaration it was
        # stamped under, and the catalog may have changed since, so the
        # current declaration cannot be used to recognise its rollup (review
        # round 3). With no declaration now, the stamper leaves attrs alone,
        # and so does this, as before #2991.
        fields, _path_fields = self._searchable_declaration(subtype, cache=cache)
        if not fields:
            return attrs
        return unstamp_without_declaration(attrs)

    def _searchable_declaration(
        self, subtype: str, *, cache: dict[str, list[str]],
    ) -> tuple[list[str], list[str]]:
        if subtype not in cache:
            cache[subtype] = self._searchable_text_fields(subtype)
        path_cache_key = f"\x00path\x00{subtype}"
        if path_cache_key not in cache:
            cache[path_cache_key] = self._searchable_text_fields(
                subtype, key="searchable_text_path_fields",
            )
        return cache[subtype], cache[path_cache_key]

    def _stamp_searchable_text(
        self,
        subtype: str,
        attrs: dict[str, Any],
        *,
        cache: dict[str, list[str]],
    ) -> dict[str, Any]:
        fields, path_fields = self._searchable_declaration(subtype, cache=cache)
        # The rule lives in `okg.substrate.searchable_text` and is mirrored by
        # `okg._stamp_searchable_text` in `db/schema.sql`. A producer's own
        # `text` body is appended after the declared parts and kept under
        # `PRODUCER_TEXT_ATTR` (issue #2991); with no declared part it is
        # stored byte for byte (issue #2972), because a document chunk's
        # `text` must stay equal to the source slice its offsets name.
        return stamp_searchable_text(attrs, fields, path_fields)

    def _searchable_text_fields(
        self, subtype: str, *, key: str = "searchable_text_fields",
    ) -> list[str]:
        if self.context.db.execute(
            "SELECT to_regclass('okg.subtypes') AS rel"
        ).fetchone()["rel"] is None:
            return []
        catalog_version_id = getattr(self, "_catalog_version_id", None)
        if catalog_version_id is None:
            row = self.context.db.execute(
                """
                SELECT s.metadata->:key AS fields
                  FROM okg.subtypes s
                  JOIN okg.catalog_versions cv
                    ON cv.catalog_version_id = s.catalog_version_id
                 WHERE s.name = :subtype
                   AND cv.status = 'applied'
                 ORDER BY s.catalog_version_id DESC
                 LIMIT 1
                """,
                {"subtype": subtype, "key": key},
            ).fetchone()
        else:
            row = self.context.db.execute(
                """
                SELECT metadata->:key AS fields
                  FROM okg.subtypes
                 WHERE name = :subtype
                   AND catalog_version_id = :catalog_version_id
                 LIMIT 1
                """,
                {
                    "subtype": subtype,
                    "key": key,
                    "catalog_version_id": catalog_version_id,
                },
            ).fetchone()
        if row is None:
            return []
        fields = row["fields"]
        if not isinstance(fields, list):
            return []
        return [str(field) for field in fields if isinstance(field, str)]

    def _missing_edge_endpoints(
        self,
        session: Any,
        *,
        edge_upserts: list[dict[str, Any]],
        node_upserts: list[dict[str, Any]],
        node_deletes: list[dict[str, Any]],
    ) -> set[str]:
        if not edge_upserts:
            return set()
        inserted = {row["node_id"] for row in node_upserts}
        deleted = {row["node_id"] for row in node_deletes}
        endpoints = {
            node_id
            for row in edge_upserts
            for node_id in (row["src"], row["dst"])
        }
        candidates = sorted(endpoints - inserted)
        existing: set[str] = set()
        if candidates:
            existing = {
                row["node_id"]
                for row in session.query(
                    """
                    SELECT node_id
                      FROM okg.graph_nodes
                     WHERE node_id = ANY(:node_ids)
                    """,
                    {"node_ids": candidates},
                )
            }
        available = (existing - deleted) | inserted
        return endpoints - available

    def _incident_edges_for_node_deletes(
        self,
        session: Any,
        node_keys: list[dict[str, Any]],
        *,
        source: str | None = None,
        exclude_edge_ids: set[int] | None = None,
        exclude_edge_types: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Edges incident to deleted nodes, for the source-scoped cascade.

        ``exclude_edge_types``: ranked edge types (item 92a), whose incident
        edges ``_apply_edge_declarers`` decides from their declarations.
        """
        node_ids = {key["node_id"] for key in node_keys}
        if not node_ids:
            return []
        source_clause = "AND source = :source" if source is not None else ""
        rows = session.query(
            f"""
            SELECT edge_id, src, dst, edge_type, attrs
            FROM okg.graph_edges
            WHERE (src = ANY(:node_ids) OR dst = ANY(:node_ids))
            {source_clause}
            """,
            {"node_ids": list(node_ids), "source": source},
        )
        excluded = exclude_edge_ids or set()
        excluded_types = exclude_edge_types or set()
        return [
            dict(row)
            for row in rows
            if int(row["edge_id"]) not in excluded
            and str(row["edge_type"]) not in excluded_types
        ]

    def _delete_incident_edges(
        self,
        session: Any,
        edge_keys: list[dict[str, Any]],
        *,
        source: str | None = None,
    ) -> tuple[int, dict[str, list[int]]]:
        if not edge_keys:
            return 0, {}
        edge_ids = [int(key["edge_id"]) for key in edge_keys]
        source_clause = "AND source = :source" if source is not None else ""
        rows = session.query(
            f"""
            SELECT edge_id, edge_type
            FROM okg.graph_edges
            WHERE edge_id = ANY(:edge_ids)
            {source_clause}
            """,
            {"edge_ids": edge_ids, "source": source},
        )
        edges_by_type: dict[str, list[int]] = {}
        for row in rows:
            edges_by_type.setdefault(str(row["edge_type"]), []).append(
                int(row["edge_id"]),
            )
        deleted = self._delete_keys_for_source(
            session,
            EDGE_TABLE,
            "edge_id",
            [{"edge_id": row["edge_id"]} for row in rows],
            source=source,
        )
        return deleted, {
            edge_type: list(dict.fromkeys(edge_ids))
            for edge_type, edge_ids in edges_by_type.items()
        }

    @staticmethod
    def _delete_keys_for_source(
        session: Any,
        table: str,
        key_field: str,
        keys: list[dict[str, Any]],
        *,
        source: str | None,
    ) -> int:
        if not keys:
            return 0

        def _apply_deletes(del_keys: list[dict[str, Any]]) -> None:
            # Route through the set-based batch splice when the session exposes
            # it (one JSONB-CTE statement) instead of per-key delete_keys, which
            # loops _splice_row -> SELECT+close+tombstone-INSERT per row. Same
            # rows, same transaction, same interval semantics.
            apply_table_edits = getattr(session, "apply_table_edits", None)
            if callable(apply_table_edits):
                apply_table_edits(table, deletes=del_keys, upserts=[])
            else:
                session.delete_keys(table, del_keys)

        if source is None:
            _apply_deletes(keys)
            return len(keys)
        key_values = list(dict.fromkeys(key[key_field] for key in keys))
        if not key_values:
            return 0
        rows = session.query(
            f"""
            SELECT {key_field}
              FROM {table}
             WHERE source = :source
               AND {key_field} = ANY(:keys)
            """,
            {"source": source, "keys": key_values},
        )
        scoped_keys = [{key_field: row[key_field]} for row in rows]
        if scoped_keys:
            _apply_deletes(scoped_keys)
        return len(scoped_keys)

    def retract_orphaned_derived_edges(self, *, branch: str = DEFAULT_BRANCH) -> int:
        """Tombstone lifecycle-owned edges left dangling by live-node changes.

        A derived edge (``provenance LIKE 'derived%'`` -- the ``derived`` /
        ``derived_deterministic`` / ``derived_semantic`` / ``derived_artifact``
        family) or any edge owned by an ``_enricher:*`` producer
        whose ``src`` or ``dst`` is no longer a live node is an ORPHAN: produced from
        a fact that a
        later re-ingest has since retracted, and it now dangles. okg's source-scoped
        deletion (:meth:`_delete_incident_edges`) only cleans up incident edges from
        the SAME ``source`` as the deleted node, so a CROSS-SOURCE derived edge -- a
        ``mentions`` edge stamped ``_publisher_derived`` pointing at a now-retracted
        ``document`` node, say -- survives the re-ingest and blocks the no-dangling
        gate. This retracts exactly those, as-of the current segment (a Chronos
        tombstone -- the edge stays live in the generations where its endpoints were
        live; querying an earlier generation still sees it), scoped to what this
        publish actually orphaned: unrelated live edges are untouched.

        Producer ownership is authoritative for lifecycle even when a legacy or
        custom enricher stamped semantic provenance as ``explicit`` or
        ``extracted_model``. An explicit edge from a genuine source remains a
        source assertion (a forward reference), governed by the deployment's
        no-dangling policy. Returns the count of edges retracted.
        """
        session = self.checkout(branch)
        with session.transaction():
            # Deletes declarations of the edges it retracts (item 92a).
            self._lock_edge_ranking_shared()
            self._lock_branch_write(branch)
            rows = session.query(
                """
                SELECT e.edge_id
                  FROM okg.graph_edges e
                 WHERE (
                       e.provenance LIKE 'derived%'
                    OR e.source LIKE '\\_enricher:%'
                 )
                   AND (
                     NOT EXISTS (
                       SELECT 1 FROM okg.graph_nodes n WHERE n.node_id = e.src
                     )
                     OR NOT EXISTS (
                       SELECT 1 FROM okg.graph_nodes n WHERE n.node_id = e.dst
                     )
                   )
                """,
                {},
            )
            if not rows:
                return 0
            keys = [{"edge_id": int(row["edge_id"])} for row in rows]
            session.delete_keys(EDGE_TABLE, keys)
            # A dangling ranked edge leaves with every declaration of it, or a
            # later write would hand the row to another declarer of the same
            # dangling edge (item 92a).
            self._purge_edge_declarations(
                session, [key["edge_id"] for key in keys],
            )
            return len(keys)

    def _lock_branch_write(self, branch: str) -> None:
        db = self.context.db
        if getattr(db, "dialect", None) != "postgres":
            return
        db.execute(
            "SELECT pg_advisory_xact_lock(hashtext(?), hashtext(?))",
            ["okg.chronos_graph", branch],
        )
        self._assert_paired_workspace_writable(branch)

    def _assert_paired_workspace_writable(self, branch: str) -> None:
        """Reject ordinary graph writes after a paired workspace is frozen.

        The advisory lock above is the common fence for all graph-fact writes.
        Read the branch row only after taking that fence so a lifecycle
        transition cannot race an ordinary writer.  The paired-workspace
        lifecycle is intentionally enforced only by the interval backend,
        which is the backend that stores the branch metadata used by the
        coordination runtime; other Chronos backends retain their existing
        write behavior.
        """
        if getattr(self.context, "backend_name", None) != "interval":
            return
        db = self.context.db
        try:
            row = db.execute(
                """
                SELECT metadata
                  FROM _chronos_branch_interval_branches
                 WHERE branch_id = ?
                """,
                (branch,),
            ).fetchone()
        except Exception as exc:  # noqa: BLE001 - metadata is a write fence
            raise SubstrateError(
                "cannot verify paired-workspace graph write state",
                data={"branch": branch},
                fix_hint=(
                    "repair the Chronos branch metadata before writing the graph"
                ),
            ) from exc
        if row is None:
            raise SubstrateError(
                "cannot verify paired-workspace graph write state: branch is missing",
                data={"branch": branch},
                fix_hint="reconcile the coordination branch before writing the graph",
            )
        try:
            raw_metadata = (
                row.get("metadata")
                if isinstance(row, Mapping)
                else row["metadata"]
            )
        except (KeyError, IndexError, TypeError) as exc:
            raise SubstrateError(
                "cannot verify paired-workspace graph write state: metadata is missing",
                data={"branch": branch},
                fix_hint=(
                    "repair the Chronos branch metadata before writing the graph"
                ),
            ) from exc
        try:
            if raw_metadata is None or raw_metadata == "":
                metadata: Mapping[str, Any] = {}
            elif isinstance(raw_metadata, Mapping):
                metadata = raw_metadata
            elif isinstance(raw_metadata, (str, bytes, bytearray)):
                decoded = json.loads(raw_metadata)
                if not isinstance(decoded, Mapping):
                    raise TypeError("metadata JSON must be an object")
                metadata = decoded
            else:
                raise TypeError("metadata must be a JSON object or mapping")
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise SubstrateError(
                "cannot verify paired-workspace graph write state: metadata is malformed",
                data={"branch": branch},
                fix_hint=(
                    "repair the paired-workspace lifecycle metadata before writing"
                ),
            ) from exc

        if metadata.get("kind") != _PAIRED_WORKSPACE_KIND:
            return
        status = metadata.get("lifecycle_status")
        if not isinstance(status, str) or status not in _PAIRED_WORKSPACE_LIFECYCLE_STATUSES:
            raise SubstrateError(
                "cannot verify paired-workspace graph write state: lifecycle metadata is malformed",
                data={
                    "branch": branch,
                    "kind": _PAIRED_WORKSPACE_KIND,
                    "lifecycle_status": status,
                },
                fix_hint=(
                    "repair the paired-workspace lifecycle status before writing"
                ),
            )
        if status in _PAIRED_WORKSPACE_TERMINAL_STATUSES:
            raise SubstrateError(
                "ordinary graph write refused for a frozen paired workspace",
                data={
                    "branch": branch,
                    "kind": _PAIRED_WORKSPACE_KIND,
                    "lifecycle_status": status,
                },
                fix_hint=(
                    "write through the current active assignment or create a new pair"
                ),
            )

    def _lock_branch_write_session(self, branch: str) -> bool:
        db = self.context.db
        if getattr(db, "dialect", None) != "postgres":
            return False
        db.execute(
            "SELECT pg_advisory_lock(hashtext(?), hashtext(?))",
            ["okg.chronos_graph", branch],
        )
        try:
            self._assert_paired_workspace_writable(branch)
        except Exception:
            # A session-scoped lock survives transaction rollback.  Do not
            # strand it when the lifecycle guard refuses the write.
            try:
                db.execute(
                    "SELECT pg_advisory_unlock(hashtext(?), hashtext(?))",
                    ["okg.chronos_graph", branch],
                )
                self.context._commit_autocommit()
            except Exception:
                self.context._rollback_autocommit()
            raise
        return True

    def _unlock_branch_write_session(self, branch: str) -> None:
        db = self.context.db
        if getattr(db, "dialect", None) != "postgres":
            return
        try:
            db.execute(
                "SELECT pg_advisory_unlock(hashtext(?), hashtext(?))",
                ["okg.chronos_graph", branch],
            )
            self.context._commit_autocommit()
        except Exception:
            self.context._rollback_autocommit()
            raise
