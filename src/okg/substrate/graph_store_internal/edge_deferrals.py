"""Edges that wait for another source's record (cross-source endpoint wedge).

Source B writes ``note references widget`` onto a ``widget`` that source A
owns. When the widget is not on the branch, or is only a ``not_loaded``
placeholder, or has a subtype the clause does not accept, the edge used to
cost B its whole run (admission) or every publish of the shared branch
(the gate). The edge now waits in ``okg.graph_edge_deferrals`` until the
record exists and every check passes; then the original row is promoted.

The table is a Chronos interval table registered like
``okg.graph_edge_declarers``: its rows branch, merge, checkpoint and roll
back with the graph rows they describe. No read operator serves it, and the
MCP read role has no grant on its physical tables (``schema.py`` excludes
them from the ``_chronos*`` pattern grant and revokes an earlier one).

This module holds the table definition, the policy, and the publish step
(pact/changes/cross-source-endpoint-wedge/design.md section 3.3):

0. a waiting row whose identity is already in ``okg.graph_edges`` is deleted
   (repairs a merge or rollback that left an edge in both tables);
1. a dangling or producer-mismatched edge whose clause is deferrable moves
   to the waiting table;
2. a waiting row whose endpoints both exist as live, non-placeholder nodes,
   which passes the producer clause with resolved subtypes and the catalog
   narrowing and cardinality checks, moves back into ``okg.graph_edges``
   byte-identical to the producer's write. A row that fails stays waiting
   in a held state and never reaches the gate.

Every move is keyed by edge identity. The step commits separately from the
checkpoint, so a crash between them is completed by the retried publish.
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Iterator, Mapping, Sequence

from okg.substrate.catalog.linker_config import (
    LINK_ENDPOINT_SOURCE,
    LOAD_STATE_ATTR,
    NOT_LOADED,
)

log = logging.getLogger(__name__)

EDGE_DEFERRALS_TABLE = "okg.graph_edge_deferrals"
EDGE_DEFERRALS_PK = ["edge_id"]

STATE_WAITING = "waiting"
STATE_SUBTYPE_MISMATCH = "subtype_mismatch"
STATE_NARROWING = "narrowing_violation"
STATE_CARDINALITY = "cardinality_violation"
DEFERRAL_STATES = (
    STATE_WAITING,
    STATE_SUBTYPE_MISMATCH,
    STATE_NARROWING,
    STATE_CARDINALITY,
)

#: The graph edge row's columns, copied verbatim in both directions.
EDGE_ROW_COLUMNS = (
    "edge_id",
    "src",
    "dst",
    "edge_type",
    "attrs",
    "provenance",
    "confidence",
    "source",
    "source_record_id",
    "source_revision",
    "run_id",
    "observed_at",
    "nomos_read_class",
    "nomos_read_class_version",
)

_WAITING_COLUMNS = (
    "state",
    "waiting_on",
    "waiting_role",
    "expected_subtype",
    "deferred_at",
    "deferred_generation",
    "catalog_version_id",
    "profile_fingerprint",
)

CREATE_EDGE_DEFERRALS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS okg.graph_edge_deferrals (
  edge_id                   BIGINT      PRIMARY KEY,
  src                       TEXT        NOT NULL,
  dst                       TEXT        NOT NULL,
  edge_type                 TEXT        NOT NULL,
  attrs                     JSONB       NOT NULL DEFAULT '{}'::jsonb,
  provenance                TEXT        NOT NULL DEFAULT 'explicit',
  confidence                REAL        NOT NULL DEFAULT 1.0,
  source                    TEXT,
  source_record_id          JSONB       NOT NULL DEFAULT '{}'::jsonb,
  source_revision           JSONB       NOT NULL DEFAULT '{}'::jsonb,
  run_id                    TEXT,
  observed_at               TIMESTAMPTZ,
  nomos_read_class          UUID,
  nomos_read_class_version  SMALLINT,
  state                     TEXT        NOT NULL DEFAULT 'waiting',
  waiting_on                TEXT        NOT NULL,
  waiting_role              TEXT        NOT NULL,
  expected_subtype          TEXT,
  deferred_at               TIMESTAMPTZ NOT NULL,
  deferred_generation       TEXT,
  catalog_version_id        BIGINT,
  profile_fingerprint       TEXT
)
"""

#: ``consistency.max_deferred_edges_per_run`` default (decision D2).
DEFAULT_MAX_DEFERRED_PER_RUN = 1000
#: ``consistency.max_deferred_edge_fraction`` default (decision D2).
DEFAULT_MAX_DEFERRED_FRACTION = 0.20
#: ``consistency.deferred_edge_warn_after`` default, seconds (decision D2: 24 h).
DEFAULT_WARN_AFTER_SECONDS = 24 * 3600
#: ``consistency.deferred_cap_mode`` default; change it here, in one line.
DEFAULT_CAP_MODE = "owner_published"
#: ``consistency.max_deferred_edges_unpublished_owner`` default.
DEFAULT_UNPUBLISHED_OWNER_CEILING = 100_000
CAP_MODES = ("owner_published", "absolute", "fraction")


@dataclass(frozen=True)
class DeferralPolicy:
    """Whether and how far a deployment lets cross-source edges wait.

    ``enabled`` is decided from the manifest by
    ``publisher.deployment_stack.load_deferral_policy``: on by default for
    ``dangling_edges: block`` deployments, off for ``prune`` deployments until
    the ``prune-baseline`` task measures them (decision D1).
    """

    enabled: bool = False
    max_per_run: int = DEFAULT_MAX_DEFERRED_PER_RUN
    max_fraction: float = DEFAULT_MAX_DEFERRED_FRACTION
    warn_after_seconds: int = DEFAULT_WARN_AFTER_SECONDS
    #: How a run's deferrals are capped (decision D2, board #196):
    #: ``owner_published`` counts only deferrals whose endpoint's owning
    #: source has published at least once and limits them to ``max_per_run``;
    #: ``absolute`` counts all against ``max_per_run``; ``fraction`` is the
    #: smaller of ``max_per_run`` and ``max_fraction`` of the run's edges.
    cap_mode: str = DEFAULT_CAP_MODE
    #: ``owner_published`` mode: deferrals whose owner has never published are
    #: not counted against ``max_per_run`` (that is the ordinary run-order
    #: case) but one run may still not park more than this many of them.
    unpublished_owner_ceiling: int = DEFAULT_UNPUBLISHED_OWNER_CEILING

    def limit_for(self, edge_count: int) -> int:
        """The most edges one run may defer: the smaller of the two caps."""
        if self.cap_mode == "fraction":
            return min(int(self.max_per_run), int(self.max_fraction * edge_count))
        return int(self.max_per_run)


SAMPLE_LIMIT = 20


def waiting_table_registered(session: Any) -> bool:
    """Whether a graph session's database has the waiting table registered."""
    backend = getattr(getattr(session, "_context", None), "_backend", None)
    return EDGE_DEFERRALS_TABLE in getattr(backend, "tables", {})


def edges_relation_sql(
    session: Any, columns: str = "edge_id, src, edge_type, dst, source",
) -> str:
    """The relation a producer's edges are read from for record-set coverage.

    A producer's waiting edges are its records: coverage reads
    ``okg.graph_edges`` plus the waiting table, so an edge that waits is
    neither missing nor stale. Without the waiting table it is the graph
    table alone.
    """
    if not waiting_table_registered(session):
        return "okg.graph_edges"
    return (
        f"(SELECT {columns} FROM okg.graph_edges "
        f"UNION ALL SELECT {columns} FROM {EDGE_DEFERRALS_TABLE})"
    )


def placeholder_predicate(alias: str) -> str:
    """SQL true for a #3666 ``not_loaded`` link-endpoint placeholder row.

    The one shared rule: a placeholder never counts as an existing endpoint,
    in admission and in promotion alike. The literal is a module constant,
    not caller input.
    """
    return (
        f"({alias}.source = '{LINK_ENDPOINT_SOURCE}' "
        f"AND {alias}.attrs ->> '{LOAD_STATE_ATTR}' = '{NOT_LOADED}')"
    )


def resolved_subtype_sql(alias: str) -> str:
    """The endpoint's subtype, NULL when it is absent or only a placeholder."""
    return (
        f"CASE WHEN {alias}.node_id IS NULL "
        f"OR {placeholder_predicate(alias)} THEN NULL "
        f"ELSE {alias}.subtype END"
    )


def waiting_row(
    edge: Mapping[str, Any],
    *,
    state: str,
    waiting_role: str,
    waiting_on: str,
    expected_subtype: str | None,
    deferred_at: datetime,
    deferred_generation: str | None,
    catalog_version_id: int | None,
    profile_fingerprint: str | None,
) -> dict[str, Any]:
    """The edge exactly as its producer wrote it, plus the waiting columns."""
    row = {column: edge.get(column) for column in EDGE_ROW_COLUMNS}
    row.update({
        "state": state,
        "waiting_on": waiting_on,
        "waiting_role": waiting_role,
        "expected_subtype": expected_subtype,
        "deferred_at": deferred_at,
        "deferred_generation": deferred_generation,
        "catalog_version_id": catalog_version_id,
        "profile_fingerprint": profile_fingerprint,
    })
    return row


def edge_row(waiting: Mapping[str, Any]) -> dict[str, Any]:
    """A waiting row's graph edge columns: the producer's write, unchanged."""
    return {column: waiting.get(column) for column in EDGE_ROW_COLUMNS}


def _select_columns(alias: str, columns: Iterable[str]) -> str:
    return ", ".join(f"{alias}.{column}" for column in columns)


EDGE_SELECT = _select_columns("e", EDGE_ROW_COLUMNS)
WAITING_SELECT = _select_columns(
    "d", (*EDGE_ROW_COLUMNS, *_WAITING_COLUMNS),
)


def _ids(rows: Iterable[Mapping[str, Any]]) -> list[int]:
    return [int(row["edge_id"]) for row in rows]


def _chunks(values: Sequence[Any], size: int = 1000) -> Iterable[Sequence[Any]]:
    for start in range(0, len(values), size):
        yield values[start:start + size]


class _TrialFailed(Exception):
    """A promotion trial found failing rows; the trial write is rolled back."""

    def __init__(
        self, narrowing: set[int], cardinality: set[int], other: set[int],
    ) -> None:
        super().__init__("promotion trial failed")
        self.narrowing = narrowing
        self.cardinality = cardinality
        self.other = other


@contextmanager
def _write_scope(store: Any, session: Any, branch: str) -> Iterator[None]:
    """One explicit transaction under the branch write fence.

    Batch upserts load an ON COMMIT DROP temp table across several
    statements, so autocommit between them would drop it; the graph store's
    own prune takes the same two locks in the same order.
    """
    with session.transaction():
        store._lock_edge_ranking_shared()
        store._lock_branch_write(branch)
        yield


@dataclass
class StepResult:
    """What one publish step did; recorded in the gate's details."""

    deduped: int = 0
    deferred: int = 0
    promoted: int = 0
    held: dict[str, int] | None = None
    skipped: str | None = None
    waiting: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "schema": "okg.edge_deferral_step.v1",
            "deduped": self.deduped,
            "deferred": self.deferred,
            "promoted": self.promoted,
            "held": dict(self.held or {}),
        }
        if self.waiting is not None:
            out.update(self.waiting)
        if self.skipped:
            out["skipped"] = self.skipped
        return out


def waiting_summary(session: Any) -> dict[str, Any]:
    """Counts by state and the oldest ``deferred_at`` of the waiting table."""
    rows = session.query(
        f"SELECT state, count(*) AS n, min(deferred_at) AS oldest "
        f"FROM {EDGE_DEFERRALS_TABLE} GROUP BY state",
    )
    by_state = {str(row["state"]): int(row["n"]) for row in rows}
    oldest = [row["oldest"] for row in rows if row["oldest"] is not None]
    return {
        "waiting_by_state": by_state,
        "waiting_total": sum(by_state.values()),
        "oldest_deferred_at": (
            min(oldest).isoformat() if oldest else None
        ),
    }


def run_edge_deferral_step(
    conn: Any,
    *,
    policy: DeferralPolicy,
    deployment_name: str,
    branch_name: str,
    catalog_version_id: int,
    target_generation_id: Any,
    candidate_edge_ids: Sequence[int] | None,
    dirty_node_ids: Sequence[str] | None,
    check_scoped_edges: Callable[[list[int]], tuple[list[int], list[int]]],
    check_cardinality: Callable[[list[int], list[str]], list[int]],
) -> StepResult:
    """Steps 0-2 of design.md 3.3 on ``branch_name``'s working segment.

    ``candidate_edge_ids`` and ``dirty_node_ids`` are None on the full gate
    path (every edge and every waiting row is in scope) and the incremental
    path's candidate edges and dirty nodes otherwise. ``check_scoped_edges``
    returns the gate's ``(dangling, narrowing)`` verdict for edge ids;
    ``check_cardinality`` its at-most-one-target verdict.
    """
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.producer_authority import (
        ProducerPolicyViolation,
        classify_edge_deferral,
        edge_rules_accept,
        load_profile_for_producer,
        profile_deferral_rules,
    )

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    backend = getattr(store.context, "_backend", None)
    if backend is None or EDGE_DEFERRALS_TABLE not in getattr(
        backend, "tables", {},
    ):
        # A database that has not run `okg migrate` since this table was
        # added has nothing waiting and nothing to defer.
        return StepResult(skipped="table_not_registered")

    result = StepResult(held={})
    session = store.checkout(branch=branch_name)
    deferred_at = datetime.now(timezone.utc)
    generation_ref = str(target_generation_id)

    # -- step 0: dedupe ----------------------------------------------------
    # The full path checks the whole (small) waiting table against the graph.
    # The incremental path checks only the rows this publish touched: its
    # candidate edges and the waiting rows on its dirty nodes; a duplicate a
    # merge or rollback left elsewhere is removed by the next full path or by
    # the publish that touches the edge. The table is indexed on both ends.
    if candidate_edge_ids is None:
        both = session.query(
            f"SELECT d.edge_id FROM {EDGE_DEFERRALS_TABLE} d "
            "JOIN okg.graph_edges e ON e.edge_id = d.edge_id",
        )
    else:
        both = []
        for chunk in _chunks(list(candidate_edge_ids)):
            both.extend(session.query(
                f"SELECT d.edge_id FROM {EDGE_DEFERRALS_TABLE} d "
                "JOIN okg.graph_edges e ON e.edge_id = d.edge_id "
                "WHERE d.edge_id = ANY(:ids)",
                {"ids": list(chunk)},
            ))
        for chunk in _chunks(list(dirty_node_ids or ())):
            both.extend(session.query(
                f"SELECT d.edge_id FROM {EDGE_DEFERRALS_TABLE} d "
                "JOIN okg.graph_edges e ON e.edge_id = d.edge_id "
                "WHERE d.src = ANY(:ids) OR d.dst = ANY(:ids)",
                {"ids": list(chunk)},
            ))
        both = list({int(row["edge_id"]): row for row in both}.values())
    if both:
        with _write_scope(store, session, branch_name):
            for chunk in _chunks(_ids(both)):
                session.delete_keys(
                    EDGE_DEFERRALS_TABLE, [{"edge_id": i} for i in chunk],
                )
        result.deduped = len(both)

    profiles: dict[str, Any] = {}

    def profile_for(source: str | None):
        """The source's active producer profile and deferral rules, or None."""
        if not source:
            return None
        if source not in profiles:
            try:
                profile, _ = load_profile_for_producer(
                    conn,
                    deployment_name=deployment_name,
                    producer_id=source,
                )
            except ProducerPolicyViolation:
                profiles[source] = None
            else:
                profiles[source] = profile_deferral_rules(conn, profile)
        return profiles[source]

    # -- step 1: defer -----------------------------------------------------
    if policy.enabled:
        result.deferred = _defer(
            conn, store, session,
            deployment_name=deployment_name,
            branch_name=branch_name,
            candidate_edge_ids=candidate_edge_ids,
            profile_for=profile_for,
            classify=classify_edge_deferral,
            accepts=edge_rules_accept,
            deferred_at=deferred_at,
            generation_ref=generation_ref,
            catalog_version_id=catalog_version_id,
        )

    # -- step 2: promote / recheck ----------------------------------------
    promoted, held = _promote(
        store, session,
        branch_name=branch_name,
        dirty_node_ids=dirty_node_ids,
        profile_for=profile_for,
        classify=classify_edge_deferral,
        accepts=edge_rules_accept,
        catalog_version_id=catalog_version_id,
        check_scoped_edges=check_scoped_edges,
        check_cardinality=check_cardinality,
    )
    result.promoted = promoted
    result.held = held
    # Generation metadata: the waiting set this publish leaves, by state, and
    # the age of its oldest row (the gate records the dict in its details).
    result.waiting = waiting_summary(store.checkout(branch=branch_name))
    return result


def _defer(
    conn: Any,
    store: Any,
    session: Any,
    *,
    deployment_name: str,
    branch_name: str,
    candidate_edge_ids: Sequence[int] | None,
    profile_for: Callable[[str | None], Any],
    classify: Callable[..., Any],
    accepts: Callable[..., bool],
    deferred_at: datetime,
    generation_ref: str,
    catalog_version_id: int,
) -> int:
    """Step 1: move dangling deferrable edges to the table.

    Also an edge of a run admitted but not yet published whose endpoint exists
    with a subtype its clause does not accept (admitted as ``subtype_mismatch``).
    """
    if candidate_edge_ids is not None and not candidate_edge_ids:
        return 0
    with conn.cursor() as cur:
        cur.execute(
            "SELECT run_id::text FROM okg.admission_envelopes "
            "WHERE deployment_name = %s AND status = 'accepted' "
            "AND run_id IS NOT NULL",
            (deployment_name,),
        )
        accepted_runs = [str(row[0]) for row in cur.fetchall()]
    scope = "" if candidate_edge_ids is None else (
        " AND e.edge_id = ANY(:ids)"
    )
    # Edges the #3666 extractor writes onto placeholders are served by design;
    # every other source's edge is judged.
    shape_sql = f"""
        SELECT e.source AS source, e.edge_type AS edge_type,
               {resolved_subtype_sql("ns")} AS src_subtype,
               {resolved_subtype_sql("nd")} AS dst_subtype
          FROM okg.graph_edges e
          LEFT JOIN okg.graph_nodes ns ON ns.node_id = e.src
          LEFT JOIN okg.graph_nodes nd ON nd.node_id = e.dst
         WHERE e.source IS NOT NULL
           AND NOT STARTS_WITH(e.source, '_extractor:'){scope}
         GROUP BY 1, 2, 3, 4
    """
    shapes: list[Mapping[str, Any]] = []
    if candidate_edge_ids is None:
        shapes = list(session.query(shape_sql))
    else:
        seen: set[tuple[Any, ...]] = set()
        for chunk in _chunks(list(candidate_edge_ids)):
            for row in session.query(shape_sql, {"ids": list(chunk)}):
                key = (
                    row["source"], row["edge_type"],
                    row["src_subtype"], row["dst_subtype"],
                )
                if key not in seen:
                    seen.add(key)
                    shapes.append(row)

    moved: list[dict[str, Any]] = []
    for shape in shapes:
        rules = profile_for(shape["source"])
        if rules is None or not rules.deferrable:
            continue
        src_sub, dst_sub = shape["src_subtype"], shape["dst_subtype"]
        if src_sub is not None and dst_sub is not None and rules.shape_accepts(
            str(shape["edge_type"]), str(src_sub), str(dst_sub),
        ):
            continue
        # A shape the producer's clauses do not accept as it stands.
        conditions = ["e.source = :source", "e.edge_type = :edge_type"]
        params: dict[str, Any] = {
            "source": shape["source"], "edge_type": shape["edge_type"],
        }
        for column, alias, value in (
            ("src", "ns", src_sub), ("dst", "nd", dst_sub),
        ):
            if value is None:
                conditions.append(
                    f"({alias}.node_id IS NULL "
                    f"OR {placeholder_predicate(alias)})"
                )
            else:
                conditions.append(
                    f"({alias}.node_id IS NOT NULL "
                    f"AND NOT {placeholder_predicate(alias)} "
                    f"AND {alias}.subtype = :{column}_subtype)"
                )
                params[f"{column}_subtype"] = value
        if src_sub is not None and dst_sub is not None:
            # Both endpoints exist: the edge is not dangling. Only an edge a
            # run admitted but no generation has published yet is moved (it
            # was accepted as a held subtype mismatch); a published edge the
            # producer's clause no longer accepts is the gate's to judge.
            if not accepted_runs:
                continue
            conditions.append("e.run_id = ANY(:accepted_runs)")
            params["accepted_runs"] = accepted_runs
        if candidate_edge_ids is not None:
            conditions.append("e.edge_id = ANY(:ids)")
            params["ids"] = list(candidate_edge_ids)
        after = -1
        while True:
            rows = session.query(
                f"""
                SELECT {EDGE_SELECT}, ns.node_id AS src_node, nd.node_id AS dst_node
                  FROM okg.graph_edges e
                  LEFT JOIN okg.graph_nodes ns ON ns.node_id = e.src
                  LEFT JOIN okg.graph_nodes nd ON nd.node_id = e.dst
                 WHERE {" AND ".join(conditions)} AND e.edge_id > :after
                 ORDER BY e.edge_id
                 LIMIT 1000
                """,
                {**params, "after": after},
            )
            if not rows:
                break
            after = int(rows[-1]["edge_id"])
            for row in rows:
                verdict = classify(
                    {
                        "src": row["src"], "dst": row["dst"],
                        "edge_type": row["edge_type"],
                        "src_subtype": src_sub, "dst_subtype": dst_sub,
                        "attrs": row.get("attrs") or {},
                    },
                    rules,
                )
                if verdict is None:
                    continue
                moved.append(waiting_row(
                    row,
                    state=verdict["state"],
                    waiting_role=verdict["waiting_role"],
                    waiting_on=verdict["waiting_on"],
                    expected_subtype=verdict.get("expected_subtype"),
                    deferred_at=deferred_at,
                    deferred_generation=generation_ref,
                    catalog_version_id=catalog_version_id,
                    profile_fingerprint=rules.profile_fingerprint,
                ))
    if not moved:
        return 0
    with _write_scope(store, session, branch_name):
        for chunk in _chunks(moved):
            session.upsert_rows(EDGE_DEFERRALS_TABLE, list(chunk))
    store.prune_edges([row["edge_id"] for row in moved], branch=branch_name)
    log.info(
        "edge deferral step: moved %d edge(s) to the waiting table (branch=%s)",
        len(moved), branch_name,
    )
    return len(moved)


def _promote(
    store: Any,
    session: Any,
    *,
    branch_name: str,
    dirty_node_ids: Sequence[str] | None,
    profile_for: Callable[[str | None], Any],
    classify: Callable[..., Any],
    accepts: Callable[..., bool],
    catalog_version_id: int,
    check_scoped_edges: Callable[[list[int]], tuple[list[int], list[int]]],
    check_cardinality: Callable[[list[int], list[str]], list[int]],
) -> tuple[int, dict[str, int]]:
    """Step 2: promote what passes, hold what fails, re-check held rows."""
    base_sql = f"""
        SELECT {WAITING_SELECT},
               {resolved_subtype_sql("ns")} AS src_subtype,
               {resolved_subtype_sql("nd")} AS dst_subtype
          FROM {EDGE_DEFERRALS_TABLE} d
          JOIN okg.graph_nodes ns ON ns.node_id = d.src
          JOIN okg.graph_nodes nd ON nd.node_id = d.dst
         WHERE NOT {placeholder_predicate("ns")}
           AND NOT {placeholder_predicate("nd")}
    """
    if dirty_node_ids is None:
        candidates = list(session.query(base_sql))
    else:
        candidates = []
        seen_ids: set[int] = set()
        for chunk in _chunks(list(dirty_node_ids)):
            for row in session.query(
                base_sql + " AND (d.src = ANY(:ids) OR d.dst = ANY(:ids))",
                {"ids": list(chunk)},
            ):
                if int(row["edge_id"]) not in seen_ids:
                    seen_ids.add(int(row["edge_id"]))
                    candidates.append(row)
        # A producer-profile change alone does not force the full gate path:
        # also re-check the held rows whose recorded fingerprint differs from
        # the producer's active one (one query by source on the small table).
        for row in session.query(
            base_sql + " AND d.state <> 'waiting'",
        ):
            rules = profile_for(row["source"])
            if (
                rules is not None
                and rules.profile_fingerprint != row["profile_fingerprint"]
                and int(row["edge_id"]) not in seen_ids
            ):
                seen_ids.add(int(row["edge_id"]))
                candidates.append(row)
    if not candidates:
        return 0, {}

    held: dict[str, int] = {}
    trial: list[dict[str, Any]] = []
    new_state: dict[int, str] = {}
    by_id: dict[int, Mapping[str, Any]] = {}
    for row in candidates:
        edge_id = int(row["edge_id"])
        by_id[edge_id] = row
        rules = profile_for(row["source"])
        record = {
            "src": row["src"], "dst": row["dst"],
            "edge_type": row["edge_type"],
            "src_subtype": row["src_subtype"],
            "dst_subtype": row["dst_subtype"],
            "attrs": row.get("attrs") or {},
        }
        if rules is None or not accepts(record, rules):
            new_state[edge_id] = STATE_SUBTYPE_MISMATCH
        else:
            trial.append(edge_row(row))

    promoted_ids: list[int] = []
    # The trial is one transaction: the rows are written, the gate's own
    # queries judge them, and a failure rolls the whole write back, so a
    # crash cannot leave a trial row in the graph with its waiting row
    # already gone. A failure holds only the trial edges that failed (never
    # a published edge a violating group also contains) and the rest are
    # tried again until nothing fails.
    remaining = list(trial)
    while remaining:
        remaining_ids = set(_ids(remaining))
        try:
            with _write_scope(store, session, branch_name):
                for chunk in _chunks(remaining):
                    session.upsert_rows("okg.graph_edges", list(chunk))
                dangling, narrowing = check_scoped_edges(sorted(remaining_ids))
                cardinality = check_cardinality(
                    sorted(remaining_ids),
                    sorted({
                        str(node) for edge in remaining
                        for node in (edge["src"], edge["dst"])
                    }),
                )
                failed_narrowing = set(narrowing) & remaining_ids
                failed_cardinality = set(cardinality) & remaining_ids
                failed_other = set(dangling) & remaining_ids
                if failed_narrowing or failed_cardinality or failed_other:
                    raise _TrialFailed(
                        failed_narrowing, failed_cardinality, failed_other,
                    )
            promoted_ids.extend(sorted(remaining_ids))
            break
        except _TrialFailed as failure:
            for edge_id in failure.narrowing:
                new_state[edge_id] = STATE_NARROWING
            for edge_id in failure.cardinality - failure.narrowing:
                new_state[edge_id] = STATE_CARDINALITY
            for edge_id in failure.other - failure.narrowing - failure.cardinality:
                new_state[edge_id] = STATE_SUBTYPE_MISMATCH
            failed_ids = failure.narrowing | failure.cardinality | failure.other
            # The rollback removed the trial rows. If the checks committed
            # the connection's transaction on the way, they are still here:
            # remove exactly the failed trial edges that are visible (never a
            # published edge: step 0 already dropped any waiting row whose
            # edge is live). A delete of a key that is not visible would write
            # a deletion marker, so nothing is pruned when the rollback held.
            still_visible = [
                int(row["edge_id"])
                for row in store.checkout(branch=branch_name).query(
                    "SELECT edge_id FROM okg.graph_edges "
                    "WHERE edge_id = ANY(:ids)",
                    {"ids": sorted(failed_ids)},
                )
            ]
            if still_visible:
                store.prune_edges(sorted(still_visible), branch=branch_name)
            remaining = [
                edge for edge in remaining
                if int(edge["edge_id"]) not in failed_ids
            ]

    with _write_scope(store, session, branch_name):
        for chunk in _chunks(promoted_ids):
            session.delete_keys(
                EDGE_DEFERRALS_TABLE, [{"edge_id": i} for i in chunk],
            )
    updates: list[dict[str, Any]] = []
    for edge_id, state in new_state.items():
        row = by_id[edge_id]
        rules = profile_for(row["source"])
        held[state] = held.get(state, 0) + 1
        updated = {
            column: row.get(column)
            for column in (*EDGE_ROW_COLUMNS, *_WAITING_COLUMNS)
        }
        updated["state"] = state
        updated["catalog_version_id"] = catalog_version_id
        if rules is not None:
            updated["profile_fingerprint"] = rules.profile_fingerprint
        updates.append(updated)
    with _write_scope(store, session, branch_name):
        for chunk in _chunks(updates):
            # An identical row is a no-op write (the table discards exact
            # visible upserts), so a recheck that changes nothing cuts
            # nothing.
            session.upsert_rows(EDGE_DEFERRALS_TABLE, list(chunk))
    return len(promoted_ids), held


def deferred_edges_report(
    conn: Any,
    *,
    branch: str = "default",
    warn_after_seconds: int = DEFAULT_WARN_AFTER_SECONDS,
    now: datetime | None = None,
    sample_limit: int = SAMPLE_LIMIT,
) -> dict[str, Any]:
    """Operator view of the waiting table: counts, age, a bounded id sample.

    Counts are by producer, edge type, endpoint subtype and state. The id
    sample is operator-only: record ids can carry sensitive names, so no
    read operator serves this report, and ``okg doctor`` withholds the ids.
    A database without the table reports ``table_present: false``.
    """
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    backend = getattr(store.context, "_backend", None)
    if backend is None or EDGE_DEFERRALS_TABLE not in getattr(
        backend, "tables", {},
    ):
        return {
            "schema": "okg.deferred_edges.v1",
            "table_present": False,
            "total": 0,
            "groups": [],
            "oldest_deferred_at": None,
            "warn": False,
        }
    session = store.checkout(branch)
    groups = [
        {
            "producer": row["source"],
            "edge_type": row["edge_type"],
            "endpoint_subtype": row["expected_subtype"],
            "state": row["state"],
            "count": int(row["n"]),
            "oldest_deferred_at": (
                row["oldest"].isoformat() if row["oldest"] else None
            ),
        }
        for row in session.query(
            f"SELECT source, edge_type, expected_subtype, state, "
            f"count(*) AS n, min(deferred_at) AS oldest "
            f"FROM {EDGE_DEFERRALS_TABLE} "
            f"GROUP BY source, edge_type, expected_subtype, state "
            f"ORDER BY source, edge_type, expected_subtype, state",
        )
    ]
    oldest_rows = session.query(
        f"SELECT min(deferred_at) AS oldest FROM {EDGE_DEFERRALS_TABLE}",
    )
    oldest = oldest_rows[0]["oldest"] if oldest_rows else None
    clock = now or datetime.now(timezone.utc)
    age = (clock - oldest).total_seconds() if oldest is not None else 0.0
    sample = [
        {
            "src": row["src"], "edge_type": row["edge_type"],
            "dst": row["dst"], "producer": row["source"],
            "state": row["state"],
            "deferred_at": row["deferred_at"].isoformat(),
        }
        for row in session.query(
            f"SELECT src, edge_type, dst, source, state, deferred_at "
            f"FROM {EDGE_DEFERRALS_TABLE} ORDER BY deferred_at, edge_id "
            f"LIMIT {int(sample_limit)}",
        )
    ]
    total = sum(group["count"] for group in groups)
    return {
        "schema": "okg.deferred_edges.v1",
        "table_present": True,
        "total": total,
        "groups": groups,
        "oldest_deferred_at": oldest.isoformat() if oldest else None,
        "oldest_age_seconds": int(age),
        "warn_after_seconds": int(warn_after_seconds),
        "warn": bool(total and age > warn_after_seconds),
        "sample": sample,
        "sample_note": "operator-only: record ids can carry sensitive names",
    }


def source_deferral_rules(conn: Any, source: str) -> Any:
    """The active deferral rules of the producer named ``source``, or None.

    For callers with no deployment name (the run-base undo): the producer id
    picks the profile. Two deployments sharing one database and one producer
    id read as no rules, so the edge keeps today's rule.
    """
    from okg.substrate.producer_authority import (
        ProducerPolicyViolation,
        load_profile_for_producer,
        profile_deferral_rules,
    )

    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT deployment_name FROM okg.producer_profiles "
            "WHERE producer_id = %s AND status = 'active'",
            (source,),
        )
        deployments = [str(row[0]) for row in cur.fetchall()]
    if len(deployments) != 1:
        return None
    # Only a deployment whose effective policy defers: with deferral off
    # (``defer_dangling_edges: false``, or ``auto`` on a prune deployment) a
    # kept edge would dangle and the deployment's own dangling policy would
    # apply, which is not what undo and retirement did before.
    from okg.substrate.deployments import find_deployment_dir
    from okg.substrate.publisher.deployment_stack import load_deferral_policy

    directory = find_deployment_dir(deployments[0])
    if directory is None or not load_deferral_policy(directory).enabled:
        return None
    try:
        profile, _ = load_profile_for_producer(
            conn, deployment_name=deployments[0], producer_id=source,
        )
    except ProducerPolicyViolation:
        return None
    return profile_deferral_rules(conn, profile)
